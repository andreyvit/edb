package edb

import (
	"errors"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// ApplyReplication consumes one complete replication log-validated source group.
// The consumer validates history/filter continuity and delivers groups in order.
// Caller-owned bytes must remain valid until return; failed groups can be retried.
func (db *DB) ApplyReplication(g *ReplicationGroup) error {
	if db.replication == nil || db.replication.opt.Apply == nil {
		return errors.New("edb: replication apply is not configured")
	}
	if g == nil {
		return errors.New("edb: nil replication group")
	}
	if g.TransactionID == 0 {
		return errors.New("edb: invalid replication transaction ID")
	}
	opt := db.replication.opt.Apply
	tx := db.BeginUpdate(TxMetadata{Kind: "system:replication_apply", Tenant: "none"})
	defer tx.Close()
	tx.internalWrite = true
	tx.rawWrites = true
	duplicate := false
	err := safelyCall(func(tx *Tx) error {
		b := tx.btx.Bucket([]byte(replicationBucket))
		checkpoint := row2.Open(b.Get(replicationCommittedKey))
		if g.TransactionID <= checkpoint.Get(replicationCommittedProp, row2.Word).AsUint() {
			duplicate = true
			return nil
		}
		if g.SchemaRev > opt.SchemaRev || opt.SupportsRevision == nil && g.SchemaRev != opt.SchemaRev || opt.SupportsRevision != nil && !opt.SupportsRevision(g.SchemaRev) {
			return ErrReplicationSchema
		}
		for i := range g.Changes {
			c := &g.Changes[i]
			if i > 0 && c.Ordinal <= g.Changes[i-1].Ordinal {
				return errors.New("edb: replication record order is not increasing")
			}
			if err := db.validateReplicationChange(c); err != nil {
				return err
			}
		}
		for i := range g.Changes {
			tx.applyReplicationChange(&g.Changes[i])
		}
		updated := row2.Edit(checkpoint)
		updated.Set([]row2.Step{{Key: replicationCommittedProp}}, values.Uint(g.TransactionID), row2.Word)
		return b.Put(replicationCommittedKey, updated.Snapshot().Raw())
	}, tx)
	if err != nil {
		return err
	}
	if duplicate {
		return nil
	}
	return tx.Commit()
}
func (db *DB) validateReplicationChange(c *ReplicationChange) error {
	if c.Op != ReplicationPut && c.Op != ReplicationDelete {
		return errors.New("edb: unsupported replication operation")
	}
	if len(c.Key) == 0 {
		return errors.New("edb: invalid replication key")
	}
	if c.Op == ReplicationPut && !c.Value.Present {
		return errors.New("edb: PUT requires a present value")
	}
	switch c.Object.Kind() {
	case ReplicationRowTable:
		t := c.Object.Table
		if db.schema.TableByNameHash(t.NameHash()) != t {
			return errors.New("edb: unresolved replication table")
		}
		if c.Op == ReplicationPut {
			v := c.Value
			if !valueFlags(v.Flags).supported() || v.SchemaVer > maxSchemaVersion || v.SchemaVer > t.latestSchemaVer {
				return errors.New("edb: unsupported replication row metadata")
			}
			if valueFlags(v.Flags) == vfKVO {
				if t.binding == nil {
					return ErrNoRecordBinding
				}
				if err := t.binding.TryValidate(v.Data); err != nil {
					return err
				}
			}
		}
	case ReplicationKVTable:
		t := c.Object.KVTable
		if db.schema.KVTableByNameHash(t.NameHash()) != t {
			return errors.New("edb: unresolved replication KV table")
		}
	case ReplicationSingletonMap:
		m := c.Object.Map
		if db.schema.KVMapByNameHash(m.NameHash()) != m {
			return errors.New("edb: unresolved replication map")
		}
		if c.Op != ReplicationPut {
			return errors.New("edb: singleton delete is unsupported")
		}
	default:
		return errors.New("edb: invalid replication object")
	}
	return nil
}
func (tx *Tx) applyReplicationChange(c *ReplicationChange) {
	switch c.Object.Kind() {
	case ReplicationRowTable:
		if c.Op == ReplicationDelete {
			tx.deleteStoredRow(c.Object.Table, c.Key)
		} else {
			v := value{Flags: valueFlags(c.Value.Flags), SchemaVer: c.Value.SchemaVer, ModCount: c.Value.ModCount, Data: c.Value.Data}
			tx.putStoredRow(c.Object.Table, c.Key, &v)
		}
	case ReplicationKVTable:
		data := c.Value.Data
		if c.Op == ReplicationDelete {
			data = nil
		} else if data == nil {
			data = []byte{}
		}
		tx.KVPutRaw(c.Object.KVTable, c.Key, data)
	case ReplicationSingletonMap:
		ensure(nonNil(tx.stx.Bucket(c.Object.Map.name, "")).Put(c.Key, c.Value.Data))
	}
}
