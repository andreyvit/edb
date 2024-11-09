// Package journal implements WAL-like append-only “journal” files.
//
// Intended use cases:
//
//  1. Database WAL files.
//  2. Log files of various kinds.
//  3. Archival of historical database records.
//
// Features:
//
//  1. Suitable for records of all sizes, from very short to very long.
//     Long records can be written to the file in chunks. Multiple short records
//     can be combined into a single transaction with minimal overhead.
//
//  2. Crash-resistant (if followed by an fsync). Contains a CRC32 checksum of
//     each record, and automatically trims the file after the first corrupted
//     record.
//
//  3. Performant.
//
//  4. Automatically rotates the files when they reach a certain size. (You can
//     also trigger the rotation programmatically at any time.)
//
//  5. Manages segment file namingю
//
// # The
//
// File format: segmentHeader (recordHeader bytes* padding* recordTrailer)
//
//   - file = segmentHeader record*
//   - segmentHeader = magic:64 segmentNumber:32 flags:32 prevChecksum:64 fixedMarker:64*4 segmentMarker:64*4 checksum:64
//   - header = magic:64 seq:32 fe:16 se:16 flags:64
//   - record = flagsAndSize:uvarint timestamp:32? bytes* checksum:64?
package journal

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrIncompatible       = fmt.Errorf("incompatible journal")
	ErrUnsupportedVersion = fmt.Errorf("unsupported journal version")
	errCorruptedFile      = fmt.Errorf("corrupted journal segment file")
)

type Options struct {
	Context          context.Context
	FileName         string // e.g. "mydb-*.bin"
	MaxFileSize      int64  // new segment after this size
	DebugName        string
	Now              func() time.Time
	JournalInvariant [32]byte
	SegmentInvariant [32]byte

	Logger  *slog.Logger
	OnLoad  func()
	Verbose bool
}

const DefaultMaxFileSize = 4 * 1024 * 1024

const IgnorableTimestampDriftSeconds = 10

const (
	magic          = 0x54414c4e52554f4a // "JOURNLAT" as little-endian uint64
	version0 uint8 = 0
)

const segmentHeaderSize = 16 * 8

type segmentHeader struct {
	Magic            uint64
	Version          uint8
	_                uint8
	Flags            uint16
	_                uint32
	SegmentOrdinal   uint32
	Timestamp        uint32
	PrevChecksum     uint64
	JournalInvariant [32]byte
	SegmentInvariant [32]byte
	_                [3]uint64
	Checksum         uint64
}

type recordHeader struct {
	sizeAndFlags uint32
	timestamp    uint32
}

const (
	segFlagAligned uint16 = 1 << 0
)

const (
	recordFlagCommit byte = 1
	recordFlagShift       = 1
	timestampFmt          = "20060102T150405"
)

// seq uint64
// sizeNFlags uint64
// flags:uint32
// size:uint32
// <data>
// size:uint32
// crc:uint32

// Journal represents a set of AOFs.
type Journal struct {
	context          context.Context
	maxFileSize      int64
	fileNamePrefix   string
	fileNameSuffix   string
	debugName        string
	dir              string
	now              func() time.Time
	logger           *slog.Logger
	serialIDs        bool
	aligned          bool
	verbose          bool
	writable         bool
	journalInvariant [32]byte
	segmentInvariant [32]byte

	writeLock sync.Mutex
	writeErr  error
	writeSeg  uint32
	writeRec  uint64
	segWriter *segmentWriter
}

func New(dir string, o Options) *Journal {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Context == nil {
		o.Context = context.Background()
	}
	if o.FileName == "" {
		o.FileName = "*"
	}
	prefix, suffix, _ := strings.Cut(o.FileName, "*")
	if o.DebugName == "" {
		o.DebugName = "journal"
	}
	if o.MaxFileSize == 0 {
		o.MaxFileSize = DefaultMaxFileSize
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Journal{
		context:          o.Context,
		maxFileSize:      o.MaxFileSize,
		fileNamePrefix:   prefix,
		fileNameSuffix:   suffix,
		debugName:        o.DebugName,
		dir:              dir,
		now:              o.Now,
		aligned:          false,
		verbose:          o.Verbose,
		journalInvariant: o.JournalInvariant,
		segmentInvariant: o.SegmentInvariant,
		logger:           o.Logger,
	}
}

func (j *Journal) Now() uint32 {
	v := j.now().Unix()
	if v < 0 {
		panic("time travel disallowed")
	}
	u := uint64(v)
	if u&0xFFFF_FFFF_0000_0000 != 0 {
		panic("time travel disallowed both ways")
	}
	return uint32(u)
}

func (j *Journal) String() string {
	return j.debugName
}

func (j *Journal) StartWriting() {
	j.writeLock.Lock()
	if j.writable || j.writeErr != nil {
		j.writeLock.Unlock()
		return
	}
	j.writable = true

	go func() {
		defer j.writeLock.Unlock()
		j.fail(j.prepareToWrite_locked())
	}()
}

func (j *Journal) prepareToWrite_locked() error {
	dirf, err := os.Open(j.dir)
	if err != nil {
		return err
	}
	defer dirf.Close()

	ds, err := dirf.Stat()
	if err != nil {
		return err
	}
	if !ds.IsDir() {
		return fmt.Errorf("%v: not a directory", j.debugName)
	}

fileLoop:
	for {
		lastName := j.findLastFile(dirf)

		if lastName == "" {
			return nil
		}

		seq, _, _, err := parseSegmentName(lastName)
		if err != nil {
			return err
		}

		f, err := j.openFile(lastName, true)
		if err != nil {
			return err
		}

		stat, err := f.Stat()
		if err != nil {
			return err
		}

		var h segmentHeader
		err = j.readHeader(f, &h, seq)
		if err == errCorruptedFile {
			j.logger.LogAttrs(j.context, slog.LevelWarn, "journal: deleting corrupted file", slog.String("jrnl", j.debugName), slog.String("file", lastName), slog.Int64("size", stat.Size()))
			err := os.Remove(lastName)
			if err != nil {
				return fmt.Errorf("journal: failed to delete corrupted file: %w", err)
			}
			continue fileLoop
		} else if err != nil {
			return err
		}
		j.writeSeg = h.SegmentOrdinal

		// TODO: read to end of file

		return nil
	}
}

func (j *Journal) FinishWriting() {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()
	j.finishWriting_locked()
}

func (j *Journal) finishWriting_locked() {
	j.writable = false
	if j.segWriter != nil {
		j.segWriter.close()
		j.segWriter = nil
	}
}

func (j *Journal) fail(err error) error {
	if err == nil {
		return nil
	}

	j.logger.LogAttrs(j.context, slog.LevelError, "journal: failed", slog.String("jrnl", j.debugName), slog.Any("err", err))

	j.finishWriting_locked()

	if j.writeErr != nil {
		j.writeErr = err
	}
	return err
}

func (j *Journal) openFile(name string, writable bool) (*os.File, error) {
	fn := filepath.Join(j.dir, name)
	if writable {
		return os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0o666)
	} else {
		return os.Open(fn)
	}
}

func (j *Journal) findLastFile(dirf fs.ReadDirFile) string {
	var lastName string
	for {
		if err := j.context.Err(); err != nil {
			break
		}

		ents, err := dirf.ReadDir(16)
		if err == io.EOF {
			break
		}
		for _, ent := range ents {
			if !ent.Type().IsRegular() {
				continue
			}
			name := ent.Name()
			if !strings.HasPrefix(name, j.fileNamePrefix) {
				continue
			}
			if !strings.HasSuffix(name, j.fileNameSuffix) {
				continue
			}
			if name > lastName {
				lastName = name
			}
		}
	}
	return lastName
}

func (j *Journal) WriteRecord(timestamp uint32, data []byte) error {
	if !j.writable {
		panic("journal is opened as read-only")
	}
	if len(data) == 0 {
		return nil
	}

	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	if j.writeErr != nil {
		return j.writeErr
	}

	if timestamp == 0 {
		timestamp = j.Now()
	}

	j.writeRec++

	if j.segWriter == nil {
		j.writeSeg++

		sw, err := startSegment(j, j.writeSeg, timestamp, j.writeRec)
		if err != nil {
			return j.fail(err)
		}
		j.segWriter = sw
	}

	return j.fail(j.segWriter.writeRecord(timestamp, data))
}

func (j *Journal) Commit() error {
	if j.segWriter == nil {
		return nil
	}
	return j.fail(j.segWriter.commit())
}

func (j *Journal) readHeader(f *os.File, h *segmentHeader, expectedSeq uint32) error {
	var buf [segmentHeaderSize]byte
	_, err := io.ReadFull(f, buf[:])
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		return errCorruptedFile
	} else if err != nil {
		return err
	}
	n, err := binary.Decode(buf[:], binary.LittleEndian, h)
	if err != nil {
		panic(err)
	}
	if n != len(buf) {
		panic("internal size mismatch")
	}

	checksum := xxhash.Sum64(buf[:segmentHeaderSize-8])
	if checksum != h.Checksum {
		return errCorruptedFile
	}
	if expectedSeq != h.SegmentOrdinal {
		return errCorruptedFile
	}
	if h.Version > version0 {
		return ErrUnsupportedVersion
	}
	if h.JournalInvariant != j.journalInvariant {
		return ErrIncompatible
	}
	if ((h.Flags & segFlagAligned) != 0) != j.aligned {
		return ErrIncompatible
	}

	return nil
}

type segmentWriter struct {
	f           *os.File
	seg         uint32
	ts          uint32
	size        int64
	hash        xxhash.Digest
	uncommitted bool
}

func startSegment(j *Journal, seg, ts uint32, rec uint64) (*segmentWriter, error) {
	name := formatSegmentName(j.fileNamePrefix, j.fileNameSuffix, seg, ts, rec)

	f, err := j.openFile(name, true)
	if err != nil {
		return nil, err
	}

	var ok bool
	defer closeAndDeleteUnlessOK(f, &ok)

	sw := &segmentWriter{
		f:    f,
		seg:  seg,
		ts:   ts,
		size: segmentHeaderSize,
	}
	sw.hash.Reset()

	var hbuf [segmentHeaderSize]byte
	fillSegmentHeader(hbuf[:], j, seg, ts, &sw.hash)

	_, err = f.Write(hbuf[:])
	if err != nil {
		return nil, err
	}

	ok = true
	return sw, nil
}

const maxRecHeaderLen = binary.MaxVarintLen64 + binary.MaxVarintLen32

func (sw *segmentWriter) writeRecord(ts uint32, data []byte) error {
	var tsDelta uint32
	if ts > sw.ts {
		tsDelta = ts - sw.ts
		sw.ts = ts
	}
	sw.uncommitted = true

	var hbuf [maxRecHeaderLen]byte
	h := appendRecordHeader(hbuf[:0], len(data), tsDelta)

	sw.hash.Write(h)
	_, err := sw.f.Write(h)
	if err != nil {
		return err
	}

	sw.hash.Write(data)
	_, err = sw.f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (sw *segmentWriter) commit() error {
	if !sw.uncommitted {
		return nil
	}
	sw.uncommitted = false

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sw.hash.Sum64())
	buf[0] |= recordFlagCommit

	sw.hash.Write(buf[:])
	_, err := sw.f.Write(buf[:])
	if err != nil {
		return err
	}

	return nil
}

func (sw *segmentWriter) close() {
	if sw.f == nil {
		return
	}
	sw.f.Close()
	sw.f = nil
}

func closeAndDeleteUnlessOK(f *os.File, ok *bool) {
	if *ok {
		return
	}
	f.Close()
	os.Remove(f.Name())
}

func fillSegmentHeader(buf []byte, j *Journal, seg, ts uint32, hash *xxhash.Digest) {
	h := segmentHeader{
		Magic:            magic,
		Version:          version0,
		SegmentOrdinal:   seg,
		Timestamp:        ts,
		PrevChecksum:     0,
		JournalInvariant: j.journalInvariant,
		SegmentInvariant: j.segmentInvariant,
	}
	if j.aligned {
		h.Flags |= segFlagAligned
	}

	n, err := binary.Encode(buf[:], binary.LittleEndian, h)
	if err != nil {
		panic(err)
	}
	if n != len(buf) {
		panic("internal size mismatch")
	}

	hash.Write(buf[:segmentHeaderSize-8])
	binary.LittleEndian.PutUint64(buf[segmentHeaderSize-8:], hash.Sum64())
	hash.Write(buf[segmentHeaderSize-8 : segmentHeaderSize])
}

func appendRecordHeader(b []byte, size int, tsDelta uint32) []byte {
	b = binary.AppendUvarint(b, uint64(size)<<recordFlagShift)
	b = binary.AppendUvarint(b, uint64(tsDelta))
	return b
}

func formatSegmentName(prefix, suffix string, seq, ts uint32, id uint64) string {
	t := time.Unix(int64(uint64(ts)), 0).UTC()
	return fmt.Sprintf("%s%012d-%s-%016x%s", prefix, seq, t.Format(timestampFmt), id, suffix)
}

func parseSegmentName(name string) (seq, ts uint32, id uint64, err error) {
	seqStr, rem, ok := strings.Cut(name, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", name)
	}
	v, err := strconv.ParseUint(seqStr, 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q (invalid segment number)", name)
	}
	seq = uint32(v)

	tsStr, idStr, ok := strings.Cut(rem, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", name)
	}
	t, err := time.ParseInLocation(timestampFmt, tsStr, time.UTC)
	if err != nil {
		return seq, 0, 0, fmt.Errorf("invalid segment file name %q (invalid timestamp)", name)
	}
	ts = uint32(t.Unix())

	id, err = strconv.ParseUint(idStr, 16, 64)
	if err != nil {
		return seq, 0, 0, fmt.Errorf("invalid segment file name %q (invalid record identifier)", name)
	}
	return
}
