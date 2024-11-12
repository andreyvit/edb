// Package journal implements WAL-like append-only journals. A journal is split
// into segments; the last segment is the one being written to.
//
// Intended use cases:
//
//   - Database WAL files.
//   - Log files of various kinds.
//   - Archival of historical database records.
//
// Features:
//
//   - Suitable for a large number of very short records. Per-record overhead
//     can be as low as 2 bytes.
//
//   - Suitable for very large records, too. (In the future, it will be possible
//     to write records in chunks.)
//
//   - Fault-resistant.
//
//   - Self-healing. Verifies the checksums and truncates corrupted data when
//     opening the journal.
//
//   - Performant.
//
//   - Automatically rotates the files when they reach a certain size.
//
// TODO:
//
//   - Trigger rotation based on time (say, each day gets a new segment).
//     Basically limit how old in-progress segments can be.
//
//   - Allow to rotate a file without writing a new record. (Otherwise
//     rarely-used journals will never get archived.)
//
//   - Give work-in-progress file a prefixed name (W*).
//
//   - Auto-commit every N seconds, after K bytes, after M records.
//
//   - Option for millisecond timestamp precision?
//
//   - Reading API. (Search based on time and record ordinals.)
//
//   - Use mmap for reading.
//
// # File format
//
// Segment files:
//
//   - file = segmentHeader item*
//   - segmentHeader = (see struct)
//   - item = record | commit
//   - record = (size << 1):uvarint timestampDelta:uvarint bytes*
//   - commit = checksum_with_bit_0_set:64
//
// We always set bit 0 of commit checksums, and we use size*2 when encoding
// records; so bit 0 of the first byte of an item indicates whether it's
// a record or a commit.
//
// Timestamps are 32-bit unix times and have 1 second precision. (Rationale
// is that the primary use of timestamps is to search logs by time, and that
// does not require a higher precision. For high-frequency logs, with 1-second
// precision, timestamp deltas will typically fit within 1 byte.)
package journal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andreyvit/edb/mmap"
	"github.com/cespare/xxhash/v2"
)

var (
	ErrIncompatible       = fmt.Errorf("incompatible journal")
	ErrUnsupportedVersion = fmt.Errorf("unsupported journal version")
	errCorruptedFile      = fmt.Errorf("corrupted journal segment file")
	errFileGone           = fmt.Errorf("journal segment is gone")
)

type Options struct {
	FileName         string // e.g. "mydb-*.bin"
	MaxFileSize      int64  // new segment after this size
	DebugName        string
	Now              func() time.Time
	JournalInvariant [32]byte
	SegmentInvariant [32]byte

	Context context.Context
	Logger  *slog.Logger
	OnLoad  func()
	Verbose bool
}

const DefaultMaxFileSize = 4 * 1024 * 1024

const (
	magic          = 0x54414c4e52554f4a // "JOURNLAT" as little-endian uint64
	version0 uint8 = 0

	segmentHeaderSize = 16 * 8
	maxRecHeaderLen   = binary.MaxVarintLen64 + binary.MaxVarintLen32

	segFlagAligned   uint16 = 1 << 0
	recordFlagCommit byte   = 1
	recordFlagShift         = 1

	timestampFmt = "20060102T150405"
)

type segmentHeader struct {
	Magic            uint64
	Version          uint8
	_                uint8
	Flags            uint16
	_                uint32
	SegmentOrdinal   uint32
	Timestamp        uint32
	RecordOrdinal    uint64
	PrevChecksum     uint64
	JournalInvariant [32]byte
	SegmentInvariant [32]byte
	_                [2]uint64
	Checksum         uint64
}

type recordHeader struct {
	sizeAndFlags uint32
	timestamp    uint32
}

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

	var failedName string
retry:
	lastName := j.findLastFile(dirf)
	if j.verbose {
		j.logger.Debug("journal last file", "journal", j.debugName, "file", lastName)
	}
	if lastName == "" {
		return nil
	}
	if lastName == failedName {
		return fmt.Errorf("journal: failed twice to continue with segment file %s", lastName)
	}

	sw, err := continueSegment(j, lastName)
	if err == errFileGone {
		goto retry
	} else if err != nil {
		return err
	}
	j.segWriter = sw
	return nil
}

func (j *Journal) ensurePreparedToWrite_locked() error {
	if j.writeErr != nil {
		return j.writeErr
	}
	if j.writable {
		return nil
	}
	err := j.prepareToWrite_locked()
	if err != nil {
		return j.fail(err)
	}
	j.writable = true
	return nil
}

func (j *Journal) FinishWriting() error {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()
	return j.finishWriting_locked()
}

func (j *Journal) finishWriting_locked() error {
	j.writable = false
	var err error
	if j.segWriter != nil {
		err = j.segWriter.close()
		j.segWriter = nil
	}
	return err
}

func (j *Journal) fail(err error) error {
	if err == nil {
		return nil
	}

	j.logger.LogAttrs(j.context, slog.LevelError, "journal: failed", slog.String("journal", j.debugName), slog.Any("err", err))

	j.finishWriting_locked()

	if j.writeErr != nil {
		j.writeErr = err
	}
	return err
}

func (j *Journal) fsyncFailed(err error) {
	// TODO: enter a TOTALLY FAILED mode that's preserved across restarts
	// (e.g. by creating a sentinel file)
	j.fail(err)
}

func (j *Journal) filePath(name string) string {
	return filepath.Join(j.dir, name)
}
func (j *Journal) openFile(name string, writable bool) (*os.File, error) {
	if writable {
		return os.OpenFile(j.filePath(name), os.O_RDWR|os.O_CREATE, 0o666)
	} else {
		return os.Open(j.filePath(name))
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
	if len(data) == 0 {
		return nil
	}
	if timestamp == 0 {
		timestamp = j.Now()
	}

	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	err := j.ensurePreparedToWrite_locked()
	if err != nil {
		return nil
	}

	var seg uint32
	var rec uint64
	var prevChecksum uint64
	if j.segWriter == nil {
		seg = 1
		rec = 1
		prevChecksum = 0
	} else if j.segWriter.shouldRotate(len(data)) {
		if j.verbose {
			j.logger.Debug("rotating segment", "journal", j.debugName, "segment", j.segWriter.seg, "segment_size", j.segWriter.size, "data_size", len(data))
		}
		seg = j.segWriter.seg + 1
		rec = j.segWriter.nextRec
		j.segWriter.close()
		prevChecksum = j.segWriter.checksum() // close might do a commit
		j.segWriter = nil
	}

	if j.segWriter == nil {
		if j.verbose {
			j.logger.Debug("starting segment", "journal", j.debugName, "segment", seg, "record", rec)
		}
		sw, err := startSegment(j, seg, timestamp, rec, prevChecksum)
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

type segmentWriter struct {
	j           *Journal
	f           *os.File
	seg         uint32
	ts          uint32
	nextRec     uint64
	size        int64
	hash        xxhash.Digest
	uncommitted bool
	modified    bool
}

func startSegment(j *Journal, seg, ts uint32, rec uint64, prevChecksum uint64) (*segmentWriter, error) {
	name := formatSegmentName(j.fileNamePrefix, j.fileNameSuffix, seg, ts, rec)

	f, err := j.openFile(name, true)
	if err != nil {
		return nil, err
	}

	var ok bool
	defer closeAndDeleteUnlessOK(f, &ok)

	sw := &segmentWriter{
		j:        j,
		f:        f,
		seg:      seg,
		ts:       ts,
		nextRec:  rec,
		size:     segmentHeaderSize,
		modified: true,
	}
	sw.hash.Reset()

	var hbuf [segmentHeaderSize]byte
	fillSegmentHeader(hbuf[:], j, seg, ts, rec, prevChecksum, &sw.hash)

	_, err = f.Write(hbuf[:])
	if err != nil {
		return nil, err
	}

	ok = true
	return sw, nil
}

func continueSegment(j *Journal, fileName string) (*segmentWriter, error) {
	f, err := j.openFile(fileName, true)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errFileGone
		}
		return nil, err
	}
	var ok bool
	defer closeUnlessOK(f, &ok)

	sr, err := verifySegment(j, f, fileName)
	if err == errCorruptedFile {
		if sr == nil || sr.committedRec == 0 {
			j.logger.LogAttrs(j.context, slog.LevelWarn, "journal: deleting completely corrupted file", slog.String("journal", j.debugName), slog.String("file", fileName))
			err := os.Remove(j.filePath(fileName))
			if err != nil {
				return nil, fmt.Errorf("journal: failed to delete corrupted file: %w", err)
			}
			return nil, errFileGone
		} else {
			j.logger.LogAttrs(j.context, slog.LevelWarn, "journal: recovered corrupted file", slog.String("journal", j.debugName), slog.String("file", fileName), slog.Int("record", int(sr.committedRec)))
			err := f.Truncate(sr.committedSize)
			if err != nil {
				return nil, fmt.Errorf("journal: failed to truncate corrupted file: %w", err)
			}

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("fseek (before reverify): %w", err)
			}

			if sr.j.verbose {
				sr.j.logger.Debug("segment recovered", "journal", sr.j.debugName, "file", fileName)
			}

			sr, err = verifySegment(j, f, fileName)
			if err == errCorruptedFile {
				return nil, fmt.Errorf("journal: failured to recover corrupted file")
			} else if err != nil {
				return nil, err
			}
			if sr.size != sr.committedSize {
				panic("journal: unreachable")
			}
		}
	}

	ok = true
	return &segmentWriter{
		j:       j,
		f:       f,
		seg:     sr.seg,
		ts:      sr.ts,
		nextRec: sr.rec + 1,
		size:    sr.committedSize,
		hash:    sr.hash,
	}, nil
}

func (sw *segmentWriter) writeRecord(ts uint32, data []byte) error {
	var tsDelta uint32
	if ts > sw.ts {
		tsDelta = ts - sw.ts
		sw.ts = ts
	}

	var hbuf [maxRecHeaderLen]byte
	h := appendRecordHeader(hbuf[:0], len(data), tsDelta)

	// if sw.j.verbose {
	// 	sw.j.logger.Debug("hash before record", "journal", sw.j.debugName, "record", string(data), "hash", fmt.Sprintf("%08x", sw.hash.Sum64()))
	// }

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

	sw.uncommitted = true
	sw.modified = true
	sw.nextRec++
	sw.size += int64(len(h) + len(data))

	// if sw.j.verbose {
	// 	sw.j.logger.Debug("hash after record", "journal", sw.j.debugName, "record", string(data), "hash", fmt.Sprintf("%08x", sw.hash.Sum64()))
	// }

	return nil
}

func (sw *segmentWriter) commit() error {
	if !sw.uncommitted {
		return nil
	}
	sw.uncommitted = false
	sw.modified = true
	sw.size += 8

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sw.hash.Sum64()|uint64(recordFlagCommit))

	sw.hash.Write(buf[:])
	_, err := sw.f.Write(buf[:])
	if err != nil {
		return err
	}

	return nil
}

func (sw *segmentWriter) close() error {
	if sw.f == nil {
		return nil
	}
	err := sw.commit()
	if sw.modified {
		err := mmap.Fdatasync(sw.f, nil)
		if err != nil {
			sw.j.fsyncFailed(err)
		}
	}
	sw.f.Close()
	sw.f = nil
	return err
}

func (sw *segmentWriter) checksum() uint64 {
	return sw.hash.Sum64()
}

func (sw *segmentWriter) shouldRotate(size int) bool {
	return sw.size+int64(size) > sw.j.maxFileSize
}

type segmentReader struct {
	j             *Journal
	f             *os.File
	r             *bufio.Reader
	hash          xxhash.Digest
	seg           uint32
	rec           uint64
	ts            uint32
	size          int64
	recordsInSeg  int
	committedRec  uint64
	committedTS   uint32
	committedSize int64
	data          []byte
}

func verifySegment(j *Journal, f *os.File, fileName string) (*segmentReader, error) {
	sr, err := newSegmentReader(j, f, fileName)
	if err != nil {
		return sr, err
	}

	for {
		err := sr.next()
		if err == io.EOF {
			return sr, nil
		} else if err != nil {
			return sr, err
		}
	}
}

func newSegmentReader(j *Journal, f *os.File, fileName string) (*segmentReader, error) {
	seg, ts, rec, err := parseSegmentName(j.fileNamePrefix, j.fileNameSuffix, fileName)
	if err != nil {
		return nil, errCorruptedFile
	}

	sr := &segmentReader{
		j:             j,
		f:             f,
		r:             bufio.NewReader(f),
		seg:           seg,
		rec:           rec - 1,
		ts:            ts,
		size:          0,
		committedRec:  0,
		committedTS:   0,
		committedSize: 0,
	}
	sr.hash.Reset()

	var h segmentHeader
	err = sr.readHeader(&h)
	if err != nil {
		return sr, err
	}
	sr.size = int64(segmentHeaderSize)
	sr.committedSize = int64(segmentHeaderSize)
	return sr, nil
}

func (sr *segmentReader) next() error {
	for {
		b, err := sr.r.Peek(maxRecHeaderLen)
		if err == io.EOF {
			if len(b) == 0 {
				// end of file; was there a commit?
				if sr.size == sr.committedSize {
					return io.EOF
				} else {
					if sr.j.verbose {
						sr.j.logger.Debug("corrupted record: end of file without a commit", "journal", sr.j.debugName)
					}
					return errCorruptedFile
				}
			}
		} else if err != nil {
			return err
		}
		if b[0]&recordFlagCommit != 0 {
			var b [8]byte
			_, err := io.ReadFull(sr.r, b[:])
			if err == io.ErrUnexpectedEOF {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: end of file in the middle of commit", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			} else if err != nil {
				return err
			}
			actual := binary.LittleEndian.Uint64(b[:])
			expected := sr.hash.Sum64() | uint64(recordFlagCommit)
			sr.hash.Write(b[:])
			if actual != expected {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: commit checksum mismatch", "journal", sr.j.debugName, "actual", fmt.Sprintf("%08x", actual), "expected", fmt.Sprintf("%08x", expected))
				}
				return errCorruptedFile
			}

			sr.size += 8
			if sr.recordsInSeg > 0 {
				sr.committedRec = sr.rec
				sr.committedTS = sr.ts
				sr.committedSize = sr.size

				if sr.j.verbose {
					sr.j.logger.Debug("commit decoded", "journal", sr.j.debugName)
				}
			} else {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: commit without a prior record", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}
		} else {
			rawSize, n1 := binary.Uvarint(b)
			if n1 <= 0 {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: cannot decode size", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}
			dataSize := int(rawSize / 2)

			tsdelta, n2 := binary.Uvarint(b[n1:])
			if n2 <= 0 {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: cannot decode timestamp", "journal", sr.j.debugName)
				}
				return errCorruptedFile
			}

			// if sr.j.verbose {
			// 	sr.j.logger.Debug("hash before record", "journal", sr.j.debugName, "hash", fmt.Sprintf("%08x", sr.hash.Sum64()))
			// }

			n := n1 + n2
			sr.hash.Write(b[:n])
			sr.r.Discard(n)

			if cap(sr.data) < dataSize {
				sr.data = make([]byte, dataSize, allocSize(dataSize))
			} else {
				sr.data = sr.data[:dataSize]
			}

			_, err = io.ReadFull(sr.r, sr.data)
			if err == io.ErrUnexpectedEOF {
				if sr.j.verbose {
					sr.j.logger.Debug("corrupted record: EOF when reading record data", "journal", sr.j.debugName, "offset", fmt.Sprintf("%08x", sr.size+int64(n)), "size", dataSize)
				}
				return errCorruptedFile
			} else if err != nil {
				return err
			}
			sr.hash.Write(sr.data)

			sr.recordsInSeg++
			sr.rec++
			sr.ts += uint32(tsdelta)
			sr.size += int64(n + dataSize)

			if sr.j.verbose {
				sr.j.logger.Debug("record decoded", "journal", sr.j.debugName, "data", string(sr.data), "hash", fmt.Sprintf("%08x", sr.hash.Sum64()))
			}

			return nil
		}
	}
}

func (sr *segmentReader) readHeader(h *segmentHeader) error {
	var buf [segmentHeaderSize]byte
	_, err := io.ReadFull(sr.r, buf[:])
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

	sr.hash.Write(buf[:segmentHeaderSize-8])
	checksum := sr.hash.Sum64()
	sr.hash.Write(buf[segmentHeaderSize-8 : segmentHeaderSize])

	if checksum != h.Checksum {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: checksum", "journal", sr.j.debugName, "actual", fmt.Sprintf("%08x", h.Checksum), "expected", fmt.Sprintf("%08x", checksum))
		}
		return errCorruptedFile
	}
	if sr.seg != h.SegmentOrdinal {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: segment ordinal", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if sr.ts != h.Timestamp {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: timestamp", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if sr.rec+1 != h.RecordOrdinal {
		if sr.j.verbose {
			sr.j.logger.Debug("corrupted header: record ordinal", "journal", sr.j.debugName)
		}
		return errCorruptedFile
	}
	if h.Version != version0 {
		if sr.j.verbose {
			sr.j.logger.Debug("incompatible header: version", "journal", sr.j.debugName)
		}
		return ErrUnsupportedVersion
	}
	if h.JournalInvariant != sr.j.journalInvariant {
		if sr.j.verbose {
			sr.j.logger.Debug("incompatible header: journal invariant", "journal", sr.j.debugName)
		}
		return ErrIncompatible
	}
	if ((h.Flags & segFlagAligned) != 0) != sr.j.aligned {
		if sr.j.verbose {
			sr.j.logger.Debug("incompatible header: flags", "journal", sr.j.debugName)
		}
		return ErrIncompatible
	}

	return nil
}

func allocSize(sz int) int {
	if sz >= math.MaxInt64/2 {
		panic("size too large")
	}
	r := 64 * 1024
	for r < sz {
		r <<= 1
	}
	return r
}

func closeAndDeleteUnlessOK(f *os.File, ok *bool) {
	if *ok {
		return
	}
	f.Close()
	os.Remove(f.Name())
}

func closeUnlessOK(f *os.File, ok *bool) {
	if *ok {
		return
	}
	f.Close()
}

func fillSegmentHeader(buf []byte, j *Journal, seg, ts uint32, rec, prevChecksum uint64, hash *xxhash.Digest) {
	h := segmentHeader{
		Magic:            magic,
		Version:          version0,
		SegmentOrdinal:   seg,
		Timestamp:        ts,
		RecordOrdinal:    rec,
		PrevChecksum:     prevChecksum,
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
	if n != segmentHeaderSize {
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

func formatSegmentName(prefix, suffix string, seg, ts uint32, id uint64) string {
	t := time.Unix(int64(uint64(ts)), 0).UTC()
	return fmt.Sprintf("%s%010d-%s-%012d%s", prefix, seg, t.Format(timestampFmt), id, suffix)
}

func parseSegmentName(prefix, suffix, name string) (seg, ts uint32, id uint64, err error) {
	origName := name
	name, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	name, ok = strings.CutSuffix(name, suffix)
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}

	segStr, rem, ok := strings.Cut(name, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	v, err := strconv.ParseUint(segStr, 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q (invalid segment number)", origName)
	}
	seg = uint32(v)

	tsStr, idStr, ok := strings.Cut(rem, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid segment file name %q", origName)
	}
	t, err := time.ParseInLocation(timestampFmt, tsStr, time.UTC)
	if err != nil {
		return seg, 0, 0, fmt.Errorf("invalid segment file name %q (invalid timestamp)", origName)
	}
	ts = uint32(t.Unix())

	id, err = strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return seg, 0, 0, fmt.Errorf("invalid segment file name %q (invalid record identifier)", origName)
	}
	return
}
