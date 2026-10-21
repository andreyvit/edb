package edb

import "sync"

const (
	indexRowsBufferCapacityInitial  = 256
	byteBufferListCapacityInitial   = 64
	keyBufferCapacityInitial        = 256
	valueBufferCapacityInitial      = 1024
	indexValueBufferCapacityInitial = 128

	keyBufferCapacityLimit       = 32 * 1024
	valueBufferCapacityLimit     = 64 * 1024
	byteBufferListCapacityLimit  = 4096
	indexRowsBufferCapacityLimit = 4096
	txIndexKeyBufferBytesLimit   = 64 * 1024
)

type indexRowBuffers struct {
	key   *byteBuffer
	value *byteBuffer
}

type indexRowsBuffer struct {
	data    indexRows
	buffers []indexRowBuffers
}

var indexRowsPool = &sync.Pool{
	New: func() any {
		return &indexRowsBuffer{
			data:    make(indexRows, 0, indexRowsBufferCapacityInitial),
			buffers: make([]indexRowBuffers, 0, indexRowsBufferCapacityInitial),
		}
	},
}

func acquireIndexRowsBuffer() *indexRowsBuffer {
	return indexRowsPool.Get().(*indexRowsBuffer)
}

func releaseIndexRowsBuffer(buf *indexRowsBuffer) {
	if cap(buf.data) > indexRowsBufferCapacityLimit || cap(buf.buffers) > indexRowsBufferCapacityLimit {
		return
	}
	clear(buf.data[:min(len(buf.buffers), cap(buf.data))])
	clear(buf.buffers)
	buf.buffers = buf.buffers[:0]
	indexRowsPool.Put(buf)
}

type byteBufferList struct {
	data []*byteBuffer
}

var byteBufferListPool = &sync.Pool{
	New: func() any {
		return &byteBufferList{data: make([]*byteBuffer, 0, byteBufferListCapacityInitial)}
	},
}

func acquireByteBufferList() *byteBufferList {
	return byteBufferListPool.Get().(*byteBufferList)
}

func releaseByteBufferList(buf *byteBufferList) {
	if cap(buf.data) > byteBufferListCapacityLimit {
		return
	}
	clear(buf.data)
	buf.data = buf.data[:0]
	byteBufferListPool.Put(buf)
}

type byteBuffer struct {
	data []byte
}

var keyBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, keyBufferCapacityInitial)}
	},
}

func acquireKeyBuffer() *byteBuffer {
	return keyBytesPool.Get().(*byteBuffer)
}

func releaseKeyBuffer(buf *byteBuffer) {
	if cap(buf.data) > keyBufferCapacityLimit {
		return
	}
	buf.data = buf.data[:0]
	keyBytesPool.Put(buf)
}

var valueBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, valueBufferCapacityInitial)}
	},
}

func acquireValueBuffer() *byteBuffer {
	return valueBytesPool.Get().(*byteBuffer)
}

func releaseValueBuffer(buf *byteBuffer) {
	if cap(buf.data) > valueBufferCapacityLimit {
		return
	}
	buf.data = buf.data[:0]
	valueBytesPool.Put(buf)
}

var indexValueBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, indexValueBufferCapacityInitial)}
	},
}

func acquireIndexValueBuffer() *byteBuffer {
	return indexValueBytesPool.Get().(*byteBuffer)
}

func releaseIndexValueBuffer(buf *byteBuffer) {
	if cap(buf.data) > valueBufferCapacityLimit {
		return
	}
	buf.data = buf.data[:0]
	indexValueBytesPool.Put(buf)
}

var emptyIndexValue = []byte{}
