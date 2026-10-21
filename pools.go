package edb

import "sync"

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
			data:    make(indexRows, 0, 256),
			buffers: make([]indexRowBuffers, 0, 256),
		}
	},
}

func acquireIndexRowsBuffer() *indexRowsBuffer {
	return indexRowsPool.Get().(*indexRowsBuffer)
}

func releaseIndexRowsBuffer(buf *indexRowsBuffer) {
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
		return &byteBufferList{data: make([]*byteBuffer, 0, 1024)}
	},
}

func acquireByteBufferList() *byteBufferList {
	return byteBufferListPool.Get().(*byteBufferList)
}

func releaseByteBufferList(buf *byteBufferList) {
	clear(buf.data)
	buf.data = buf.data[:0]
	byteBufferListPool.Put(buf)
}

type byteBuffer struct {
	data []byte
}

var keyBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, 32768)} // max key size in Bolt
	},
}

func acquireKeyBuffer() *byteBuffer {
	return keyBytesPool.Get().(*byteBuffer)
}

func releaseKeyBuffer(buf *byteBuffer) {
	buf.data = buf.data[:0]
	keyBytesPool.Put(buf)
}

var valueBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, 65536)}
	},
}

func acquireValueBuffer() *byteBuffer {
	return valueBytesPool.Get().(*byteBuffer)
}

func releaseValueBuffer(buf *byteBuffer) {
	buf.data = buf.data[:0]
	valueBytesPool.Put(buf)
}

var indexValueBytesPool = &sync.Pool{
	New: func() any {
		return &byteBuffer{data: make([]byte, 0, 65536)}
	},
}

func acquireIndexValueBuffer() *byteBuffer {
	return indexValueBytesPool.Get().(*byteBuffer)
}

func releaseIndexValueBuffer(buf *byteBuffer) {
	buf.data = buf.data[:0]
	indexValueBytesPool.Put(buf)
}

var emptyIndexValue = []byte{}
