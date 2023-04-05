package edb

import "sync"

var indexRowsPool = &sync.Pool{
	New: func() any {
		return make(indexRows, 0, 256)
	},
}

var arrayOfBytesPool = &sync.Pool{
	New: func() any {
		return make([][]byte, 0, 1024)
	},
}

var keyBytesPool = &sync.Pool{
	New: func() any {
		return make([]byte, 0, 32768) // max key size in Bolt
	},
}

func releaseKeyBytes(b []byte) {
	keyBytesPool.Put(b[:0])
}

var valueBytesPool = &sync.Pool{
	New: func() any {
		return make([]byte, 0, 65536)
	},
}

var indexValueBytesPool = &sync.Pool{
	New: func() any {
		return make([]byte, 0, 65536)
	},
}

var emptyIndexValue = []byte{}
