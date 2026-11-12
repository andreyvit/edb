// Package kvo defines logical models, typed properties, and record access and
// editing over MessagePack, Fast2, Compact2, native Go structs, and decoded memory.
// Records and collection views use 16-byte handles. Storage codecs live in the
// codec subpackages; values supplies the shared value representation.
//
// Fastest materializes the native binding when attached, or indexed memory
// otherwise, independently of the storage format. Fast keeps an existing Fast2
// view and otherwise uses Fastest.
package kvo
