// Package row2 reads and writes the Fast2 and Compact2 KVO row formats.
// A Field or View borrows at most one byte span. The external schema supplies
// signedness and field meanings. Select the codec at the table boundary:
// these bytes cannot be decoded as Fast1 word-map data.
package row2
