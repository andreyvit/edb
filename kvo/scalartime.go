package kvo

import "time"

// TimeOffsetMicros is offset to Time.UnixMicro() that kvo stores, chosen
// such that time.Time{}.UnixMicro() = -TimeOffsetMicros. With this offset,
// 0 micros correspond to zero time instead of Unix epoch, and we can treat
// all times as unsigned integers.
const TimeOffsetMicros = 62_135_596_800_000_000

func TimeToUint64(t time.Time) uint64 {
	return uint64(t.UnixMicro()) + TimeOffsetMicros
}

func Uint64ToTime(u uint64) time.Time {
	return time.UnixMicro(int64(u) - TimeOffsetMicros)
}
