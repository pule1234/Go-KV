package util

import "strconv"

func StrToInt64(val string) (int64, error) {
	return strconv.ParseInt(val, 10, 64)
}

func Float64ToStr(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func StrToFloat64(val string) (float64, error) {
	return strconv.ParseFloat(val, 64)
}

func StrToUint(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}
