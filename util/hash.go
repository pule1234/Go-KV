package util

import "unsafe"

//go:linkname runtimeMemhash runtime.memhash
//go:noescape
func runtimeMemhash(p unsafe.Pointer, seed, s uintptr) uintptr

func MemHash(buf []byte) uint64 {
	return rthash(buf, 923)
}

func rthash(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return seed
	}

	if unsafe.Sizeof(uintptr(0)) == 8 {
		return uint64(runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b))))
	}
	lo := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed), uintptr(len(b)))
	hi := runtimeMemhash(unsafe.Pointer(&b[0]), uintptr(seed>>32), uintptr(len(b)))
	return uint64(hi)<<32 | uint64(lo)
}
