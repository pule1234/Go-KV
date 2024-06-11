package mmap

import "os"

// 指定文件映射到内存中，并返回映射后的字节切片，以便对文件进行读写操作
func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

func Munmap(b []byte) error {
	return munmap(b)
}

func Madvise(b []byte, readahead bool) error {
	return madvise(b, readahead)
}

// 将内存中的数据同步到文件系统中
func Msync(b []byte) error {
	return msync(b)
}
