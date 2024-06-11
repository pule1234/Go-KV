package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmap(fd *os.File, write bool, size int64) ([]byte, error) {
	// 只读
	protect := syscall.PAGE_READONLY
	access := syscall.FILE_MAP_READ

	if write {
		// 读写
		protect = syscall.PAGE_READWRITE
		access = syscall.FILE_MAP_WRITE
	}
	// 获取文件信息
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	// 如果文件大小小于要映射的大小， 则需要调整文件大小，使其能够容纳要映射的大小
	if fi.Size() < size {
		if err := fd.Truncate(size); err != nil {
			return nil, fmt.Errorf("truncate: %s", err)
		}
	}

	//Open a file mapping handle
	sizelo := uint32(size >> 32)
	sizehi := uint32(size) & 0xffffffff

	//创建一个文件对象， 并返回一个句柄
	handler, err := syscall.CreateFileMapping(syscall.Handle(fd.Fd()), nil,
		uint32(protect), sizelo, sizehi, nil)
	if err != nil {
		return nil, os.NewSyscallError("CreateFileMapping", err)
	}

	// 将文件映射到进程的地址空间中，并返回映射的起始地址
	addr, err := syscall.MapViewOfFile(handler, uint32(access), 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", err)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(handler)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(size), int(size)}

	// 将内存映射的起始地址转换为字节切片
	data := *(*[]byte)(unsafe.Pointer(&sl))

	return data, nil
}

// 取消内存映射
func munmap(b []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&b[0])))
}

func madvise(b []byte, readahead bool) error {
	// Do Nothing. We don’t care about this setting on Windows
	return nil
}

func msync(b []byte) error {
	return syscall.FlushViewOfFile(uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)))
}
