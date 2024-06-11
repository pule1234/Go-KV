package ioselector

import (
	"io"
	"os"
	"rose/mmap"
)

// MMapSelector represents using memory-mapped file I/O.
// 需要实现IOSelector接口中的方法
type MMapSelector struct {
	fd     *os.File
	buf    []byte
	bufLen int64
}

// 创建一个新的内存映射文件选择器
// 内存映射是一种将文件的一部分或整个内容映射到内存中的机制，
// 使得对文件的读写操作可以直接映射到内存，而不需要通过传统的读写函数
func NewMMapSelector(fname string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}

	file, err := openFile(fname, fsize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, fsize)
	if err != nil {
		return nil, err
	}

	return &MMapSelector{fd: file, buf: buf, bufLen: int64(len(buf))}, nil
}

func (lm *MMapSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}
	// 判断offset是否小于0 或者 当前值 + 需要添加的长度是否会超过阈值
	if offset < 0 || length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

// Read copy data from mapped region(buf) into slice b at offset.
func (lm *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	//要读取的数据长度加上偏移量超过了映射区域的长度 lm.bufLen，
	//那么就无法从映射区域中读取足够的数据
	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}

	return copy(b, lm.buf[offset:]), nil
}

func (lm *MMapSelector) Sync() error {
	return mmap.Msync(lm.buf)
}

func (lm *MMapSelector) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return nil
	}
	// 取消内存映射
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	return lm.fd.Close()
}

func (lm *MMapSelector) Delete() error {
	// 取消内存映射
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	// 释放资源
	lm.buf = nil
	// 调整文件大小尾0 ， 清空文件内容
	if err := lm.fd.Truncate(0); err != nil {
		return err
	}
	if err := lm.fd.Close(); err != nil {
		return err
	}
	return os.Remove(lm.fd.Name())
}
