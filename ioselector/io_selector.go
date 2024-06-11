package ioselector

import (
	"errors"
	"os"
)

// ErrInvalidFsize invalid file size.
var ErrInvalidFsize = errors.New("fsize can`t be zero or negative")

const FilePerm = 0644

type IOSelector interface {
	//Write a slice to log file at offset
	// It returns the number of bytes written and an error , if any
	Write(b []byte, offset int64) (int, error)

	// Read a slice from offet
	Read(b []byte, offset int64) (int, error)
	//写盘
	Sync() error
	// 关闭文件
	Close() error
	// Delete the file
	Delete() error
}

func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}

	return fd, nil
}
