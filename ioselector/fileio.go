package ioselector

import "os"

type FileIOSelector struct {
	fd *os.File // system file descriptor.
}

func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	// 在指定的offset处写入b中的数据
	return fio.fd.WriteAt(b, offset)
}

func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
