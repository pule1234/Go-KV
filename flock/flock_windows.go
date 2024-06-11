package flock

import "syscall"

// D：tmp

type FileLockGuard struct {
	//文件句柄，于表示操作系统中的文件，管理文件的状态
	fd syscall.Handle
}

// 根据给定的路径和读写模式来创建或打开文件，并返回一个表示文件锁的就结构体指针
func AcquireFileLock(path string, readOnly bool) (*FileLockGuard, error) {
	// 将文件路径字符串转换为UTF-16编码的指针
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	//设置两个变量用于存储访问权限和模式
	var access, mode uint32
	if readOnly {
		access = syscall.GENERIC_READ
		mode = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE
	} else {
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}

	file, err := syscall.CreateFile(ptr, access, mode, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err == syscall.ERROR_FILE_NOT_FOUND {
		file, err = syscall.CreateFile(ptr, access, mode, nil,
			syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	}
	if err != nil {
		return nil, err
	}
	return &FileLockGuard{fd: file}, nil
}

func SyncDir(name string) error {
	return nil
}

func (fl *FileLockGuard) Release() error {
	return syscall.Close(fl.fd)
}
