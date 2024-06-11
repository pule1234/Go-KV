package logfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"rose/ioselector"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCrc invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")
	// ErrWriteSizeNotEqual write size is not equal to entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")
	// ErrEndOfEntry end of entry in log file.
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")
	// ErrUnsupportedIoType unsupported io type, only mmap and fileIO now.
	ErrUnsupportedIoType = errors.New("unsupported io type")
	// ErrUnsupportedLogFileType unsupported log file type, only WAL and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// 初始的logfile id
	InitialLogFileId = 0
	// 前缀
	FilePrefix = "log."
)

type FileType int8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
		List: "log.list.",
		Hash: "log.hash.",
		Sets: "log.sets.",
		ZSet: "log.zset.",
	}

	// FileTypesMap name->type
	FileTypesMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// logfile文件对象
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, iotype IOType) (lf *LogFile, err error) {
	lf = &LogFile{Fid: fid}
	//获取日志文件名
	fileName, err := lf.getLogFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}
	var selector ioselector.IOSelector
	// 依照iotype 生成selector
	switch iotype {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}

	lf.IoSelector = selector
	return
}

func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// 从文件中获取header
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// read entry key and value
	if kSize > 0 || vSize > 0 {
		// 获取	key value
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

func (lf *LogFile) getLogFileName(path string, fid uint32, ftype FileType) (name string, err error) {
	if _, ok := FileNamesMap[ftype]; !ok {
		return "", ErrUnsupportedLogFileType
	}
	fname := FileNamesMap[ftype] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fname)
	return
}

func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}

func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}
