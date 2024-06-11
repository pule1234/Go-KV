package rose

import "time"

type DataIndexMode int

const (
	// 表示将值直接存储在索引上，不需要再去向logfile文件查找
	KeyValueMemMode DataIndexMode = iota

	// 表示只将值保存在内存中， 获取值时需要进行磁盘查找
	KeyOnlyMemMode
)

// FileIO（标准文件 IO）和 MMap（内存映射）。
type IOType int8

const (
	//标准文件
	FileIO IOType = iota
	// 内存映射
	MMap
)

type Options struct {
	// 内存文件
	DBPath string
	// 索引模式
	IndexMode DataIndexMode
	//文件读写的io类型
	Iotype IOType
	//Sync 表示是否将写入从操作系统缓存同步到实际磁盘。
	Sync bool
	// gc回收的频率   默认为8小时
	LogFileGCInterval time.Duration
	// gc回收的比率  默认为0.5 ，超过此比率开始gc
	LogFileGCRatio float64
	//每个日志文件的阈值大小， 超过该值，则会重新开启一个activelogfile文件
	LogFileSizeThreshold int64
	// DiscardBufferSize 创建一个通道，在键更新或删除时，将发送旧条目的大小。
	// 条目大小将被保存在丢弃文件中，在日志文件垃圾回收运行时记录无效大小，并在需要时使用。
	// 此选项表示该通道的大小。
	// 如果出现诸如 send discard chan fail 的错误，您可以增加此选项以避免出现该错误。
	DiscardBufferSize int
}

func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IndexMode:            KeyOnlyMemMode,
		Iotype:               FileIO,
		Sync:                 false,
		LogFileGCInterval:    time.Hour * 8,
		LogFileGCRatio:       0.5,
		LogFileSizeThreshold: 512 << 20,
		DiscardBufferSize:    8 << 20,
	}
}
