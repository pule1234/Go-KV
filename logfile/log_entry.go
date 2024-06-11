package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// MaxHeaderSize max entry header size
// crc32	typ    kSize	vSize	expiredAt
//
//	4    +   1   +   5   +   5    +    10      = 25
const MaxHeaderSize = 25

type EntryType byte

const (
	// TypeDelete represents entry type is delete.
	TypeDelete EntryType = iota + 1
	// TypeListMeta represents entry is list meta.
	TypeListMeta
)

// LogEntry is the data will be appended in log file.
type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64 // time.Unix
	Type      EntryType
}

type entryHeader struct {
	crc32     uint32 // check sum
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}

// EncodeEntry 将entry编码成[]byte
// encoded Entry:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//
//	|--------------------------crc check---------------------------|
func EncodeEntry(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}

	header := make([]byte, MaxHeaderSize)
	// 将类型信息写入到header的第5个字节， 因为前四个字节用于存储crc32校验和
	header[4] = byte(e.Type)
	var index = 5
	// key size
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	// value size
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	// expireAt
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	// 将header中的内容复制到buf中，覆盖buf的前index个字节
	copy(buf[:index], header[:])
	// key and value.
	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	//crc32
	crc := crc32.ChecksumIEEE(buf[4:])
	//将计算得到的crc32校验和以LittleEndian的方式写入buf的前4个字节，用于校验数据完整性。
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5
	ksize, n := binary.Varint(buf[index:])
	h.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	return h, int64(index + n)
}

// 获取entry  crc的值
func getEntryCrc(e *LogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
