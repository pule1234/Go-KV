package rose

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"rose/ioselector"
	"rose/logfile"
	"rose/logger"
	"sort"
	"sync"
)

const (
	discardRecordSize = 12
	// 8kb, contains mostly 682 records in file.
	discardFileSize int64 = 2 << 12
	discardFileName       = "discard"
)

var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

type discard struct {
	sync.Mutex
	once     *sync.Once
	valChan  chan *indexNode
	file     ioselector.IOSelector
	freeList []int64          // 可以分配的文件偏移量
	location map[uint32]int64 // 每个文件的文件偏移量
}

// 新建discard
func newDiscard(path, name string, bufferSize int) (*discard, error) {
	fname := filepath.Join(path, name)
	// 创建内存映射文件选择器
	file, err := ioselector.NewMMapSelector(fname, discardFileSize)
	if err != nil {
		return nil, err
	}

	// 记录偏移量
	var freeList []int64
	var offset int64
	location := make(map[uint32]int64)
	for {
		buf := make([]byte, 8)
		//读取8个字节的数据到buf
		if _, err := file.Read(buf, offset); err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		if fid == 0 && total == 0 {
			freeList = append(freeList, offset)
		} else {
			location[fid] = offset
		}
		offset += discardRecordSize
	}
	d := &discard{
		valChan:  make(chan *indexNode, bufferSize),
		once:     new(sync.Once),
		file:     file,
		freeList: freeList,
		location: location,
	}

	// 后台监听更新
	go d.listenUpdates()
	return d, nil
}

func (d *discard) sync() error {
	return d.file.Sync()
}

// 用来获取需要被垃圾回收的 logfile 列表
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	var offset int64
	var ccl []uint32
	d.Lock()
	defer d.Unlock()

	for {
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}
		// 累加偏移量
		offset += discardRecordSize
		// 解码读取fid， tital 和discard三个字段
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:8])
		discard := binary.LittleEndian.Uint32(buf[8:12])
		var curRatio float64
		if total != 0 && discard != 0 {
			// 计算出删除空间在总空间的占用比率
			curRatio = float64(discard) / float64(total)
		}
		// 需要忽略活跃 logfile, 如果当前的 logfile 的删除数据占比超过了垃圾回收 ratio 阈值,
		// 则添加到 ccl 集合里.
		if curRatio >= ratio && fid != activeFid {
			ccl = append(ccl, fid)
		}
	}

	// 进行正序排序，老的logfile在数据的前面
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) listenUpdates() {
	for {
		select {
		case idxNode, ok := <-d.valChan:
			//如果通道被关闭，则执行文件关闭操作
			if !ok {
				if err := d.file.Close(); err != nil {
					fmt.Println("粗无")
					logger.Errorf("close discard file err: %v", err)
				}
				return
			}
			// 记录丢弃记录的计数
			d.incrDiscard(idxNode.fid, idxNode.entrySize)
		}
	}
}

func (d *discard) incrDiscard(fid uint32, delta int) {
	if delta > 0 {
		d.incr(fid, delta)
	}
}

// discard file 的形式
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
func (d *discard) incr(fid uint32, delta int) {
	d.Lock()
	defer d.Unlock()

	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	var buf []byte
	if delta > 0 {
		buf = make([]byte, 4)
		offset += 8
		if _, err := d.file.Read(buf, offset); err != nil {
			logger.Errorf("incr value in discard err:%v", err)
			return
		}

		v := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		buf = make([]byte, discardRecordSize)
	}

	if _, err := d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err:%v", err)
		return
	}
}

// 调用该方法之前必须先持有锁
func (d *discard) alloc(fid uint32) (int64, error) {
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}

	//弹出末端
	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}

func (d *discard) clear(fid uint32) {
	d.incr(fid, -1)
	d.Lock()
	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
	d.Unlock()
}

func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	// 该文件存在直接返回
	if _, ok := d.location[fid]; ok {
		return
	}

	offset, err := d.alloc(fid)
	if err != nil {
		logger.Errorf("discard file allocate err: %+v", err)
		return
	}

	buf := make([]byte, 8)
	//文件标识fid和总大小totalSize以小端字节序写入到切片中。
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:8], totalSize)
	if _, err = d.file.Write(buf, offset); err != nil {
		logger.Errorf("incr value in discard err: %v", err)
		return
	}
}

func (d *discard) closeChan() {
	d.once.Do(func() { close(d.valChan) })
}
