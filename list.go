package rose

import (
	"bytes"
	"encoding/binary"
	"math"
	"rose/ds/art"
	"rose/logfile"
	"rose/logger"
)

// list中一个数据在radixtree对应两个节点信息， 一个存储valuepos 另一个存储mate元数据

func (db *RoseDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key[:], buf[4:])
	return key, seq
}

// list中每一个key对应着一个radixtree
// lpush key value1 value2 value3......
func (db *RoseDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewArt()
	}

	for _, val := range values {
		// 插入处理
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// 当key不存在时直接返回
func (db *RoseDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, val := range values {
		if err := db.pushInternal(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

func (db *RoseDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewArt()
	}
	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *RoseDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, val := range values {
		if err := db.pushInternal(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

func (db *RoseDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

func (db *RoseDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// // LMove原子性地返回并移除源列表中的第一个/最后一个元素，并将该元素推入目标列表的第一个/最后一个位置
func (db *RoseDB) LMove(srcKey, dstKey []byte, srcIsLeft, dstIsLeft bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	popValue, err := db.popInternal(srcKey, srcIsLeft)
	if err != nil {
		return nil, err
	}
	if popValue == nil {
		return nil, nil
	}

	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewArt()
	}
	if err = db.pushInternal(dstKey, popValue, dstIsLeft); err != nil {
		return nil, err
	}
	return popValue, nil
}

func (db *RoseDB) LLen(key []byte) int {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return 0
	}
	return int(tailSeq - headSeq - 1)
}

// 按照传入的索引找到该元素
func (db *RoseDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}

	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrWrongIndex
	}
	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// 在指定的index处设置值
func (db *RoseDB) LSet(key []byte, index int, value []byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return err
	}
	if seq >= tailSeq || seq <= headSeq {
		return ErrWrongIndex
	}
	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}
	return nil
}

// LRage   lrange key start end
func (db *RoseDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.listIndex.trees[string(key)] == nil {
		return nil, ErrKeyNotFound
	}

	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	var startSeq, endSeq uint32

	// 获取起始索引
	startSeq, err = db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return nil, err
	}

	// 获取尾部索引
	endSeq, err = db.listSequence(headSeq, tailSeq, end)
	if err != nil {
		return nil, err
	}

	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}

	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}

	if startSeq >= tailSeq || endSeq <= headSeq || startSeq > endSeq {
		return nil, ErrWrongIndex
	}

	// 从头部遍历到尾部
	for seq := startSeq; seq <= endSeq; seq++ {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil

}

// 写入数据到日志文件和索引
func (db *RoseDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	//每一个list key 都有一个单独的radixTree 索引对象
	idxTree := db.listIndex.trees[string(key)]
	// 从kv里获取list key 的元数据 ： 头和尾序号
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return err
	}
	var seq = headSeq
	// 是否是对于左边的操作， 调整位置
	if !isLeft {
		seq = tailSeq
	}

	// 编码序号和key生成新的key， enckey中的前四个字节为seq
	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: val}
	valuePos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}

	if err = db.updateIndexTree(idxTree, ent, valuePos, true, List); err != nil {
		return err
	}

	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}

	// 更新key的元数据， 头尾序号
	err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
	return err
}

func (db *RoseDB) listMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	val, err := db.getVal(idxTree, key, List)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	// 对于该key， 初始化headseq和tailseq的值
	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	// 当前val 不为空
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

func (db *RoseDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, len(key)+4)
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key[:])
	return buf
}

func (db *RoseDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	// 前4个字节为head序号，后4个字节为tail序号
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)

	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(idxTree, ent, pos, true, List)
	return err
}

func (db *RoseDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	// 首先获取相关索引对象
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return nil, err
	}
	// 如果没有数据则直接返回
	if tailSeq-headSeq-1 <= 0 {
		return nil, nil
	}

	//移动位置
	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	enckey := db.encodeListKey(key, seq)
	// 先从索引中过去对应的值
	val, err := db.getVal(idxTree, enckey, List)
	if err != nil {
		return nil, err
	}

	//在logfile文件中写入一个删除标记
	ent := &logfile.LogEntry{Key: enckey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	// 删除索引上的节点
	oldVal, updated := idxTree.Delete(enckey)
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.saveListMeta(idxTree, key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	// send discard
	db.sendDiscard(oldVal, updated, List)
	_, entrySize := logfile.EncodeEntry(ent)
	node := &indexNode{fid: pos.fid, entrySize: entrySize}
	select {
	case db.discards[List].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}

	if tailSeq-headSeq-1 == 0 {
		// 为空需要重置matadata
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			_ = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		}
		delete(db.listIndex.trees, string(key))
	}
	return val, nil
}

// 获取index 在list中的位置， index> 0 表示从头部获取，  < 0 表示从尾部获取
func (db *RoseDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32
	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}

// lrem  key count value
func (db *RoseDB) LRem(key []byte, count int, value []byte) (int, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if count == 0 {
		count = math.MaxUint32
	}

	var discardCount int
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return discardCount, nil
	}
	headSeq, tailSeq, err := db.listMeta(idxTree, key)
	if err != nil {
		return discardCount, err
	}

	//声明三个空数组，用于存储保留序列号、待移除序列号和保留值序列
	reserveSeq, discardSeq, reserveValueSeq := make([]uint32, 0), make([]uint32, 0), make([][]byte, 0)
	// 符合条件的元素移动到待移除序列号数组中，并将保留元素的序列号和值分别存储到对应的数组中
	classifyData := func(key []byte, seq uint32) error {
		encKey := db.encodeListKey(key, seq)
		val, err := db.getVal(idxTree, encKey, List)
		if err != nil {
			return err
		}
		if bytes.Equal(value, val) {
			discardSeq = append(discardSeq, seq)
			discardCount++
		} else {
			reserveSeq = append(reserveSeq, seq)
			temp := make([]byte, len(val))
			copy(temp, val)
			reserveValueSeq = append(reserveValueSeq, temp)
		}
		return nil
	}

	// 向列表中添加保留的元素
	addReserveData := func(key []byte, value []byte, isLeft bool) error {
		if db.listIndex.trees[string(key)] == nil {
			db.listIndex.trees[string(key)] = art.NewArt()
		}
		if err := db.pushInternal(key, value, isLeft); err != nil {
			return err
		}
		return nil
	}

	// 若count>0 则从头到尾处理    若count < 0 则从尾到头处理
	if count > 0 {
		for seq := headSeq + 1; seq < tailSeq; seq++ {
			if err := classifyData(key, seq); err != nil {
				return discardCount, err
			}
			if discardCount == count {
				break
			}
		}

		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			// 删除所有
			for seq := headSeq + 1; seq <= discardSeq[discardSeqLen-1]; seq++ {
				if _, err := db.popInternal(key, true); err != nil {
					return discardCount, err
				}
			}
			// 再添加回不需要删除的数据
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] < discardSeq[discardSeqLen-1] {
					if err := addReserveData(key, reserveValueSeq[i], true); err != nil {
						return discardCount, err
					}
				}
			}
		}
	} else {
		count = -count
		for seq := tailSeq - 1; seq > headSeq; seq-- {
			if err := classifyData(key, seq); err != nil {
				return discardCount, err
			}
			if discardCount == count {
				break
			}
		}

		discardSeqLen := len(discardSeq)
		if discardSeqLen > 0 {
			for seq := tailSeq - 1; seq >= discardSeq[discardSeqLen-1]; seq-- {
				if _, err := db.popInternal(key, false); err != nil {
					return discardCount, err
				}
			}
			for i := len(reserveSeq) - 1; i >= 0; i-- {
				if reserveSeq[i] > discardSeq[discardSeqLen-1] {
					if err := addReserveData(key, reserveValueSeq[i], false); err != nil {
						return discardSeqLen, err
					}
				}
			}
		}
	}
	return discardCount, nil
}
