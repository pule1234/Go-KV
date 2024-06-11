package rose

import (
	"rose/ds/art"
	"rose/logfile"
	"rose/logger"
	"rose/util"
)

// 两组数据结构， 一个是radixtree 另一个是hashmap和skiplist组成的sortedSet结构
func (db *RoseDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()
	// 计算出member的hashcode
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	//获取radixtree
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	// 将score权重值转换成字节数据
	scoreBuf := []byte(util.Float64ToStr(score))
	zsetkey := db.encodeKey(key, scoreBuf)
	// zet中logfile 是以 权重值score和key作为键
	entry := &logfile.LogEntry{Key: zsetkey, Value: member}
	pos, err := db.writeLogEntry(entry, Zset)
	if err != nil {
		return err
	}
	_, size := logfile.EncodeEntry(entry)
	pos.entrySize = size
	ent := &logfile.LogEntry{Key: sum, Value: member}
	if err := db.updateIndexTree(idxTree, ent, pos, true, Zset); err != nil {
		return err
	}
	// 存到跳表上
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	return nil
}

// 获取权重值
func (db *RoseDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	return db.zsetIndex.indexes.ZScore(string(key), string(sum))
}

func (db *RoseDB) ZRem(key, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	//获取member的hashcode
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	// 先从跳表上删除
	ok := db.zsetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return nil
	}

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}

	idxTree := db.zsetIndex.trees[string(key)]
	oldVal, deleted := idxTree.Delete(sum)
	db.sendDiscard(oldVal, deleted, Zset)
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Zset)
	if err != nil {
		return err
	}

	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[Zset].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

// 返回存储在key下的元素数量
func (db *RoseDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZCard(string(key))
}

// 正序获取传入区域范围内的元素
// return member
func (db *RoseDB) ZRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, false)
}

// 逆序获取传入区域范围内的元素
func (db *RoseDB) ZRevRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, true)
}

func (db *RoseDB) zRangeInternal(key []byte, start, stop int, rev bool) ([][]byte, error) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	//当前key的索引树
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	var res [][]byte
	var values []interface{}

	if rev {
		// 在跳表中获取值
		values = db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
	} else {
		values = db.zsetIndex.indexes.ZRange(string(key), start, stop)
	}
	for _, val := range values {
		v, _ := val.(string)
		if val, err := db.getVal(idxTree, []byte(v), Zset); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}

// ZRank 返回存储在键名为 key 的有序集合中成员 member 的排名（即索引），按照分数从低到高的顺序排列
func (db *RoseDB) ZRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, false)
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *RoseDB) ZRevRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, true)
}

func (db *RoseDB) zRankInternal(key []byte, member []byte, rev bool) (ok bool, rank int) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	// 拿到当前的索引树
	if db.zsetIndex.trees[string(key)] == nil {
		return
	}

	//初始化murhash
	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	var res int64
	if rev {
		res = db.zsetIndex.indexes.ZRevRank(string(key), string(sum))
	} else {
		res = db.zsetIndex.indexes.ZRank(string(key), string(sum))
	}
	if res != -1 {
		ok = true
		rank = int(res)
	}

	return
}
