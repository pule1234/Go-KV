package rose

import (
	"rose/ds/art"
	"rose/logfile"
	"rose/logger"
	"rose/util"
)

// Set 为无序

// set 每一个key对应一个radixtree
func (db *RoseDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.setIndex.trees[string(key)]

	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		// 计算mem的hash code
		if err := db.setIndex.murhash.Write(mem); err != nil {
			return err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		// 重置该 hash 计算器, 整个 set 集合共用一个 hash 计算对象，入口方法有锁保证其线程安全.
		db.setIndex.murhash.Reset()

		ent := &logfile.LogEntry{Key: key, Value: mem}
		valuePos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return err
		}
		// 索引上的内容，对应的是mem的hashcode编码
		entry := &logfile.LogEntry{Key: sum, Value: mem}
		_, size := logfile.EncodeEntry(ent)
		valuePos.entrySize = size
		err = db.updateIndexTree(idxTree, entry, valuePos, true, Set)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *RoseDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() && count > 0 {
		count--
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	for _, val := range values {
		if err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (db *RoseDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil
	}
	for _, mem := range members {
		if err := db.sremInternal(key, mem); err != nil {
			return err
		}
	}
	return nil
}

// 判断member是不是该key中的成员
func (db *RoseDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	node := idxTree.Get(sum)
	return node != nil
}

func (db *RoseDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()
	// 返回这个key下的所有value
	return db.sMembers(key)
}

// 获取储存在键上的集合的元素数量
func (db *RoseDB) SCrad(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if db.setIndex.trees[string(key)] == nil {
		return 0
	}
	return db.setIndex.trees[string(key)].Size()
}

// // SDiff函数返回第一个集合与所有后续集合之间的差集成员。如果没有传递键作为参数，则返回错误。
func (db *RoseDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// keys 为空直接返回
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}
	// 存储除了第一个键外的所有其他键的值
	successiveSet := make(map[uint64]struct{})
	// 遍历传入的所有的key
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, value := range members {
			h := util.MemHash(value)
			if _, ok := successiveSet[h]; !ok {
				successiveSet[h] = struct{}{}
			}
		}
	}
	if len(successiveSet) == 0 {
		return firstSet, nil
	}
	res := make([][]byte, 0)
	for _, value := range firstSet {
		h := util.MemHash(value)
		// 找到除了第一个key 其他key中没有的值 存储到res中
		if _, ok := successiveSet[h]; !ok {
			res = append(res, value)
		}
	}
	return res, nil
}

// SDiffStore函数与SDiff相同，但是不返回结果集，而是将结果存储在第一个参数中
// 将key【0】 中没有的，添加到key【0】
func (db *RoseDB) SDiffStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	diff, err := db.SDiff(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, diff); err != nil {
		return -1, err
	}
	return db.SCrad(destination), nil
}

func (db *RoseDB) SUion(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	// 用于记录是否存在
	set := make(map[uint64]struct{})
	// 存储合并之后的值
	unionSet := make([][]byte, 0)
	for _, key := range keys {
		values, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, val := range values {
			h := util.MemHash(val)
			if _, ok := set[h]; !ok {
				set[h] = struct{}{}
				unionSet = append(unionSet, val)
			}
		}
	}
	return unionSet, nil
}

func (db *RoseDB) SUnionStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	union, err := db.SUion(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, union); err != nil {
		return -1, err
	}
	return db.SCrad(destination), nil
}

// 删除索引上的节点，并写上日志
func (db *RoseDB) sremInternal(key []byte, member []byte) error {
	//找到索引树
	idxTree := db.setIndex.trees[string(key)]

	err := db.setIndex.murhash.Write(member)
	if err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	// 在索引树上删除key对应的节点
	val, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}

	// 写入删除日志
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}
	db.sendDiscard(val, updated, Set)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[Set].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

func (db *RoseDB) sMembers(key []byte) ([][]byte, error) {
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}

	var values [][]byte
	idxTree := db.setIndex.trees[string(key)]
	itr := idxTree.Iterator()
	for itr.HasNext() {
		node, _ := itr.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// 所有key中都出现的value
func (db *RoseDB) SInter(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}
	num := len(keys)
	// 用于记录每一个值出现的次数
	set := make(map[uint64]int)
	interSet := make([][]byte, 0)
	for _, key := range keys {
		values, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, val := range values {
			h := util.MemHash(val)
			set[h]++
			if set[h] == num {
				interSet = append(interSet, val)
			}
		}
	}
	return interSet, nil
}

func (db *RoseDB) SInterStore(keys ...[]byte) (int, error) {
	destination := keys[0]
	inter, err := db.SInter(keys[1:]...)
	if err != nil {
		return -1, err
	}
	if err := db.sStore(destination, inter); err != nil {
		return -1, err
	}
	return db.SCrad(destination), nil
}

func (db *RoseDB) sStore(destination []byte, vals [][]byte) error {
	for _, val := range vals {
		if isMember := db.SIsMember(destination, val); !isMember {
			if err := db.SAdd(destination, val); err != nil {
				return err
			}
		}
	}
	return nil
}
