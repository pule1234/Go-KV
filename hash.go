package rose

import (
	"bytes"
	"errors"
	"math"
	"math/rand"
	"regexp"
	"rose/ds/art"
	"rose/logfile"
	"rose/logger"
	"rose/util"
	"strconv"
	"time"
)

// hset key field value field value
func (db *RoseDB) HSet(key []byte, args ...[]byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	// 判断数量是否有效
	if len(args) == 0 || len(args)&1 == 1 {
		return ErrWrongNumberOfArgs
	}

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]
	for i := 0; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		hashKey := db.encodeKey(key, field)
		// 将key 和 field写入日志文件
		entry := &logfile.LogEntry{Key: hashKey, Value: value}
		valuePos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return err
		}

		// 再构建一个entry对象，这里的key为field字段
		// 将field 插入到radixtree中
		ent := &logfile.LogEntry{Key: field, Value: value}
		_, size := logfile.EncodeEntry(entry)
		valuePos.entrySize = size
		err = db.updateIndexTree(idxTree, ent, valuePos, true, Hash)
		if err != nil {
			return err
		}
	}
	return nil
}

// 若插入的值存在，则返回
func (db *RoseDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}

	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}

	if val != nil {
		return false, nil
	}
	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: value}
	valuePos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return false, err
	}

	entry := &logfile.LogEntry{Key: field, Value: value}
	_, size := logfile.EncodeEntry(ent)
	valuePos.entrySize = size
	err = db.updateIndexTree(idxTree, entry, valuePos, true, Hash)
	if err != nil {
		return false, err
	}
	return true, nil
}

// hget key field  直接传入field，然后再radixtree上查找
func (db *RoseDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// hmget field1 field2 field3.....
func (db *RoseDB) HMGet(key []byte, fields ...[]byte) (vals [][]byte, err error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	length := len(fields)
	if db.hashIndex.trees[string(key)] == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	for _, field := range fields {
		val, err := db.getVal(idxTree, field, Hash)
		if err == ErrKeyNotFound {
			vals = append(vals, nil)
		} else {
			vals = append(vals, val)
		}
	}
	return
}

func (db *RoseDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0, nil
	}
	idxTree := db.hashIndex.trees[string(key)]

	// 计数
	var count int
	for _, field := range fields {
		// 先直接从索引上删除
		val, updated := idxTree.Delete(field)
		if !updated {
			continue
		}
		hashKey := db.encodeKey(key, field)
		entry := &logfile.LogEntry{Key: hashKey, Type: logfile.TypeDelete}
		valuePos, err := db.writeLogEntry(entry, Hash)
		if err != nil {
			return 0, err
		}
		count++
		// 删除需要通知到discard
		db.sendDiscard(val, updated, Hash)
		_, size := logfile.EncodeEntry(entry)
		node := &indexNode{fid: valuePos.fid, entrySize: size}
		select {
		case db.discards[Hash].valChan <- node:
		default:
			logger.Warn("send to discard chan fail")
		}
	}
	return count, nil
}

// hexists key field   判断值是否存在
func (db *RoseDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return false, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}
	return val != nil, nil
}

// 获取其中键值对的个数
func (db *RoseDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.hashIndex.trees[string(key)]
	return idxTree.Size()
}

// 获取key下的所有field
func (db *RoseDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var keys [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return keys, nil
	}
	// 创建索引树迭代器, 用于遍历Hash 整个数据中的字段
	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

func (db *RoseDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var values [][]byte
	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return values, nil
	}

	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		val, err := db.getVal(tree, node.Key(), Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

func (db *RoseDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	tree, ok := db.hashIndex.trees[string(key)]
	if !ok {
		return [][]byte{}, nil
	}
	var index int
	pairs := make([][]byte, tree.Size()*2)
	iter := tree.Iterator()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		field := node.Key()
		val, err := db.getVal(tree, field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		pairs[index], pairs[index+1] = field, val
		index += 2
	}
	return pairs[:index], nil
}

// return length(value)   hstrlen key field
func (db *RoseDB) HStrLen(key, field []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return 0
	}
	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err == ErrKeyNotFound {
		return 0
	}
	return len(val)
}

func (db *RoseDB) HScan(key []byte, prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}
	// 加锁
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.hashIndex.trees[string(key)]
	fields := idxTree.PrefixScan(prefix, count)
	if len(fields) == 0 {
		return nil, nil
	}

	//遍历正则解析器
	var reg *regexp.Regexp
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}

	values := make([][]byte, 2*len(fields))
	var index int
	for _, field := range fields {
		// 忽略不匹配的正则规则数据
		if reg != nil && !reg.Match(field) {
			continue
		}
		val, err := db.getVal(idxTree, field, Hash)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		values[index], values[index+1] = field, val
		index += 2
	}
	return values, nil
}

func (db *RoseDB) HIncrBy(key, field []byte, incr int64) (int64, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}

	idxTree := db.hashIndex.trees[string(key)]
	val, err := db.getVal(idxTree, field, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	// 相等则说明val为空
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}

	valInt64, err := util.StrToInt64(string(val))
	if err != nil {
		return 0, ErrWrongValueType
	}

	//防止溢出
	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))

	hashKey := db.encodeKey(key, field)
	ent := &logfile.LogEntry{Key: hashKey, Value: val}
	valuePos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return 0, err
	}
	entry := &logfile.LogEntry{Key: field, Value: val}
	_, size := logfile.EncodeEntry(ent)
	valuePos.entrySize = size
	err = db.updateIndexTree(idxTree, entry, valuePos, true, Hash)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// 获取指定数量的字段或字段值
// 从 RoseDB 中的 Hash 数据类型中随机获取指定数量的字段或字段值，
// 并根据参数 withValues 的不同选择是否获取字段值，
// 以及根据参数 count 的正负来决定返回不重复的或重复的字段或字段值。
func (db *RoseDB) HRandField(key []byte, count int, withValues bool) ([][]byte, error) {
	if count == 0 {
		return [][]byte{}, nil
	}
	//存储字段或者字段值
	var values [][]byte
	var err error
	// 表示每个字段或者字段值对的长度，默认为1
	var pairLength = 1
	// 先取出所有的，然后再随机
	if !withValues {
		values, err = db.HKeys(key)
	} else {
		pairLength = 2
		values, err = db.HGetAll(key)
	}

	if err != nil {
		return [][]byte{}, err
	}
	if len(values) == 0 {
		return [][]byte{}, nil
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 字段或者字段值对的数量
	pairCount := len(values) / pairLength

	if count > 0 {
		//如果需要返回的数量大于等于所有字段或字段值对的数量，则直接返回所有字段或字段值
		if count >= pairCount {
			return values, nil
		}
		var noDupValues = values
		// 计算需要踢出的重复元素的数量
		diff := pairCount - count
		for i := 0; i < diff; i++ {
			//生成一个随机索引
			rndIdx := rnd.Intn(len(noDupValues)/pairLength) * pairLength
			noDupValues = append(noDupValues[:rndIdx], noDupValues[rndIdx+pairLength:]...)
		}
		return noDupValues, nil
	}
	count = -count
	var dupValues [][]byte
	for i := 0; i < count; i++ {
		rndIdx := rnd.Intn(pairCount) * pairLength
		dupValues = append(dupValues, values[rndIdx:rndIdx+pairLength]...)
	}
	return dupValues, nil
}
