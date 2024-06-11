package rose

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"rose/logfile"
	"rose/logger"
	"rose/util"
	"strconv"
	"time"
)

func (db *RoseDB) Set(key, value []byte) error {
	// 加锁，rosedb里每一个redis类型都有一把锁，减小锁竞争
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 构建entry
	entry := &logfile.LogEntry{Key: key, Value: value}
	// 写入到活跃的logfile文件中
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// 把 entry 的信息插入到 string 类型的 index 索引上, 这里的 index 使用 radixTree 实现的.
	// 如果是内存模式，需要在 index 保存 kv，而存储模式在 index 只需要保存 key 和 valuePos.
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return err
}

func (db *RoseDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	return db.getVal(db.strIndex.idxTree, key, String)
}

// MGet get the values of all specified keys.
func (db *RoseDB) MGet(keys [][]byte) ([][]byte, error) {
	// 加锁
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}

	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		// 忽略不存在的情况
		if err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// 获取范围内的数据
// mget key start end
func (db *RoseDB) GetRange(key []byte, start, end int) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return []byte{}, nil
	}
	// 负偏移量可以用来从字符串末尾开始提供偏移量。
	// 因此，-1 表示最后一个字符，-2 表示倒数第二个字符，依此类推。
	if start < 0 {
		start = len(val) + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = len(val) + end
		if end < 0 {
			end = 0
		}
	}

	if end > len(val)-1 {
		end = len(val) - 1
	}

	if start > len(val)-1 || start > end {
		return []byte{}, nil
	}
	return val[start : end+1], nil
}

// 这个方法用于获取指定键的值并删除该键。它与Get方法类似，但是在获取值的同时，如果键存在，也会将其删除。
func (db *RoseDB) GetDel(key []byte) ([]byte, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return nil, err
	}

	oldVal, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(oldVal, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return val, nil
}

func (db *RoseDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 写入一个删除标记
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// 在索引上删除该key
	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}

// setEX key value time
func (db *RoseDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	expiredAt := time.Now().Add(duration).Unix()
	entry := &logfile.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
}

// 若存在，则不让设置
func (db *RoseDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	// Key exists in db.
	if val != nil {
		return nil
	}

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
}

// // 原子操作，要么全部成功，要么全部失败
// mset key1 value1  key2 value2 ........
func (db *RoseDB) MSet(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 判断是否为偶数   key value, key value ......
	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	//每次遍历一对
	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		entry := &logfile.LogEntry{Key: key, Value: value}
		valuePos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
		if err != nil {
			return err
		}
	}
	return nil
}

// `MSetNX` 将给定的键设置为它们相应的值。
// 即使只有一个键已经存在，`MSetNX` 也不会执行任何操作。
// mset key1 value1 key2 value2......
func (db *RoseDB) MSetNX(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		if val != nil {
			return nil
		}
	}

	// 只记录key， 若key存在则直接跳过
	var addedKeys = make(map[uint64]struct{})

	for i := 0; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		h := util.MemHash(key)
		if _, ok := addedKeys[h]; ok {
			continue
		}
		entry := &logfile.LogEntry{Key: key, Value: value}
		valPos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		err = db.updateIndexTree(db.strIndex.idxTree, entry, valPos, true, String)
		if err != nil {
			return err
		}
		addedKeys[h] = struct{}{}
	}
	return nil
}

func (db *RoseDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}

	if oldVal != nil {
		value = append(value, oldVal...)
	}

	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return err
}

// Decr 将存储在键处的数字减一。
// 如果键不存在，在执行操作之前会将其设置为0。
// 如果值不是整数类型，则返回 ErrWrongKeyType 错误。
// 此外，如果在减少值后超过最大整数值，它将返回 ErrIntegerOverflow 错误。
// decr key
func (db *RoseDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)
}

func (db *RoseDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -decr)
}

func (db *RoseDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}

func (db *RoseDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}

func (db *RoseDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}
	// 将获取到的值解析为int64的对象
	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}
	// 检查是否产生溢出
	if (incr < 0 && valInt64 < 0 && incr < (math.MinInt64-valInt64)) ||
		(incr > 0 && valInt64 > 0 && incr > (math.MaxInt64-valInt64)) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	entry := &logfile.LogEntry{Key: key, Value: val}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// strlen key
func (db *RoseDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(val)
}

func (db *RoseDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0
	}

	return db.strIndex.idxTree.Size()
}

// pattern 匹配模式   返回匹配的key value
func (db *RoseDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	var reg *regexp.Regexp
	// 如果匹配模式不为空， 则编译匹配模式为正则表达式对象
	if pattern != "" {
		var err error
		if reg, err = regexp.Compile(pattern); err != nil {
			return nil, err
		}
	}
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	if db.strIndex.idxTree == nil {
		return nil, nil
	}
	// 根据前缀扫描数据库中的键，并返回符合条件的键数组
	keys := db.strIndex.idxTree.PrefixScan(prefix, count)
	if len(keys) == 0 {
		return nil, nil
	}

	var results [][]byte
	for _, key := range keys {
		if reg != nil && !reg.Match(key) {
			continue
		}
		val, err := db.getVal(db.strIndex.idxTree, key, String)
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		if err != ErrKeyNotFound {
			results = append(results, key, val)
		}
	}
	return results, nil
}

// 设置过期时间
// expire key time
func (db *RoseDB) Expire(key []byte, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.SetEX(key, val, duration)
}

// 获取当前key的所剩时间
// ttl key
func (db *RoseDB) TTL(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	node, err := db.getIndexNode(db.strIndex.idxTree, key)
	if err != nil {
		return 0, err
	}
	var ttl int64
	if node.expiredAt != 0 {
		ttl = node.expiredAt - time.Now().Unix()
	}
	return ttl, nil
}

// Persist remove the expiration time for the given key
// persist key
func (db *RoseDB) Persist(key []byte) error {
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.Set(key, val)
}

// get all keys of String
func (db *RoseDB) GetStrsKeys() ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return nil, nil
	}

	var keys [][]byte
	iter := db.strIndex.idxTree.Iterator()
	ts := time.Now().Unix()
	for iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return nil, err
		}
		indexNode, _ := node.Value().(*indexNode)
		if indexNode == nil {
			continue
		}
		if indexNode.expiredAt != 0 && indexNode.expiredAt <= ts {
			continue
		}
		keys = append(keys, node.Key())
	}
	return keys, nil
}

// Cas Compare and Set. if current value of key is the same as oldValue,set newValue
func (db *RoseDB) Cas(key, oldValue, newValue []byte) (bool, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	curValue, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return false, nil
	}

	if !bytes.Equal(oldValue, curValue) {
		return false, nil
	}

	entry := &logfile.LogEntry{Key: key, Value: newValue}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return false, err
	}

	err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos, true, String)
	return true, err
}

func (db *RoseDB) Cad(key, delvalue []byte) (bool, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	curValue, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(curValue, delvalue) {
		return false, nil
	}
	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return false, err
	}

	val, updated := db.strIndex.idxTree.Delete(key)
	db.sendDiscard(val, updated, String)
	_, size := logfile.EncodeEntry(entry)
	node := &indexNode{fid: pos.fid, entrySize: size}
	select {
	case db.discards[String].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return true, nil
}
