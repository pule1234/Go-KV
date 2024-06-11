package rose

import (
	"io"
	"rose/ds/art"
	"rose/logfile"
	"rose/logger"
	"rose/util"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type DataType = int8

const (
	String DataType = iota
	List
	Hash
	Set
	Zset
)

// 建立不同类型的索引树
func (db *RoseDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent, pos)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetsIndex(ent, pos)
	case Zset:
		db.buildZSetIndex(ent, pos)
	}
}

func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
	var listKey = ent.Key
	if ent.Type != logfile.TypeListMeta {
		listKey, _ = db.decodeListKey(ent.Key)
	}
	// 若不存在， 为该key创建一个索引树
	if db.listIndex.trees[string(listKey)] == nil {
		db.listIndex.trees[string(listKey)] = art.NewArt()
	}
	idxTree := db.listIndex.trees[string(listKey)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxNode)
}

func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	key, field := db.decodeKey(ent.Key)
	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewArt()
	}
	idxTree := db.hashIndex.trees[string(key)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(field)
		return
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(field, idxNode)
}

func (db *RoseDB) buildSetsIndex(ent *logfile.LogEntry, pos *valuePos) {
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewArt()
	}
	idxTree := db.setIndex.trees[string(ent.Key)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Value)
		return
	}

	if err := db.setIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}

	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()
	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(sum, idxNode)
}

func (db *RoseDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	if ent.Type == logfile.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		if db.zsetIndex.trees[string(ent.Key)] != nil {
			db.zsetIndex.trees[string(ent.Key)].Delete(ent.Value)
		}
		return
	}

	key, scoreBuf := db.decodeKey(ent.Key)
	score, _ := util.StrToFloat64(string(scoreBuf))
	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		logger.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()
	idxTree := db.zsetIndex.trees[string(key)]
	if idxTree == nil {
		idxTree = art.NewArt()
		db.zsetIndex.trees[string(key)] = idxTree
	}

	_, size := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	idxTree.Put(sum, idxNode)
}

// 读取logfile中的数据， 并添加到索引中
func (db *RoseDB) loadIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		//排序
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})

		for i, fid := range fids {
			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activeLogFiles[dataType]
			} else {
				logFile = db.archivedLogFiles[dataType][fid]
			}
			if logFile == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			// 初始化偏移量
			var offset int64
			for {
				//从0开始，从偏移量读取entry， 其内部先读取entry header，再根据ksize和vsize获取kv键值数据
				entry, esize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += esize
			}
			// set latest log file`s WriteAt.
			//// 如果遍历到最新的 logfile, 则原子保存当前的活跃日志文件的 offset.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

func (db *RoseDB) updateIndexTree(idxTree *art.AdaptiveRadixTree,
	ent *logfile.LogEntry, pos *valuePos, sendDiscard bool, dType DataType) error {

	var size = pos.entrySize
	if dType == String || dType == List {
		_, size = logfile.EncodeEntry(ent)
	}

	// 构建索引的 node 节点对象, node 结构包含 value 在磁盘文件上的位置信息
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
	// 如果是全内存模式, 则需要保存 value 值, KeyOnlyMemMode 则只需要存 value 的偏移量信息。
	if db.opts.IndexMode == KeyValueMemMode {
		idxNode.value = ent.Value
	}

	// 记录过期时间
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}

	// 将key和index node 插入到radix tree索引中
	oldVal, updated := idxTree.Put(ent.Key, idxNode)
	if sendDiscard {
		// 通过 discard 记录删除值.
		db.sendDiscard(oldVal, updated, dType)
	}
	return nil
}

func (db *RoseDB) getVal(idxTree *art.AdaptiveRadixTree,
	key []byte, dataType DataType) ([]byte, error) {
	// 先从索引树中获取indexnode
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	// 判断是否过期
	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		return nil, ErrKeyNotFound
	}

	// 如果是内存模式，则直接返回node的value
	if db.opts.IndexMode == KeyValueMemMode && len(idxNode.value) != 0 {
		return idxNode.value, nil
	}

	// 判断存储模式, 如果是内存模式，只存储valuepos，也就是磁盘文件的偏移量
	logFile := db.getActiveLogFile(dataType)
	if logFile.Fid != idxNode.fid {
		//和活跃的logFIle文件对应不上
		logFile = db.getArchivedLogFile(dataType, idxNode.fid)
	}
	if logFile == nil {
		return nil, ErrLogFileNotFound
	}

	// 从logFile里获取编解码后的kv数据
	ent, _, err := logFile.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}

	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}
	return ent.Value, nil
}

func (db *RoseDB) getIndexNode(idxTree *art.AdaptiveRadixTree, key []byte) (*indexNode, error) {
	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	idxNode, _ := rawValue.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}
	return idxNode, nil
}
