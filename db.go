package rose

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"rose/ds/art"
	"rose/ds/zset"
	"rose/flock"
	"rose/logfile"
	"rose/logger"
	"rose/util"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrKeyNotFound
	ErrKeyNotFound = errors.New("key not found")
	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")
	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
	// ErrIntegerOverflow overflows int64 limitations
	ErrIntegerOverflow = errors.New("increment or decrement overflow")
	// ErrWrongValueType value is not a number
	ErrWrongValueType = errors.New("value is not an integer")
	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")
	// ErrGCRunning log file gc is running
	ErrGCRunning = errors.New("log file gc is running, retry later")
)

const (
	logFileTypeNum   = 5
	encodeHeaderSize = 10
	//对于list结构， 初始列表的序号
	initialListSeq  = math.MaxUint32 / 2
	discardFilePath = "DISCARD"
	lockFileName    = "FLOCK"
)

type (
	// RoseDB a db instance.
	RoseDB struct {
		activeLogFiles   map[DataType]*logfile.LogFile
		archivedLogFiles map[DataType]archivedFiles
		fidMap           map[DataType][]uint32 // only used at startup, never update even though log files changed.
		discards         map[DataType]*discard
		opts             Options
		strIndex         *strIndex  // String indexes(adaptive-radix-tree).
		listIndex        *listIndex // List indexes.
		hashIndex        *hashIndex // Hash indexes.
		setIndex         *setIndex  // Set indexes.
		zsetIndex        *zsetIndex
		mu               sync.RWMutex
		fileLock         *flock.FileLockGuard
		closed           uint32
		gcState          int32
	}

	archivedFiles map[uint32]*logfile.LogFile

	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	listIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet // skiplist + sortset
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewArt(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newHashIdx() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIdx() *setIndex {
	return &setIndex{
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{
		indexes: zset.New(),
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

// Open a rose instance
func Open(opts Options) (*RoseDB, error) {
	// create the dir path if not exists.
	// 如果文件不存在则创建文件
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, lockFileName)
	//尝试获取一个文件锁
	lockGuard, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &RoseDB{
		activeLogFiles:   make(map[DataType]*logfile.LogFile),
		archivedLogFiles: make(map[DataType]archivedFiles),
		opts:             opts,
		fileLock:         lockGuard,
		strIndex:         newStrsIndex(),
		listIndex:        newListIdx(),
		hashIndex:        newHashIdx(),
		setIndex:         newSetIdx(),
		zsetIndex:        newZSetIdx(),
	}

	// init discard file.
	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	// load the log files from disk.
	if err := db.loadLogFiles(); err != nil {
		return nil, err
	}

	// load indexes from log files.
	if err := db.loadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// handle log files garbage collection.
	go db.handleLogFileGC()
	return db, nil
}

func (db *RoseDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeLogFiles[dataType]
}

func (db *RoseDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFiles[dataType] != nil {
		lf = db.archivedLogFiles[dataType][fid]
	}
	return lf
}

func (db *RoseDB) initDiscard() error {
	// 拼接
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	// 对每一个类型进行操作
	for i := String; i < logFileTypeNum; i++ {
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		//创建discard
		dis, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			return err
		}
		discards[i] = dis
	}

	db.discards = discards
	return nil
}

// 加载日志文件 ，对每一个数据类型创建归档logfile和活跃的logfile
// 只实例化logfile对象，不读取其内容
func (db *RoseDB) loadLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	fileInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}
	fidMap := make(map[DataType][]uint32)
	for _, file := range fileInfos {
		// rosedb 的 logfile 文件名前缀是 `log.type`，
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			// 获取logfile的fid
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			// 获取logfile的datatype
			typ := DataType(logfile.FileTypesMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}

	db.fidMap = fidMap

	// 遍历各个datatype的logfile列表
	for dataType, fids := range fidMap {
		// 先查看并创建每个类型的日志文件
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}
		// load logfile in order  旧的日志文件在前面
		sort.Slice(fids, func(i, j int) bool {
			return fids[i] < fids[j]
		})
		opts := db.opts
		for i, fid := range fids {
			ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.Iotype)

			lf, err := logfile.OpenLogFile(opts.DBPath, fid, opts.LogFileSizeThreshold, ftype, iotype)
			if err != nil {
				return err
			}

			// 最后的一个文件为活跃的logfile
			if i == len(fids)-1 {
				db.activeLogFiles[dataType] = lf
			} else {
				db.archivedLogFiles[dataType][fid] = lf
			}
		}
	}
	return nil
}

func (db *RoseDB) sendDiscard(oldVal interface{}, updated bool, dataType DataType) {
	if !updated || oldVal == nil {
		return
	}
	node, _ := oldVal.(*indexNode)
	if node == nil || node.entrySize <= 0 {
		return
	}
	select {
	case db.discards[dataType].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
}

// 将所有数据备份到指定的目录下
func (db *RoseDB) Backup(path string) error {
	// if log file gc is running, can not backup the db.
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGCRunning
	}

	if err := db.Sync(); err != nil {
		return err
	}
	if !util.PathExist(path) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return util.CopyDir(db.opts.DBPath, path)
}
func (db *RoseDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, activeFile := range db.activeLogFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	for _, dis := range db.discards {
		if err := dis.sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *RoseDB) handleLogFileGC() {
	// 判断gc的频率参数是否有效
	if db.opts.LogFileGCInterval <= 0 {
		return
	}

	quitSig := make(chan os.Signal, 1)
	signal.Notify(quitSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				logger.Warn("log file gc is running, skip it")
				break
			}

			// 遍历每个类型 执行gc
			for dType := String; dType < logFileTypeNum; dType++ {
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1, db.opts.LogFileGCRatio)
					if err != nil {
						logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(dType)
			}
		case <-quitSig:
			return
		}
	}
}

// 传入entry和数据类型
func (db *RoseDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	//初始化logfile 查找或创建活跃的logfile文件
	if err := db.initLogFile(dataType); err != nil {
		return nil, err
	}
	// 获取对应类型的logfile
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}

	opts := db.opts
	//编码entry为bytes字节数组
	entBuf, esize := logfile.EncodeEntry(ent)

	// 判断当前logfile的size是否和entry相加大于阈值，若大于则需要构建一个新的logfile日志文件
	if activeLogFile.WriteAt+int64(esize) > opts.LogFileSizeThreshold {
		// 写盘
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}

		db.mu.Lock()
		//将当前的activelogfile放到归档集合中
		activeFileId := activeLogFile.Fid //记录当前logfile的fid
		if db.archivedLogFiles[dataType] == nil {
			db.archivedLogFiles[dataType] = make(archivedFiles)
		}
		db.archivedLogFiles[dataType][activeFileId] = activeLogFile

		ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.Iotype)
		lf, err := logfile.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, ftype, iotype)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		// 跟踪记录discards
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		// 将生成的logfile文件加入到活跃文件的集合中
		db.activeLogFiles[dataType] = lf
		activeLogFile = lf
		db.mu.Unlock()
	}

	// 计算当前偏移量
	writeAt := atomic.LoadInt64(&activeLogFile.WriteAt)
	// write entry and sync(if necessary)
	if err := activeLogFile.Write(entBuf); err != nil {
		return nil, err
	}

	// 若开启了实时同步
	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &valuePos{fid: activeLogFile.Fid, offset: writeAt}, nil
}

func (db *RoseDB) initLogFile(dataType DataType) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 若当前的活跃的logfile存在则直接return
	if db.activeLogFiles[dataType] != nil {
		return nil
	}
	opts := db.opts
	ftype, iotype := logfile.FileType(dataType), logfile.IOType(opts.Iotype)
	lf, err := logfile.OpenLogFile(opts.DBPath, logfile.InitialLogFileId, opts.LogFileSizeThreshold, ftype, iotype)
	if err != nil {
		return err
	}
	db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	db.activeLogFiles[dataType] = lf
	return nil
}

// return key field
func (db *RoseDB) decodeKey(key []byte) ([]byte, []byte) {
	var index int
	//读取key 返回长度和占用字节数
	keySize, i := binary.Varint(key[index:])
	index += i
	// 读取 field 返回占用字节数
	_, i = binary.Varint(key[index:])
	index += i
	// 计算分隔符的位置
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}

func (db *RoseDB) doRunGC(dataType DataType, specifiedFid int, gcRatio float64) error {
	// 原子更新 gcState 值，1 为正在进行垃圾回收，0 为空闲中.
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()

		// 获取key关联的radix node
		indexVal := db.strIndex.idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		// 如果在内存索引中有该记录，且 fid 和 offset 都是一样的，那么则需要把该数据写到当前的活跃的 logfile 日志文件里.
		if node != nil && node.fid == fid && node.offset == offset {
			// 进行重写，把该 entry 到日志文件里
			valuePos, err := db.writeLogEntry(ent, String)
			if err != nil {
				return err
			}
			// 更新内存的索引值，使用新的 valuePos 来关联 key.
			if err = db.updateIndexTree(db.strIndex.idxTree, ent, valuePos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteList := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()
		var listKey = ent.Key
		if ent.Type != logfile.TypeListMeta {
			listKey, _ = db.decodeListKey(ent.Key)
		}

		if db.listIndex.trees[string(listKey)] == nil {
			return nil
		}
		idxTree := db.listIndex.trees[string(listKey)]
		indexVal := idxTree.Get(ent.Key)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, ent, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()
		key, field := db.decodeKey(ent.Key)
		if db.hashIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.hashIndex.trees[string(key)]
		indexVal := idxTree.Get(field)
		if indexVal == nil {
			return nil
		}

		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, Hash)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: field, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Hash); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()
		if db.setIndex.trees[string(ent.Key)] == nil {
			return nil
		}
		idxTree := db.setIndex.trees[string(ent.Key)]
		if err := db.setIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			// rewrite entry
			valuePos, err := db.writeLogEntry(ent, Set)
			if err != nil {
				return err
			}
			// update index
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Set); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSet := func(fid uint32, offset int64, ent *logfile.LogEntry) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()
		key, _ := db.decodeKey(ent.Key)
		if db.zsetIndex.trees[string(key)] == nil {
			return nil
		}
		idxTree := db.zsetIndex.trees[string(key)]
		if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
			logger.Fatalf("fail to write murmur hash: %v", err)
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()

		indexVal := idxTree.Get(sum)
		if indexVal == nil {
			return nil
		}
		node, _ := indexVal.(*indexNode)
		if node != nil && node.fid == fid && node.offset == offset {
			valuePos, err := db.writeLogEntry(ent, Zset)
			if err != nil {
				return err
			}
			entry := &logfile.LogEntry{Key: sum, Value: ent.Value}
			_, size := logfile.EncodeEntry(ent)
			valuePos.entrySize = size
			if err = db.updateIndexTree(idxTree, entry, valuePos, false, Zset); err != nil {
				return err
			}
		}
		return nil
	}

	// 根据传入的dataType获取对应的当前获取的logfile
	activeLogFile := db.getActiveLogFile(dataType)

	// rosedb 是懒惰式实例化 activeLogFile 的，如果 rosedb 启动后一直无写入，那么就无需实例化活跃的 logfile.
	if activeLogFile == nil {
		return nil
	}

	// 保证数据安全，将discards的数据进行同步落盘
	if err := db.discards[dataType].sync(); err != nil {
		return err
	}

	//获取符合垃圾回收阈值的logFile的id列表
	ccl, err := db.discards[dataType].getCCL(activeLogFile.Fid, gcRatio)
	if err != nil {
		return err
	}

	for _, fid := range ccl {
		//// 如果是手动触发的垃圾回收，需要校验传入的 fid 是否合法.
		//		// 还有如果不匹配, 则忽略.
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			continue
		}
		// 从归档集合里获取 fid 和 dataType 对应的 logfile 对象
		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}

		var offset int64
		for {
			ent, size, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == io.EOF || err == logfile.ErrEndOfEntry {
					break
				}
				return err
			}

			// 累加偏移量
			var off = offset
			offset += size
			if ent.Type == logfile.TypeDelete {
				continue
			}
			ts := time.Now().Unix()
			if ent.ExpiredAt != 0 && ent.ExpiredAt <= ts {
				continue
			}
			var rewriteErr error
			// doRunGC 方法内部定义了多个匿名方法，这里会根据 dataType 的类型调用不同的处理方法.
			switch dataType {
			case String:
				rewriteErr = maybeRewriteStrs(archivedFile.Fid, off, ent)
			case List:
				rewriteErr = maybeRewriteList(archivedFile.Fid, off, ent)
			case Hash:
				rewriteErr = maybeRewriteHash(archivedFile.Fid, off, ent)
			case Set:
				rewriteErr = maybeRewriteSets(archivedFile.Fid, off, ent)
			case Zset:
				rewriteErr = maybeRewriteZSet(activeLogFile.Fid, off, ent)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}
		db.mu.Lock()
		// 删除旧的logfile，该旧的 logfile 已被合并回收了，则可以删除旧的 logfile.
		delete(db.archivedLogFiles[dataType], fid)
		_ = archivedFile.Delete()
		db.mu.Unlock()
		// 既然 logfile 都被删除了，自然要干掉关联的 discard 统计信息.
		db.discards[dataType].clear(fid)
	}
	return nil
}

func (db *RoseDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.fileLock != nil {
		//关闭文件锁
		_ = db.fileLock.Release()
	}

	// 对每个datype的活跃文件执行同步落盘和关闭操作
	for _, activeFile := range db.activeLogFiles {
		_ = activeFile.Close()
	}
	// close the archived files.
	for _, archived := range db.archivedLogFiles {
		for _, file := range archived {
			_ = file.Sync()
			_ = file.Close()
		}
	}
	for _, dis := range db.discards {
		dis.closeChan()
	}

	atomic.StoreUint32(&db.closed, 1)
	db.strIndex = nil
	db.hashIndex = nil
	db.listIndex = nil
	db.setIndex = nil
	db.zsetIndex = nil
	return nil
}

func (db *RoseDB) RunLogFileGC(dataType DataType, fid int, gcRatio float64) error {
	if atomic.LoadInt32(&db.gcState) > 0 {
		return ErrGCRunning
	}
	return db.doRunGC(dataType, fid, gcRatio)
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf[:index], header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		return buf
	}
	return header[:index]
}
