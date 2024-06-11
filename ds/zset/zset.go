package zset

import (
	"math"
	"math/rand"
	"rose/logfile"
	"rose/util"
)

const (
	maxLevel    = 32
	probability = 0.25
)

type EncodeKey func(key, subKey []byte) []byte

type (
	// 有序集合
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	// 有序集合的节点
	SortedSetNode struct {
		dict map[string]*sklNode // 以member为key
		skl  *skipList
	}

	// 跳表的层级信息
	sklLevel struct {
		forward *sklNode // 后一个节点
		span    uint64   // 当前节点与前一个节点直接的跨度
	}

	//跳表的节点
	sklNode struct {
		member   string      // 成员对象
		score    float64     // 权重值
		backword *sklNode    // 前一个节点的指针
		level    []*sklLevel // 该节点的层级信息
	}

	skipList struct {
		head   *sklNode // 头节点
		tail   *sklNode // 尾节点
		length int64    //跳表的长度
		level  int16    // 层级数
	}
)

func New() *SortedSet {
	return &SortedSet{
		record: make(map[string]*SortedSetNode),
	}
}

// 接收日志条目chn ，
func (z *SortedSet) IterateAndSend(chn chan *logfile.LogEntry, encode EncodeKey) {
	for key, ss := range z.record {
		// 将键名转换成zsetkey
		zsetKey := []byte(key)
		// 检查当前节点是否为空
		if ss.skl.head == nil {
			return
		}
		// 从当前节点开始，一直到末尾
		for e := ss.skl.head.level[0].forward; e != nil; e = e.level[0].forward {
			// 将每个节点的权重score转换成字节数组
			scoreBuf := []byte(util.Float64ToStr(e.score))
			// 编码key和权重为enckey
			enckey := encode(zsetKey, scoreBuf)
			chn <- &logfile.LogEntry{Key: enckey, Value: []byte(e.member)}
		}
	}
	return
}

func (z *SortedSet) ZAdd(key string, score float64, member string) {
	//当前节点不存在时， 则创建该key对应的node
	if !z.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]

	var node *sklNode
	if exist {
		// 如果存在， 但是不相等，则修改跳表上的值
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	} else {
		node = item.skl.sklInsert(score, member)
	}

	if node != nil {
		// 将添加生成的节点，存到map对应的key中
		item.dict[member] = node
	}
}

func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}
	// 获取节点
	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}
	return true, node.score
}

// ZCard返回存储在键中的有序集合的基数（元素数量）
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

// ZRank返回存储在键中的有序集合中成员的排名（索引），分数从低到高排序。
// 排名（或索引）是从0开始计算的，这意味着具有最低分数的成员排名为0。
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}
	// 拿到当前node
	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}
	// 获取排名
	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank--
	return rank
}

func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)
	// rev！  从尾部开始
	return z.record[key].skl.length - rank
}

// ZIncrBy函数将存储在键中的有序集合中成员的分数增加指定的增量。
// 如果成员在有序集合中不存在，则将其分数增加到增量值（就好像其先前的分数为0.0）。
// 如果键不存在，则创建一个具有指定成员作为唯一成员的新有序集合。
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}
	z.ZAdd(key, increment, member)
	return increment
}

func (z *SortedSet) ZRange(key string, start, stop int) []interface{} {
	// 当前key不存在直接返回
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, false)
}

func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}

// 反过来
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}

func (z *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}

// 在跳表上删除节点
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}
	// 获取节点
	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}
	return false
}

func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	// 先找到最小值的前一个位置
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}

	return
}

// ZRevScoreRange返回存储在键中的有序集合中分数介于max和min之间（包括分数等于max或min的元素）的所有元素。
// 与有序集合的默认排序相反，对于此命令，元素被认为是按分数从高到低排序的。
func (z *SortedSet) ZRevSocreRange(key string, max, min float64) (val []interface{}) {
	if !z.exist(key) || max < min {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}

		val = append(val, p.member, p.score)
		p = p.backword
	}
	return
}

// ZKeyExists check if the key exists in zset.
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// ZClear clear the key in zset.
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

// 根据rank 获取 member 和 score
func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {
	skl := z.record[key].skl
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}

	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}
	return node.member, node.score
}

func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	//获取该key对应的跳表
	skl := z.record[key].skl
	length := skl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	// 不正确参数
	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}

	span := (stop - start) + 1

	var node *sklNode
	// 按照reverse 向node赋值， node为第一个元素
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		// node 向下一个移动
		if reverse {
			node = node.backword
		} else {
			node = node.level[0].forward
		}
	}
	return
}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {
			rank += p.level[i].span
			p = p.level[i].forward
		}
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

// 跳表上的节点删除
func (skl *skipList) sklDelete(score float64, member string) {
	update := make([]*sklNode, maxLevel)
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		// 找到需要删除的节点
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		update[i] = p
	}

	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		// 删除节点
		skl.sklDeleteNode(p, update)
		return
	}
}

// 接受一个要删除的节点和该节点的前驱节点信息数组update
func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		// 检查当前层中的前驱节点的下一个节点是否为要删除的节点
		if updates[i].level[i].forward == p {
			updates[i].level[i].span += p.level[i].span - 1  // 更新跨度
			updates[i].level[i].forward = p.level[i].forward // 舍去p， 链接到p的下一个节点上
		} else {
			updates[i].level[i].span--
		}
	}

	// 检查要删除的节点在第0层的下一个节点是否存在
	if p.level[0].forward != nil {
		p.level[0].forward.backword = p.backword
	} else {
		skl.tail = p.backword
	}

	// 删除多余的层，直到跳表的高度，不超过实际高度
	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	skl.length--
}

// 跳表插入的实现
func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	// 用于保存每层需要更新的节点
	updates := make([]*sklNode, maxLevel)
	//保存每一层的排名信息
	rank := make([]uint64, maxLevel)

	p := skl.head
	// 从最顶层开始往下遍历
	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		// 判断当前的p是否存在第i层，当前层是否存在节点
		if p.level[i] != nil {
			//找到合适的插入位置， score从小到大， 当score相等，则比较member
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
				// 排名增加
				rank[i] += p.level[i].span
				p = p.level[i].forward
			}
		}
		updates[i] = p
	}

	level := randomLevel()
	if level > skl.level {
		// 若随机出来的高度超过当前高度, 则更新高度
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	// 创建一个新的节点
	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		// 链接新节点到跳表
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		// 计算跨度
		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head {
		//若第一个为头节点， 则没有上一个节点
		p.backword = nil
	} else {
		p.backword = updates[0]
	}

	if p.level[0].forward != nil {
		// 若当前层p的下一个节点存在，则插入p
		p.level[0].forward.backword = p
	} else {
		skl.tail = p
	}

	skl.length++
	return p

}

// 根据node 获取节点
func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	// 用于记录在跳表层级中已经遍历过的总跨度
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		//循环跳出判断   不是最后一个节点 && 总跨度不大于rank
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		// 按照跨度查找， 相等时，代表该元素存在
		if traversed == rank {
			return p
		}
	}
	return nil
}

func randomLevel() int16 {
	var level int16 = 1
	for level < maxLevel {
		if rand.Float64() < probability {
			break
		}
		level++
	}
	return level
}

func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}
	for i := range node.level {
		node.level[i] = new(sklLevel)
	}
	return node
}
