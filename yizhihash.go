package hash

//一致性哈希(Consistent Hashing)分流
//author: dindin666666
//date: 2022-1-15

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var RatioSh = 0
var RatioBj = 0
var WghMap = map[string]int{}
var cHashRing *Consistent

const DEFAULT_REPLICAS = 1

type HashRing []uint32

func (c HashRing) Len() int {
	return len(c)
}

func (c HashRing) Less(i, j int) bool {
	return c[i] < c[j]
}

func (c HashRing) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type Node struct {
	site string
	Weight   int
}

func NewNode(name string, weight int) *Node {
	return &Node{
		site: name,
		Weight:   weight,
	}
}

type Consistent struct {
	Nodes     map[uint32]Node
	numReps   int
	Resources map[string]bool
	ring      HashRing
	sync.RWMutex
}

func NewConsistent() *Consistent {
	return &Consistent{
		Nodes:     make(map[uint32]Node),
		numReps:   DEFAULT_REPLICAS,
		Resources: make(map[string]bool),
		ring:      HashRing{},
	}
}

func (c *Consistent) Add(node *Node) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.site]; ok {
		return false
	}

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		c.Nodes[c.hashStr(str)] = *(node)
	}
	c.Resources[node.site] = true
	c.sortHashRing()
	return true
}

func (c *Consistent) AddWgh(node *Node,n1 int,n2 int) bool {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.site]; !ok {
		return false
	}

	//count := c.numReps * node.Weight
	for i := n1; i < n2; i++ {
		str := c.joinStr(i, node)
		c.Nodes[c.hashStr(str)] = *(node)
	}
	c.Resources[node.site] = true
	c.sortHashRing()
	return true
}

func (c *Consistent) sortHashRing() {
	c.ring = HashRing{}
	for k := range c.Nodes {
		c.ring = append(c.ring, k)
	}
	sort.Sort(c.ring)
}

func (c *Consistent) joinStr(i int, node *Node) string {
	return node.site + "*" +
		"-" + strconv.Itoa(i)
}

// MurMurHash算法 :https://github.com/spaolacci/murmur3
func (c *Consistent) hashStr(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) Get(key string) Node {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashStr(key)
	i := c.search(hash)

	return c.Nodes[c.ring[i]]
}

func (c *Consistent) search(hash uint32) int {

	i := sort.Search(len(c.ring), func(i int) bool { return c.ring[i] >= hash })
	if i < len(c.ring) {
		if i == len(c.ring)-1 {
			return 0
		} else {return i}
	} else {
		return len(c.ring) - 1
	}
}

func (c *Consistent) Remove(node *Node) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.site]; !ok {
		return
	}

	delete(c.Resources, node.site)

	count := c.numReps * node.Weight
	for i := 0; i < count; i++ {
		str := c.joinStr(i, node)
		delete(c.Nodes, c.hashStr(str))
	}
	c.sortHashRing()
}

func (c *Consistent) RemoveWgh(node *Node, n1 int,n2 int) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.Resources[node.site]; !ok {
		return
	}

	//delete(c.Resources, node.site)

	//count := c.numReps * node.Weight
	for i := n1; i < n2; i++ {
		str := c.joinStr(i, node)
		delete(c.Nodes, c.hashStr(str))
	}
	c.sortHashRing()
}

//根据传入的包含分流节点和比例的map，初始化hash环
func hashCircleInit(cenWgh map[string]int)(flag bool) {

	cHashRing = NewConsistent()
	
	//将分流比例百分比化
	var sum int =0

	for _,v :=range cenWgh{
		sum+=v
	}

	for k,v :=range cenWgh{
		cenWgh[k] = int((float32(v) / float32(sum)) * 100)
	}
	//往hash环中新增节点包括虚拟节点
	for k,v := 	range cenWgh{
		cHashRing.Add(NewNode(k, v))
	}

	var tmpsh int = 0
	var tmpbj int = 0
	for _, v := range cHashRing.Nodes {
		//fmt.Println("Hash:", k, " IP:", v.site)
		if v.site == "sh" { tmpsh++}
		if v.site == "bj" { tmpbj++}
	}
	fmt.Println("sh",tmpsh,"bj:",tmpbj)
	//将初始化比例保留用于后续hash环调整
	WghMap = cenWgh
	return true
}

//根据主键获取对应的分流节点
func hashGetCen(appId string)(site string){
	k := cHashRing.Get(appId)
	//fmt.Println(k.site)
	return k.site
}

//重构hash环
func hashReBuild(cenWgh map[string]int)(flag bool){
	//1.新增节点
	WghMapTmp := WghMap
	cenWghTmp := cenWgh
	for k, v := range cenWgh{
		_,ok:= WghMapTmp[k]
		//if !ok { delete(cenWghTmp, k) }
		//3.比例修改
		if ok {
			if WghMapTmp[k] > v{
				cHashRing.RemoveWgh(NewNode(k, v),v,WghMapTmp[k])
			}else{
				cHashRing.AddWgh(NewNode(k, v),WghMapTmp[k],v)
			}
		}

		if !ok {
			cHashRing.Add(NewNode(k, v))
			WghMapTmp[k] = v
		}
	}

	//2.删除节点
	for k,v := range WghMap{
		_,ok:= cenWghTmp[k]
		if !ok{
			cHashRing.Remove(NewNode(k, v))
			delete(cenWghTmp, k)
		}
	}
	WghMap = WghMapTmp
	cenWgh = cenWghTmp

	var tmpsh int = 0
	var tmpbj int = 0
	var tmpwg int = 0
	for _, v := range cHashRing.Nodes {
		//fmt.Println("Hash:", k, " IP:", v.site)
		if v.site == "sh" { tmpsh++}
		if v.site == "bj" { tmpbj++}
		if v.site == "wg" { tmpwg++}
	}
	fmt.Println("sh",tmpsh,"bj:",tmpbj,"wg",tmpwg)

	return true
}



