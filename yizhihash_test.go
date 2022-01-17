package hash

import (
	"fmt"
	"testing"
)

func Test(t *testing.T){
	var cen = map[string]int{}
	cen["sh"] = 70
	cen["bj"] = 30
	hashCircleInit(cen)
	fmt.Println("center:",hashGetCen("123"))
	var cen1 = map[string]int{}
	cen1["sh"] = 60
	//cen1["bj"] = 40
	cen1["wg"] = 40
	_ = hashReBuild(cen1)
}