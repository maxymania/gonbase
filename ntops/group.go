/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package ntops

import "github.com/maxymania/gonbase/newtree"
import "github.com/vmihailenco/msgpack"
import "errors"
import "sync"
import "sort"
import "math"

var EIsSumary = errors.New("IsSumary")

var groupEntryBuffers = sync.Pool{ New:func()interface{} { return []byte(nil) } }

type GroupEntry struct{
	GroupID uint64
	Article uint64
	Expires uint64
	Value   []byte
}
func (g *GroupEntry) Marshal() []byte {
	data,_ := msgpack.Marshal(false,g)
	return data
}
func (g *GroupEntry) DecodeMsgpack(src *msgpack.Decoder) error {
	var err1,err2,err3,err4 error
	g.GroupID,err1 = src.DecodeUint64()
	g.Article,err2 = src.DecodeUint64()
	g.Expires,err3 = src.DecodeUint64()
	err4 = src.Decode(&g.Value)
	
	if err1==nil { err1 = err2 }
	if err3==nil { err3 = err4 }
	
	if err1==nil { err1 = err3 }
	return err1
	//return src.Decode(&g.GroupID,&g.Article,&g.Expires,&g.Value)
}
func (g *GroupEntry) EncodeMsgpack(dst *msgpack.Encoder) error {
	return dst.Encode(g.GroupID,g.Article,g.Expires,g.Value)
}

type groupSumary struct{
	GroupLow    uint64
	GroupHigh   uint64
	ArticleLow  uint64
	ArticleHigh uint64
	ExpiresLow  uint64
	ExpiresHigh uint64
	Count       uint64
}
func (g *groupSumary) DecodeMsgpack(src *msgpack.Decoder) error {
	var err1,err2,err3,err4,err5,err6 error
	
	g.GroupLow   ,err1 = src.DecodeUint64()
	g.GroupHigh  ,err2 = src.DecodeUint64()
	g.ArticleLow ,err3 = src.DecodeUint64()
	g.ArticleHigh,err4 = src.DecodeUint64()
	g.ExpiresLow ,err5 = src.DecodeUint64()
	g.ExpiresHigh,err6 = src.DecodeUint64()
	
	if err1==nil { err1 = err2 }
	if err3==nil { err3 = err4 }
	if err5==nil { err5 = err6 }
	
	if err1==nil { err1 = err3 }
	if err1==nil { return err5 }
	return err1
	//return src.Decode(&g.GroupLow,&g.GroupHigh,&g.ArticleLow,&g.ArticleHigh,&g.ExpiresLow,&g.ExpiresHigh,&g.Count)
}
func (g *groupSumary) EncodeMsgpack(dst *msgpack.Encoder) error {
	return dst.Encode(g.GroupLow,g.GroupHigh,g.ArticleLow,g.ArticleHigh,g.ExpiresLow,g.ExpiresHigh,g.Count)
}

type groupGeneral struct{
	IsSumary bool
	GE GroupEntry
	GS groupSumary
}
func (g *groupGeneral) DecodeMsgpack(src *msgpack.Decoder) error {
	var err error
	g.IsSumary,err = src.DecodeBool()
	if err!=nil { return err }
	if g.IsSumary {
		return g.GS.DecodeMsgpack(src)
	} else {
		/* Memory-Optimization. */
		if cap(g.GE.Value)==0 {
			g.GE.Value = groupEntryBuffers.Get().([]byte)
		}
		err = g.GE.DecodeMsgpack(src)
		g.GS.GroupLow    = g.GE.GroupID
		g.GS.GroupHigh   = g.GE.GroupID
		g.GS.ArticleLow  = g.GE.Article
		g.GS.ArticleHigh = g.GE.Article
		g.GS.ExpiresLow  = g.GE.Expires
		g.GS.ExpiresHigh = g.GE.Expires
		g.GS.Count = 1
		return err
	}
}
func (g *groupGeneral) EncodeMsgpack(dst *msgpack.Encoder) error {
	dst.EncodeBool(g.IsSumary)
	if g.IsSumary {
		return g.GS.EncodeMsgpack(dst)
	} else {
		return g.GE.EncodeMsgpack(dst)
	}
}
func (g *GroupEntry) Unmarshal(u []byte) error {
	var gg groupGeneral
	err := msgpack.Unmarshal(u,&gg)
	if err!=nil { return err }
	if gg.IsSumary { return EIsSumary }
	*g = gg.GE
	return nil
}

var groupGeneral_pool = sync.Pool{New:func()interface{} { return new(groupGeneral) } }

func (g *groupGeneral) free() {
	/* Memory-Optimization. */
	if cap(g.GE.Value)>0 {
		groupEntryBuffers.Put(g.GE.Value)
	}
	g.GE.Value = nil
	
	groupGeneral_pool.Put(g)
}
func groupGeneral_alloc() *groupGeneral {
	return groupGeneral_pool.Get().(*groupGeneral)
}

func (g *groupGeneral) merge(o *groupGeneral) {
	if g.GS.GroupLow    > o.GS.GroupLow    { g.GS.GroupLow    = o.GS.GroupLow    }
	if g.GS.ArticleLow  > o.GS.ArticleLow  { g.GS.ArticleLow  = o.GS.ArticleLow  }
	if g.GS.ExpiresLow  > o.GS.ExpiresLow  { g.GS.ExpiresLow  = o.GS.ExpiresLow  }
	if g.GS.GroupHigh   < o.GS.GroupHigh   { g.GS.GroupHigh   = o.GS.GroupHigh   }
	if g.GS.ArticleHigh < o.GS.ArticleHigh { g.GS.ArticleHigh = o.GS.ArticleHigh }
	if g.GS.ExpiresHigh < o.GS.ExpiresHigh { g.GS.ExpiresHigh = o.GS.ExpiresHigh }
	g.GS.Count += o.GS.Count
}
func (g *groupGeneral) penalty(o *groupGeneral) (grp,art,exp uint64) {
	if g.GS.GroupLow    > o.GS.GroupLow    { grp += g.GS.GroupLow-o.GS.GroupLow       }
	if g.GS.GroupHigh   < o.GS.GroupHigh   { grp += o.GS.GroupHigh-g.GS.GroupHigh     }
	
	if g.GS.ArticleLow  > o.GS.ArticleLow  { art += g.GS.ArticleLow-o.GS.ArticleLow   }
	if g.GS.ArticleHigh < o.GS.ArticleHigh { art += o.GS.ArticleHigh-g.GS.ArticleHigh }
	
	if g.GS.ExpiresLow  > o.GS.ExpiresLow  { exp += g.GS.ExpiresLow-o.GS.ExpiresLow   }
	if g.GS.ExpiresHigh < o.GS.ExpiresHigh { exp += o.GS.ExpiresHigh-g.GS.ExpiresHigh }
	
	return
}
type GroupExpired struct{
	Timestamp uint64
}
type GroupQuery struct {
	GroupID uint64
	ArticleLow uint64
	ArticleHigh uint64
}

func (g *GroupQuery) ExtractGroupCount(b []byte) (uint64,bool) {
	k1 := groupGeneral_alloc()
	defer k1.free()
	if err := msgpack.Unmarshal(b,k1); err!=nil { panic(err) }
	
	if k1.GS.GroupLow != g.GroupID || k1.GS.GroupHigh != g.GroupID { return 0,false }
	return k1.GS.Count,true
}

func (g *groupGeneral) consistent(q interface{}) bool {
	switch v := q.(type) {
	case *GroupEntry: /* This is going to be a simple lookup. */
		if g.GS.GroupLow    > v.GroupID { return false }
		if g.GS.GroupHigh   < v.GroupID { return false }
		if g.GS.ArticleLow  > v.Article { return false }
		if g.GS.ArticleHigh < v.Article { return false }
	case *GroupExpired:
		if g.GS.ExpiresLow > v.Timestamp { return false }
	case *GroupQuery:
		if g.GS.GroupLow    > v.GroupID { return false }
		if g.GS.GroupHigh   < v.GroupID { return false }
		if v.ArticleHigh >= v.ArticleLow {
			if g.GS.ArticleLow  > v.ArticleHigh { return false }
			if g.GS.ArticleHigh < v.ArticleLow  { return false }
		}
	}
	return true
}

type GroupOps struct{}

var GroupOpsImpl newtree.TreeOps = GroupOps{}

func (GroupOps) Consistent(p []byte, q interface{}) bool {
	k1 := groupGeneral_alloc()
	defer k1.free()
	if err := msgpack.Unmarshal(p,k1); err!=nil { return true }
	return k1.consistent(q)
}

func (GroupOps) Union(P newtree.Elements) []byte {
	k1 := groupGeneral_alloc()
	k2 := groupGeneral_alloc()
	k1.IsSumary = true
	defer k1.free()
	defer k2.free()
	for i,p := range P {
		if err := msgpack.Unmarshal(p.Val,k2); err!=nil { panic(err) }
		if i==0 {
			k1.GS = k2.GS
		} else {
			k1.merge(k2)
		}
	}
	data,_ := msgpack.Marshal(k1)
	return data
}

func (GroupOps) Penalty(E1,E2 []byte) (F float64) {
	k1 := groupGeneral_alloc()
	k2 := groupGeneral_alloc()
	defer k1.free()
	defer k2.free()
	if err := msgpack.Unmarshal(E1,k1); err!=nil { panic(err) }
	if err := msgpack.Unmarshal(E2,k2); err!=nil { panic(err) }
	
	grp,art,exp := k1.penalty(k2)
	
	F += math.Log(float64(grp))
	F *= 44.4
	F += math.Log(float64(art))
	F *= 44.4
	F += math.Log(float64(exp))
	
	return
}
func (g GroupOps) FirstSplit(P newtree.Elements,maxsize int) (newtree.Elements,newtree.Elements) {
	//g.Sort(P)
	return firstSplitSorted(P,maxsize)
}
func (GroupOps) Sort(E newtree.Elements) {
	dec := decpool.Get().(*decoder)
	
	/* Step two: Decode */
	for i := range E {
		dec.reset(E[i].Val)
		v := groupGeneral_alloc()
		err := v.DecodeMsgpack(dec.Dec)
		if err!=nil { panic(err) }
		E[i].Tmp = v
	}
	
	/* Step three: Sort */
	sort.Slice(E,func(i,j int) bool {
		k1 := E[i].Tmp.(*groupGeneral)
		k2 := E[j].Tmp.(*groupGeneral)
		if k1.GS.GroupLow < k2.GS.GroupLow { return true }
		if k1.GS.GroupLow > k2.GS.GroupLow { return false }
		if k1.GS.ArticleLow < k2.GS.ArticleLow { return true }
		if k1.GS.ArticleLow > k2.GS.ArticleLow { return false }
		return false
	})
	
	/* Step four: Free */
	for i := range E { E[i].Tmp.(*groupGeneral).free() }
}


