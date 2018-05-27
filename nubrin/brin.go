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


package nubrin

//import "github.com/coreos/bbolt"
import "github.com/vmihailenco/msgpack"
import "math"

type BrinNode struct{
	IRMin uint64
	IRMax uint64
	KRMin uint64
	KRMax uint64
	Count uint64
}
func (b *BrinNode) DecodeMsgpack(src *msgpack.Decoder) error {
	err := src.Decode(&b.IRMin,&b.IRMax,&b.KRMin,&b.KRMax,&b.Count)
	b.IRMax+=b.IRMin
	b.KRMax+=b.KRMin
	return err
}
func (b *BrinNode) EncodeMsgpack(dst *msgpack.Encoder) error {
	return dst.Encode(b.IRMin,b.IRMax-b.IRMin,b.KRMin,b.KRMax-b.KRMin,b.Count)
}
func (b *BrinNode) Single(I,K uint64) {
	b.IRMin = I
	b.IRMax = I
	b.KRMin = K
	b.KRMax = K
	b.Count = 1
}
func (b *BrinNode) Merge(c *BrinNode) {
	if b.IRMin>c.IRMin { b.IRMin = c.IRMin }
	if b.KRMin>c.KRMin { b.KRMin = c.KRMin }
	if b.IRMax<c.IRMax { b.IRMax = c.IRMax }
	if b.KRMax<c.KRMax { b.KRMax = c.KRMax }
	b.Count += c.Count
}

func (b *BrinNode) remMerge(c *BrinNode) {
	if b.Count==0 {
		*b = *c
		return
	}
	if b.IRMin>c.IRMin { b.IRMin = c.IRMin }
	if b.KRMin>c.KRMin { b.KRMin = c.KRMin }
	if b.IRMax<c.IRMax { b.IRMax = c.IRMax }
	if b.KRMax<c.KRMax { b.KRMax = c.KRMax }
	b.Count += c.Count
}

/* Log2(length/count) */
func (b BrinNode) FillFactorLog() float64 {
	i := b.Count
	if i==0 { i = 1 }
	k := 1+b.KRMax-b.KRMin
	if k==0 { k = 1 }
	return math.Log2(float64(k))-math.Log2(float64(i))
}
func (b *BrinNode) Length() uint64 {
	return 1+b.KRMax-b.KRMin
}
func (b *BrinNode) Distance(c *BrinNode) uint64 {
	k := uint64(0)
	if b.KRMax<c.KRMin {
		k = c.KRMin-b.KRMax
	} else if c.KRMax<b.KRMin {
		k = b.KRMin-c.KRMax
	}
	return k
}
func (b *BrinNode) DistanceLog(c *BrinNode) float64 {
	k := uint64(0)
	if b.KRMax<c.KRMin {
		k = c.KRMin-b.KRMax
	} else if c.KRMax<b.KRMin {
		k = b.KRMin-c.KRMax
	}
	if k==0 { k = 1 }
	return math.Log2(float64(k))
}

type BrinElement []BrinNode

func (b BrinElement) Len() int { return len(b) }
func (b BrinElement) Less(i, j int) bool { return b[i].KRMax<b[j].KRMax }
func (b BrinElement) Swap(i, j int) { b[i],b[j]=b[j],b[i] }

func (b *BrinElement) minify() {
	c := (*b)[:0]
	for _,e := range *b {
		if e.Count==0 { continue }
		c = append(c,e)
	}
	*b = c
}

type BrinStruct struct {
	_msgpack struct{} `msgpack:",asArray"`
	Low,High uint64
	Elems BrinElement
}


func combiLog(a,b,c uint64) float64 {
	b+=c
	if b<1 { b = 1 }
	if a<1 { a = 1 }
	return math.Log2(float64(a))-math.Log2(float64(b))
}
func monoLog(a uint64) float64 {
	if a<1 { a = 1 }
	return math.Log2(float64(a))
}
func monoLogi(a int) float64 {
	if a<1 { a = 1 }
	return math.Log2(float64(a))
}
func one2zero(i uint64) uint64{
	if i==1 { i = 0 }
	return i
}

func simpleMergePolicy(e1,e2 *BrinNode) bool {
	e := *e1
	e.Merge(e2)
	if e.Length()<256 { return true }
	return false
}

func simpleCompactionPolicy(e1,e2 *BrinNode) bool {
	e := *e1
	e.Merge(e2)
	
	eL := e.Length()
	d := e1.Distance(e2)
	
	/* Heuristic 0: Tolerate a certain length no matter what. */
	if eL<256 { return true }
	
	/* Heuristic 1: Honor full overlap. */
	if d<=1 { return true }
	
	e1ffl := e1.FillFactorLog()
	e2ffl := e2.FillFactorLog()
	effl  := e.FillFactorLog()
	
	/* Heuristic 2: Honor fill-efficiency increases. */
	if e1ffl >= effl { return true }
	if e2ffl >= effl { return true }
	
	return false
}

func mergePenalty(e1,e2 *BrinNode) float64 {
	e := *e1
	e.Merge(e2)
	
	//e1ffl := e1.FillFactorLog()
	//e2ffl := e2.FillFactorLog()
	//effl  := e.FillFactorLog()
	
	//return effl-((e1ffl+e2ffl)/2.0)
	return e.FillFactorLog()
}


