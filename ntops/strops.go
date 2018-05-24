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
import "bytes"
import "sort"
import "sync"
import "fmt"

func strcpy(d *[]byte,s []byte) {
	*d = append((*d)[:0],s...)
}

type strKey struct{
	_msgpack struct{} `msgpack:",asArray"`
	IsRange bool
	Low  []byte
	High []byte
}
var strKeyPool = sync.Pool{New:func() interface{}{ return new(strKey) }}
func strKeyNew() *strKey { return strKeyPool.Get().(*strKey) }
func (s *strKey) free() { strKeyPool.Put(s) }
func (s *strKey) clear() {
	s.IsRange = false
	s.Low = s.Low[:0]
	s.High = s.High[:0]
}
func (s *strKey) decode() {
	if s.IsRange {
		strcpy(&s.High,s.Low)
		s.IsRange = false
	}
}
func (s strKey) String() string {
	return fmt.Sprintf("[%q %q]",s.Low,s.High)
}

func EncodePair(k,v []byte) []byte {
	data,_ := msgpack.Marshal(&strKey{IsRange:false,Low:k,High:v})
	return data
}

type StrRange struct{
	Low,High []byte
}

func (s *strKey) matchRange(q *StrRange) bool{
	pre := bytes.Compare(q.High,s.Low)
	post := bytes.Compare(s.High,q.Low)
	return (pre>=0) && (post>=0)
}
func (s *strKey) merge(o *strKey) {
	if bytes.Compare(s.Low,o.Low)>0 { strcpy(&s.Low,o.Low) }
	if bytes.Compare(s.High,o.High)<0 { strcpy(&s.High,o.High) }
}
func (s *strKey) set(o *strKey) {
	strcpy(&s.Low ,o.Low )
	strcpy(&s.High,o.High)
}

type StrOps struct{}

var StrOpsImpl newtree.TreeOps = StrOps{}

func (s StrOps) Consistent(p []byte, q interface{}) bool {
	k := strKeyNew()
	defer k.free()
	err := msgpack.Unmarshal(p,k)
	if err!=nil { return true }
	k.decode()
	switch v := q.(type) {
	case *StrRange:
		return k.matchRange(v)
	case StrRange:
		return k.matchRange(&v)
	}
	return false
}
func (s StrOps) Union(P newtree.Elements) []byte {
	k1 := strKeyNew()
	k2 := strKeyNew()
	k1.clear()
	defer k1.free()
	defer k2.free()
	for i,p := range P {
		if err := msgpack.Unmarshal(p.Val,k2); err!=nil { panic(err) }
		if i==0 {
			k1.set(k2)
		} else {
			k1.merge(k2)
		}
	}
	data,_ := msgpack.Marshal(k1)
	return data
}
func (s StrOps) Penalty(E1,E2 []byte) float64 {
	k1 := strKeyNew()
	k2 := strKeyNew()
	defer k1.free()
	defer k2.free()
	err := msgpack.Unmarshal(E1,k1)
	if err!=nil { return 4 }
	err = msgpack.Unmarshal(E2,k2)
	if err!=nil { return 3.5 }
	k1.decode()
	k2.decode()
	if bytes.Compare(k2.Low,k1.Low)<0 { return 3 } /* if k2.Low ... k1.Low then 3! */
	if bytes.Compare(k1.High,k2.Low)<0 { return 2 } /* if k1.High ... k2.Low then 2! */
	if bytes.Compare(k1.High,k2.High)<0 { return 1 } /* if k1.High ... k2.High then 1! */
	return 0
}
func (s StrOps) FirstSplit(P newtree.Elements,maxsize int) (newtree.Elements,newtree.Elements) {
	//s.Sort(P)
	return firstSplitSorted(P,maxsize)
}
func (s StrOps) Sort(E newtree.Elements) {
	k1 := strKeyNew()
	k2 := strKeyNew()
	defer k1.free()
	defer k2.free()
	sort.Slice(E,func(i,j int) bool {
		err := msgpack.Unmarshal(E[i].Val,k1)
		if err!=nil { return false }
		err = msgpack.Unmarshal(E[j].Val,k2)
		if err!=nil { return false }
		return bytes.Compare(k1.Low,k2.Low)<0
	})
}

func firstSplitSorted(P newtree.Elements,maxsize int) (newtree.Elements,newtree.Elements) {
	z := 4
	lng := P.Length()
	if lng<=maxsize { return P,nil }
	lng>>=1
	
	for i,e := range P {
		if z > lng {
			return korrektur(P[:i],P[i:])
		}
		z+= e.Length()
		if z > maxsize {
			return korrektur(P[:i],P[i:])
		}
	}
	return P,nil
}

func korrektur(a,b newtree.Elements) (c,d newtree.Elements) {
	if len(a)==0 { return b[:1],b[1:] }
	return a,b
}
