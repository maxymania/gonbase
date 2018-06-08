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


package replidb

import "bytes"
import bolt "github.com/coreos/bbolt"
import avl "github.com/emirpasic/gods/trees/avltree"
import "sync"

var (
	iFree = []byte("Free")
	iSeq  = []byte("Seq")
)

type Pair struct{
	Name []byte
	Num  uint64
}
func (p *Pair) Copy() {
	p.Name = append([]byte(nil),p.Name...)
}

func BytesComparator(a, b interface{}) int {
	return bytes.Compare(a.([]byte),b.([]byte))
}
func PairComparator(a, b interface{}) int {
	A := a.(Pair)
	B := b.(Pair)
	i := bytes.Compare(A.Name,B.Name)
	if i==0 {
		if A.Num<B.Num { return -1 }
		if A.Num>B.Num { return  1 }
	}
	return i
}

type Sequence struct{
	DB        *bolt.DB
	Table     []byte
	
	Exist     *avl.Tree
	DontExist *avl.Tree
	Seqs      *avl.Tree
	
	/*
	This barrier essentially exist, to thwart any interference between
	the Transactions and the Background-Flush.
	
	Additionally, it allows for multiple possible Transactions to operate
	concurrently without screwing up the AVL trees.
	
	These mutexes are declared in their respective lock-order.
	*/
	bgf sync.Mutex
	w1,w2 sync.RWMutex
}
func (s *Sequence) Init() {
	s.Exist     = avl.NewWith(PairComparator)
	s.DontExist = avl.NewWith(PairComparator)
	s.Seqs      = avl.NewWith(BytesComparator)
}
func (s *Sequence) Lowest(name []byte) (uint64,bool) {
	s.w1.RLock(); defer s.w1.RUnlock()
	s.w2.RLock(); defer s.w2.RUnlock()
	var us [2]uint64
	s.DB.View(func(t *bolt.Tx) (_e error) {
		/*
		Perform bkt := tx[s.Table][name].Free
		*/
		bkt := t.Bucket(s.Table)
		if bkt==nil { return }
		bkt = bkt.Bucket(name)
		if bkt==nil { return }
		bkt = bkt.Bucket(iFree)
		if bkt==nil { return }
		c := bkt.Cursor()
		
		for k,_ := c.First(); len(k)!=0; k,_ = c.Next() {
			elem := Decode(k)
			if _,ok := s.DontExist.Get(Pair{name,elem}); ok { continue }
			us[0] = elem
		}
		return
	})
	n,ok := s.Exist.Ceiling(Pair{name,0})
	if ok {
		pair := n.Key.(Pair)
		if bytes.Equal(pair.Name,name) {
			us[1] = pair.Num
		}
	}
	for _,u := range us {
		if u!=0 { return u,true }
	}
	return 0,false
}
func (s *Sequence) GetSequence(name []byte) uint64 {
	s.w1.RLock(); defer s.w1.RUnlock()
	s.w2.RLock(); defer s.w2.RUnlock()
	if i,ok := s.Seqs.Get(name); ok { return i.(uint64) }
	var u uint64
	s.DB.View(func(t *bolt.Tx) (_e error) {
		/*
		Perform bkt := tx[s.Table][name].Seq
		*/
		bkt := t.Bucket(s.Table)
		if bkt==nil { return }
		bkt = bkt.Bucket(name)
		if bkt==nil { return }
		u = Decode(bkt.Get(iSeq))
		return
	})
	return u
}

func (s *Sequence) Delete(pair Pair) {
	s.w1.Lock(); defer s.w1.Unlock()
	noaction := true
	s.DB.View(func(t *bolt.Tx) (_e error) {
		encoder := NewUIntBuffer()
		defer encoder.Free()
		/*
		Perform bkt := tx[s.Table][name].Free
		*/
		bkt := t.Bucket(s.Table)
		if bkt==nil { return }
		bkt = bkt.Bucket(pair.Name)
		if bkt==nil { return }
		bkt = bkt.Bucket(iFree)
		if bkt==nil { return }
		if len(bkt.Get(encoder.Encode(pair.Num)))!=0 { noaction = false }
		return
	})
	s.Exist.Remove(pair)
	if noaction { return }
	if _,ok := s.DontExist.Get(pair); ok { return }
	pair.Copy()
	s.DontExist.Put(pair,pair)
}
func (s *Sequence) Create(pair Pair) {
	s.w1.Lock(); defer s.w1.Unlock()
	action := true
	s.DB.View(func(t *bolt.Tx) (_e error) {
		encoder := NewUIntBuffer()
		defer encoder.Free()
		/*
		Perform bkt := tx[s.Table][name].Free
		*/
		bkt := t.Bucket(s.Table)
		if bkt==nil { return }
		bkt = bkt.Bucket(pair.Name)
		if bkt==nil { return }
		bkt = bkt.Bucket(iFree)
		if bkt==nil { return }
		if len(bkt.Get(encoder.Encode(pair.Num)))!=0 { action = false }
		return
	})
	s.DontExist.Remove(pair)
	if !action { return }
	if _,ok := s.Exist.Get(pair); ok { return }
	pair.Copy()
	s.Exist.Put(pair,pair)
}
func (s *Sequence) SetSequence(pair Pair) {
	pair.Copy()
	s.w1.Lock(); defer s.w1.Unlock()
	s.Seqs.Put(pair.Name,pair.Num)
}

/* Background-Flush function. */
func (s *Sequence) BGFlush() error {
	/* Thwart concurrent access to .BGFlush() */
	s.bgf.Lock(); defer s.bgf.Unlock()
	
	/* Exclude the writers. */
	s.w1.RLock(); defer s.w1.RUnlock()
	
	err := s.DB.Update(func(t *bolt.Tx) (_e error) {
		encoder := NewUIntBuffer()
		defer encoder.Free()
		td,err := t.CreateBucketIfNotExists(s.Table)
		if err!=nil { return err }
		
		for n := s.Exist.Left(); n!=nil ; n = n.Next() {
			pair := n.Key.(Pair)
			bkt,err := td.CreateBucketIfNotExists(pair.Name)
			if err!=nil { return err }
			bkt,err = bkt.CreateBucketIfNotExists(iFree)
			if err!=nil { return err }
			err = bkt.Put(encoder.Encode(pair.Num),iFree)
			if err!=nil { return err }
		}
		for n := s.DontExist.Left(); n!=nil ; n = n.Next() {
			pair := n.Key.(Pair)
			bkt := td.Bucket(pair.Name)
			if bkt==nil { continue }
			bkt = bkt.Bucket(iFree)
			if bkt==nil { continue }
			err = bkt.Delete(encoder.Encode(pair.Num))
			if err!=nil { return err }
		}
		for n := s.Seqs.Left(); n!=nil ; n = n.Next() {
			name := n.Key.([]byte)
			val  := n.Value.(uint64)
			bkt,err := td.CreateBucketIfNotExists(name)
			if err!=nil { return err }
			return bkt.Put(iSeq,encoder.Encode(val))
		}
		return
	})
	if err!=nil { return err }
	
	/* Exclude the readers. */
	s.w2.Lock(); defer s.w2.Unlock()
	
	s.Exist.Clear()
	s.DontExist.Clear()
	s.Seqs.Clear()
	
	return nil
}

