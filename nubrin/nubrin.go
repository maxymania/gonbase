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

import "github.com/coreos/bbolt"
import "github.com/vmihailenco/msgpack"
import "context"
import "sort"

type TSRecord struct{
	K,E uint64
	Value []byte
}

type TSIndex struct{
	_extensible struct{}
	
	Index,Table *bolt.Bucket
	Mod   uint64
}

func (t *TSIndex) process(e uint64,c *bolt.Cursor,page *BrinStruct) error {
	E := Encode(e)
	
	k,v := c.Seek(E)
	if len(k)!=0 {
		if err := msgpack.Unmarshal(v,page); err!=nil { return err }
		/* Lemma: e <= page.High */
		if (page.Low<=e) { /* We found it. */
			/* Assert: Decode(k)==page.High */
			return nil
		}
		H := page.Low-1
		k,_ = c.Prev()
		L := Decode(k)
		
		/*
		NOTE: L==H is impossible, because, if e<page.Low, then we would already have seeked
		      to the previous record.
		*/
		
		/* Assert: L<=e<=H */
		
		if (H-L)<=t.Mod { /* We found a nice and cozy range. */
			/* No nothing. */
		} else if (H-t.Mod)<e { /* We search for space at the end of the range. */
			L = (H-t.Mod)+1
		} else if (L+t.Mod)>e { /* We search for space at the beginning of the range. */
			H = (L+t.Mod)-1
		} else { /* We search for space in the middle of the free range. */
			L = e-(e%t.Mod)
			H = L + t.Mod
		}
		page.Low   = one2zero(L+1)
		page.High  = H
		page.Elems = page.Elems[:0]
		return nil
	}
	
	k,v = c.Last()
	if len(k)!=0 {
		L := Decode(k)
		/* Lemma: L < e */
		
		H := (L+t.Mod)
		
		if H < e {
			L = e-(e%t.Mod)
			H = L + t.Mod
		}
		
		page.Low   = one2zero(L+1)
		page.High  = H
		page.Elems = page.Elems[:0]
		return nil
	}
	
	{
		L := e-(e%t.Mod)
		H := L + t.Mod
		
		page.Low   = one2zero(L+1)
		page.High  = H
		page.Elems = page.Elems[:0]
		return nil
	}
}

func (t *TSIndex) Insert(k, e uint64, v []byte) error {
	if err := t.Table.Put(Encode(k),append(Encode(e),v...)); err!=nil { return err }
	
	var elem BrinStruct
	
	c := t.Index.Cursor()
	if err := t.process(e,c,&elem); err!=nil { return err }
	
	var node BrinNode
	node.Single(e,k)
	
	{
		sd,si := 0.0,-1
		for i := range elem.Elems {
			d := elem.Elems[i].DistanceLog(&node)
			if si<0 || sd>d {
				sd,si = d,i
			}
		}
		if si<0 {
			elem.Elems = append(elem.Elems,node)
		} else if sd < 1.0 { /* Fast path. */
			elem.Elems[si].Merge(&node)
		} else {
			if simpleMergePolicy(&(elem.Elems[si]),&node) {
				elem.Elems[si].Merge(&node)
			} else {
				elem.Elems = append(elem.Elems,node)
			}
		}
	}
	
	sort.Sort(elem.Elems)
	
	{
		for i := range elem.Elems {
			if i==0 { continue } /* Skip the first element. */
			if elem.Elems[i].Count==0 { continue } /* Skip empty or emptied-out Elements. */
			if simpleCompactionPolicy( &elem.Elems[i-1] , &elem.Elems[i] ) {
				elem.Elems[i-1].Merge(&elem.Elems[i])
				elem.Elems[i].Count = 0
			}
		}
		elem.Elems.minify()
		if len(elem.Elems)>4 {
			for len(elem.Elems)>4 {
				logar := (monoLogi(len(elem.Elems))+1.5)*1.3
				sd,si := logar,-1
				for i := range elem.Elems {
					if i==0 { continue } /* Skip the first element. */
					d := mergePenalty(&elem.Elems[i-1],&elem.Elems[i])
					if d<sd { sd,si = d,i }
				}
				if si>0 {
					elem.Elems[si-1].Merge(&elem.Elems[si])
					elem.Elems[si].Count = 0
					elem.Elems.minify()
					continue
				}
				break
			}
		}
	}
	
	data,err := msgpack.Marshal(&elem)
	if err!=nil { return err }
	
	return t.Index.Put(Encode(elem.High),data)
}
func (t *TSIndex) Lookup(k uint64) (uint64,[]byte) {
	a,b := SplitOffSecond(t.Table.Get(Encode(k)))
	return Decode(a),b
}

func (t *TSIndex) Search(ctx context.Context,e uint64,ch chan <- TSRecord) error {
	defer close(ch)
	
	k,v := t.Index.Cursor().Seek(Encode(e))
	if len(k)==0 { return nil } /* Not found. */
	
	var page BrinStruct
	
	if err := msgpack.Unmarshal(v,&page); err!=nil { return err }
	
	if e<page.Low || page.High<e { return nil } /* Not found. */
	
	if len(page.Elems)==0 { return nil } /* Not found. */
	
	
	var n BrinNode
	for i,e := range page.Elems {
		if i==0 {
			n = e
		} else {
			n.Merge(&e)
		}
	}
	
	c := t.Table.Cursor()
	
	done := ctx.Done()
	
	for _,e := range page.Elems {
		for k,v := c.Seek(Encode(e.KRMin)); len(k)>0 && Decode(k)<=e.KRMax ; k,v = c.Next() {
			ee,vv := SplitOffSecond(v)
			E := Decode(ee)
			if E<n.IRMin || n.IRMax<E {
				if ctx.Err()==nil { continue }
			} else {
				select {
				case ch <- TSRecord{Decode(k),E,vv}: continue
				case <- done:
				}
				break
			}
			continue
		}
	}
	
	return nil
}
func (t *TSIndex) deleteObject(ctx context.Context,page *BrinStruct,now uint64,consumer func([]byte)) error {
	cur := t.Table.Cursor()
	
	for i,e := range page.Elems {
		/*
		We will rebuild every element as we go.
		*/
		var node,motiv BrinNode
		
		for k,v := cur.Seek(Encode(e.KRMin)); len(k)!=0 && Decode(k)<e.KRMax; k,v = cur.Next() {
			ee,_ := SplitOffSecond(v)
			E := Decode(ee)
			if E <= now {
				consumer(v)
				if err := cur.Delete(); err!=nil { return err }
			} else {
				/*
				At this point, we retain the record.
				
				Record every element within the suspected range.
				*/
				if page.Low<=E && E<=page.High {
					motiv.Single(E,Decode(k))
					node.remMerge(&motiv)
				}
			}
			
			if ctx.Err()!=nil {
				/*
				If we abort the loop, our new element is incomplete.
				To fix this, we just copy the old one.
				*/
				node = e
				break
			}
		}
		page.Elems[i] = node
		
		/* If the inner loop exists, we need to exit the outer one as well. */
		if ctx.Err()!=nil { break }
	}
	return nil
}

func (t *TSIndex) DeleteExpire(ctx context.Context,now uint64,consumer func([]byte)) error {
	var page BrinStruct
	
	cur := t.Index.Cursor()
	
	lcbuf := make([]byte,0,9)
	
	for k,v := cur.First(); len(k)!=0 ; k,v = cur.Next() {
		if ctx.Err()!=nil { break }
		if err := msgpack.Unmarshal(v,&page); err!=nil { return err }
		if now < page.Low { break }
		err := t.deleteObject(ctx,&page,now,consumer)
		if err!=nil { return err }
		
		page.Elems.minify()
		
		if len(page.Elems)==0 {
			/*
			Minify emptied-out the elements, meaning, that there is no indexed
			record left. In this case, we will simply delete the Page...
			*/
			cur.Delete()
		} else {
			/*
			...otherwise, we will write the Page back.
			*/
			data,err := msgpack.Marshal(v,&page)
			if err!=nil { return err }
			err = t.Index.Put(append(lcbuf,k...),data)
			if err!=nil { return err }
		}
	}
	return nil
}

