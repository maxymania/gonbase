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


package newtree

import "github.com/byte-mug/golibs/bufferex"
import "context"

type Tree struct {
	IBase
	Ops TreeOps
}

/* -------------------------------------------------------------------------------- */

func (t *Tree) getPage(id int64) (b bufferex.Binary,e Elements,err error) {
	e = allocElements()
	b,err = t.PageRead(id)
	if err!=nil { return }
	_,err = e.BinDecode(b.Bytes())
	return
}
func (t *Tree) insertPage(e Elements) (int64,error) {
	b := bufferex.AllocBinary(t.Page())
	defer b.Free()
	_,err := e.BinEncode(b.Bytes())
	if err!=nil { return 0,err }
	id,err := t.PageAlloc()
	if err!=nil { return 0,err }
	err = t.PageWrite(id,b.Bytes())
	return id,err
}
func (t *Tree) putPage(id int64, e Elements) error {
	b := bufferex.AllocBinary(t.Page())
	defer b.Free()
	_,err := e.BinEncode(b.Bytes())
	if err!=nil { return err }
	return t.PageWrite(id,b.Bytes())
}
func (t *Tree) getRoot(id int64) (rr Root,re error) {
	b,err := t.HeadRead(id)
	defer b.Free()
	if err!=nil { re = err; return }
	re = rr.BinDecode(b.Bytes())
	return
}
func (t *Tree) putRoot(id int64,rr Root) error {
	b := bufferex.AllocBinary(HeadSize)
	defer b.Free()
	err := rr.BinEncode(b.Bytes())
	if err!=nil { return err }
	return t.HeadWrite(id,b.Bytes())
}

func (t *Tree) NewRoot() (int64,error) {
	id,err := t.HeadAlloc()
	if err!=nil { return 0,err }
	err = t.putRoot(id,Root{0,0})
	return id,err
}

/* -------------------------------------------------------------------------------- */

func (t *Tree) insert(id int64,nitem []byte) (r_elems Elements, r_err error) {
	b,node,err := t.getPage(id)
	defer freeElements(node)
	defer b.Free()
	if err!=nil { r_err = err; return }
	
	r_elems = Elements{{Ptr:id}}
	
	if node.IsLeaf() {
		node = append(node,Element{Val:nitem})
	} else {
		sp := -1
		sc := float64(0)
		for i,e := range node {
			c := t.Ops.Penalty(e.Val,nitem)
			if (sp < 0) || (sc > c) {
				sp = i
				sc = c
			}
		}
		s_elems,err := t.insert(node[sp].Ptr,nitem)
		if err!=nil { r_err = err; return }
		if len(s_elems)==0 {
			r_elems = nil
			r_err = t.PageFree(id)
			return
		}
		node[sp] = s_elems[0]
		if len(s_elems) > 1 {
			node = append(node,s_elems[1:]...)
		}
	}
	{
		cur,rest := t.Ops.FirstSplit(node,t.Page())
		t.Ops.Sort(cur)
		err = t.putPage(id,cur)
		if err!=nil { r_err = err; return }
		r_elems[0].Val = t.Ops.Union(cur)
		
		for len(rest)>0 {
			var e Element
			cur,rest = t.Ops.FirstSplit(rest,t.Page())
			e.Ptr,err = t.insertPage(cur)
			if err!=nil { r_err = err; return }
			e.Val = t.Ops.Union(cur)
			r_elems = append(r_elems,e)
		}
	}
	
	return
}

func (t *Tree) Insert(obj int64,nitem []byte) error {
	rr,err := t.getRoot(obj)
	if err!=nil { return err }
	
	if rr.Ptr==0 {
		id,err := t.insertPage(Elements{{Val:nitem}})
		if err!=nil { return err }
		rr.Ptr   = id
		rr.Depth = 1
		return t.putRoot(obj,rr)
	}
	
	elems,err := t.insert(rr.Ptr,nitem)
	if err!=nil { return err }
	
	if len(elems)==0 {
		return t.PageFree(rr.Ptr)
	} else if len(elems) > 1 {
		id,err := t.insertPage(elems)
		if err!=nil { return err }
		rr.Ptr = id
		rr.Depth++
		return t.putRoot(obj,rr)
	}
	if rr.Ptr!=elems[0].Ptr {
		/* For whatever reason, the pointer changed. Update it. */
		rr.Ptr = elems[0].Ptr
		return t.putRoot(obj,rr)
	}
	
	return nil
}

/* -------------------------------------------------------------------------------- */

func (t *Tree) search(
	ctx context.Context,
	id int64,
	q interface{},
	ch chan <- []byte) error {
	b,node,err := t.getPage(id)
	defer freeElements(node)
	defer b.Free()
	if err!=nil { return err }
	
	for _,e := range node {
		if t.Ops.Consistent(e.Val,q) {
			if e.Ptr == 0 {
				select {
				case ch <- e.Val:
				case <- ctx.Done(): return ctx.Err()
				}
			} else {
				err := ctx.Err()
				if err!=nil { return err }
				err = t.search(ctx,e.Ptr,q,ch)
				if err!=nil { return err }
			}
		}
	}
	return nil
}
func (t *Tree) Search(
	ctx context.Context,
	obj int64,
	q interface{},
	ch chan <- []byte) error {
	defer close(ch)
	rr,err := t.getRoot(obj)
	if err!=nil { return err }
	return t.search(ctx,rr.Ptr,q,ch)
}

/* -------------------------------------------------------------------------------- */

func (t *Tree) delete(
	ctx context.Context,
	id int64,
	q interface{},
	chk func([]byte) bool) (r_elems Elements, r_inner int, r_abort, r_err error) {
	b,onode,err := t.getPage(id)
	defer freeElements(onode)
	defer b.Free()
	if err!=nil { r_err = err; return }
	
	r_elems = Elements{{Ptr:id}}
	
	node := allocElements()[:0]
	defer freeElements(node)
	
	for i,e := range onode {
		r_abort = ctx.Err()
		if r_abort!=nil {
			node = append(node,onode[i:]...)
			break
		}
		if !t.Ops.Consistent(e.Val,q) {
			node = append(node,e)
			continue
		}
		if e.Ptr!=0 {
			elems,_,_,err := t.delete(ctx,e.Ptr,q,chk)
			if err!=nil { r_err = err; return }
			node = append(node,elems...)
			continue
		}
		if !chk(e.Val) {
			node = append(node,e)
		}
	}
	
	if len(node)==0 {
		r_elems = nil
		r_err = t.PageFree(id)
		return
	}
	
	r_inner = len(node)
	{
		cur,rest := t.Ops.FirstSplit(node,t.Page())
		t.Ops.Sort(cur)
		err = t.putPage(id,cur)
		if err!=nil { r_err = err; return }
		r_elems[0].Val = t.Ops.Union(cur)
		for len(rest)>0 {
			var e Element
			cur,rest = t.Ops.FirstSplit(rest,t.Page())
			e.Ptr,err = t.insertPage(cur)
			if err!=nil { r_err = err; return }
			e.Val = t.Ops.Union(cur)
			r_elems = append(r_elems,e)
		}
	}
	
	return
}
func (t *Tree) walkOneOne(rr Root) (Root,bool,error) {
	id := rr.Ptr
	b,onode,err := t.getPage(id)
	defer freeElements(onode)
	defer b.Free()
	if err!=nil { return rr,false,err }
	switch len(onode) {
	case 0:
		rr.Ptr = 0
	case 1:
		rr.Ptr = onode[0].Ptr
	default:
		return rr,false,nil
	}
	err = t.PageFree(id)
	return rr,true,err
}
func (t *Tree) walkOnes(rr Root) (Root,error) {
	var more bool
	var err error
	for {
		rr,more,err = t.walkOneOne(rr)
		if !more { break }
	}
	return rr,err
}
func (t *Tree) Delete(
	ctx context.Context,
	obj int64,
	q interface{},
	chk func([]byte) bool) (r_abort, r_err error) {
	rr,err := t.getRoot(obj)
	if err!=nil { return nil,err }
	
	if rr.Ptr==0 {
		return nil,nil
	}
	
	elems,inner,abort,err := t.delete(ctx,rr.Ptr,q,chk)
	if err!=nil { return abort,err }
	
	if len(elems)==0 {
		return abort,t.PageFree(rr.Ptr)
	} else if len(elems) > 1 {
		id,err := t.insertPage(elems)
		if err!=nil { return abort,err }
		rr.Ptr = id
		rr.Depth++
		return abort,t.putRoot(obj,rr)
	}
	if rr.Ptr!=elems[0].Ptr {
		/* For whatever reason, the pointer changed. Update it. */
		rr.Ptr = elems[0].Ptr
		if inner < 2 {
			rr,err = t.walkOnes(rr)
			if err!=nil { return abort,err }
		}
		return abort,t.putRoot(obj,rr)
	}
	
	if inner < 2 {
		rr,err = t.walkOnes(rr)
		if err!=nil { return abort,err }
		return abort,t.putRoot(obj,rr)
	}
	return abort,nil
}

/* -------------------------------------------------------------------------------- */

