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

import "errors"
import "fmt"
import "bytes"
import "sync"
var EShort = errors.New("Too short")

type Element struct{
	Val []byte
	Ptr int64
	Tmp interface{}
}
func (e *Element) clear() { e.Tmp = nil }
func (e *Element) cleanse() { e.Tmp = nil; e.Ptr = 0 }
func (e Element) Length() int { return len(e.Val)+12 }
func (e *Element) BinDecode(b []byte) ([]byte,error) {
	if len(b)<4 { return nil,EShort }
	l := frm.Uint32(b)
	if len(b)<(int(l)+12) { return nil,EShort }
	e.Val = b[4:l+4]
	e.Ptr = int64(frm.Uint64(b[l+4:]))
	e.Tmp = nil
	return b[l+12:],nil
}
func (e Element) BinEncode(b []byte) (int,error) {
	lng := len(e.Val)+12
	if lng > len(b) { return 0,EShort }
	frm.PutUint32(b,uint32(len(e.Val)))
	copy(b[4:],e.Val)
	frm.PutUint64(b[lng-8:],uint64(e.Ptr))
	return lng,nil
}
func (e Element) String() string { return fmt.Sprintf("{%q %d}",e.Val,e.Ptr) }

type Elements []Element
func (e Elements) clear() {
	e = e[:cap(e)]
	for i := range e { e[i].clear() }
}
func (e Elements) cleanse() {
	e = e[:cap(e)]
	for i := range e { e[i].cleanse() }
}
func (e Elements) Length() int {
	res := 4
	for _,elem := range e {
		res += elem.Length()
	}
	return res
}
func (e *Elements) resize32(l uint32) {
	if uint32(cap(*e)) < l {
		*e = make(Elements,l)
	} else {
		*e = (*e)[:l]
	}
}
func (e *Elements) BinDecode(b []byte) ([]byte,error) {
	if len(b)<4 { return nil,EShort }
	l := frm.Uint32(b)
	e.resize32(l)
	b = b[4:]
	var err error
	for i := range *e {
		b,err = (*e)[i].BinDecode(b)
		if err!=nil { return nil,err }
	}
	return b,nil
}
func (e Elements) BinEncode(b []byte) (int,error) {
	if len(b)<4 { return 0,EShort }
	frm.PutUint32(b,uint32(len(e)))
	i := 4
	for _,el := range e {
		j,err := el.BinEncode(b[i:])
		if err!=nil { return 0,err }
		i += j
	}
	return i,nil
}
func (e Elements) IsLeaf() bool {
	for _,ee := range e { if ee.Ptr!=0 { return false } }
	return true
}
func (e Elements) IsNode() bool {
	for _,ee := range e { if ee.Ptr==0 { return false } }
	return true
}
func (e Elements) String() string {
	buf := new(bytes.Buffer)
	fmt.Fprintf(buf,"[\n")
	for _,ee := range e {
		fmt.Fprintf(buf,"\t%v\n",ee)
	}
	fmt.Fprintf(buf,"]")
	return buf.String()
}

type Root struct{
	Ptr    int64
	Depth uint32
}
func (r *Root) BinDecode(b []byte) error {
	if len(b)<16 { return EShort }
	r.Ptr = int64(frm.Uint64(b))
	r.Depth = frm.Uint32(b[8:])
	return  nil
}
func (r Root) BinEncode(b []byte) error {
	if len(b)<16 { return EShort }
	frm.PutUint64(b,uint64(r.Ptr))
	frm.PutUint32(b[8:],r.Depth)
	return  nil
}

var poolElements = sync.Pool{New:func()interface{}{ return Elements(nil) }}

func allocElements() Elements {
	e := poolElements.Get().(Elements)
	return e
}
func freeElements(e Elements) {
	poolElements.Put(e)
}

