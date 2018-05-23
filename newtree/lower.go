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

import "github.com/cznic/file"
import "github.com/byte-mug/golibs/bufferex"

type ReadAlloc struct{
	P int
	F file.File
}
func (r *ReadAlloc) Page() int { return r.P }
func (r *ReadAlloc) PageAlloc() (int64,error) { return 0,EReadOnly }
func (r *ReadAlloc) PageWrite(id int64,b []byte) error { return EReadOnly }
func (r *ReadAlloc) PageFree(id int64) error { return EReadOnly }
func (r *ReadAlloc) HeadAlloc() (int64,error) { return 0,EReadOnly }
func (r *ReadAlloc) HeadWrite(id int64,b []byte) error { return EReadOnly }
func (r *ReadAlloc) HeadFree(id int64) error { return EReadOnly }

func (r *ReadAlloc) PageRead(id int64) (b bufferex.Binary,e error) {
	b = bufferex.AllocBinary(r.P)
	_,e = r.F.ReadAt(b.Bytes(),id)
	return
}
func (r *ReadAlloc) HeadRead(id int64) (b bufferex.Binary,e error){
	b = bufferex.AllocBinary(HeadSize)
	_,e = r.F.ReadAt(b.Bytes(),id)
	return
}

type WriteAlloc struct{
	ReadAlloc
	A *file.Allocator
}
func (r *WriteAlloc) PageAlloc() (int64,error) { return r.A.Alloc(int64(r.P)) }
func (r *WriteAlloc) PageFree(id int64) error { return r.A.Free(id) }
func (r *WriteAlloc) HeadAlloc() (int64,error) { return r.A.Alloc(HeadSize) }
func (r *WriteAlloc) HeadFree(id int64) error { return r.A.Free(id) }

func (r *WriteAlloc) PageWrite(id int64,b []byte) error {
	_,err := r.F.WriteAt(b,id)
	return err
}
func (r *WriteAlloc) HeadWrite(id int64,b []byte) error {
	_,err := r.F.WriteAt(b,id)
	return err
}
//
