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


package mapfile

import "os"
import "github.com/byte-mug/golibs/bufferex"

type Filer interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Stat() (os.FileInfo, error)
	Sync() error
    Truncate(int64) error
	Close() error
}

type WrapStore struct{
	Filer
}
func (f *WrapStore) GetInAt(p int, off int64) (n bufferex.Binary, err error) {
	n = bufferex.AllocBinary(p)
	_,err = f.ReadAt(n.Bytes(),off)
	if err!=nil { resetB(&n) }
	return
}
func (f *WrapStore) Unwrap(i interface{}) bool {
	return false
}
func (f *WrapStore) GetOutAt(p int, off int64) (n bufferex.Binary, c Cookie, err error) {
	return bufferex.AllocBinary(p),nil,nil
}
func (f *WrapStore) CommitOutAt(c Cookie,p bufferex.Binary, off int64) (err error) {
	defer p.Free()
	_,err = f.WriteAt(p.Bytes(),off)
	return
}

var _ LLS = (*WrapStore)(nil)

