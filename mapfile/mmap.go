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
import "github.com/edsrzf/mmap-go"
import "github.com/byte-mug/golibs/bufferex"

var eCookie Cookie = struct{mmapCookie bool}{ mmapCookie: true }

func shorten(a []byte, l int64) ([]byte,bool){
	if int64(len(a))<=l { return a,true }
	return a[:l],false
}

type MmapStore struct{
	LLS
	IsGrown bool
	area []byte
	memo mmap.MMap
}
func (m *MmapStore) ReadAt(p []byte, off int64) (n int, err error) {
	end := off+int64(len(p))
	if end<=int64(len(m.area)) {
		copy(p,m.area[off:end])
		return len(p),nil
	}
	return m.LLS.ReadAt(p,off)
}
func (m *MmapStore) WriteAt(p []byte, off int64) (n int, err error) {
	end := off+int64(len(p))
	if end<=int64(len(m.area)) {
		copy(m.area[off:end],p)
		return len(p),nil
	}
	return m.LLS.WriteAt(p,off)
}
func (m *MmapStore) RefreshMmap() (err error) {
	var f *os.File
	if !m.LLS.Unwrap(&f) { panic("no *os.File to unwrap") }
	m.memo.Unmap()
	m.memo,err = mmap.Map(f,mmap.RDWR,0)
	m.area = m.memo
	m.IsGrown = false
	return
}
func (m *MmapStore) GetInAt(p int, off int64) (n bufferex.Binary, err error) {
	end := off+int64(p)
	
	/* If the position is out of bounds, use the default impl. */
	if end > int64(len(m.area)) { return m.LLS.GetInAt(p,off) }
	
	n = bufferex.NewBinaryInplace(m.area[off:end])
	return
}
func (m *MmapStore) Unwrap(i interface{}) bool {
	if a,ok := i.(*mmap.MMap); ok {
		*a = m.area
		return true
	}
	return m.LLS.Unwrap(i)
}
func (m *MmapStore) GetOutAt(p int, off int64) (n bufferex.Binary, c Cookie, err error) {
	end := off+int64(p)
	if end<=int64(len(m.area)) {
		n = bufferex.NewBinaryInplace(m.area[off:end])
		c = eCookie
		return
	}
	return m.LLS.GetOutAt(p,off)
}
func (m *MmapStore) CommitOutAt(c Cookie,p bufferex.Binary, off int64) (err error) {
	if c==eCookie { return }
	return m.LLS.CommitOutAt(c,p,off)
}
func (m *MmapStore) Truncate(lng int64) error {
	err := m.LLS.Truncate(lng)
	if err==nil {
		m.area,m.IsGrown = shorten(m.area,lng)
	}
	return err
}

var _ LLS = (*MmapStore)(nil)


