// +build 386 amd64 riscv

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

import "unsafe"

type format struct{}

func (format) Uint64(b []byte) uint64 {
	return *(*uint64)(unsafe.Pointer(&b[0]))
}
func (format) Uint32(b []byte) uint32 {
	return *(*uint32)(unsafe.Pointer(&b[0]))
}
func (format) PutUint64(b []byte, u uint64) {
	*(*uint64)(unsafe.Pointer(&b[0])) = u
}
func (format) PutUint32(b []byte, u uint32) {
	*(*uint32)(unsafe.Pointer(&b[0])) = u
}

var frm = format{}

