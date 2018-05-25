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

import "encoding/binary"

func Encode(V uint64) []byte {
	b := make([]byte,9)
	b[0] = 0
	binary.BigEndian.PutUint64(b[1:],V)
	for i,c := range b {
		if c==0 { continue }
		if c>=16 { i-- }
		b[i] |= byte((8-i)<<4)
		return b[i:]
	}
	b[8] |= 0<<4
	return b[8:]
}

func Decode(b []byte) (i uint64) {
	if len(b)==0 { return }
	i = uint64(b[0]&15)
	for _,c := range b[1:] {
		i = (i<<8)|uint64(c)
	}
	return
}
func SplitOff(b []byte) (n,r []byte) {
	l := len(b)
	if l==0 { return }
	i := int(uint(b[0])>>4)+1
	if i>l {
		n = make([]byte,i)
		copy(n,b)
		return
	}
	n = b[:i]
	r = b[i:]
	return
}
func SplitOffSecond(b []byte) (n,r []byte) {
	l := len(b)
	if l==0 { return }
	i := int(uint(b[0])>>4)+1
	if i>=l { return }
	n = b[:i]
	r = b[i:]
	return
}


