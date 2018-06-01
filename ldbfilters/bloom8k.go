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


package ldbfilters

import farm "github.com/dgryski/go-farm"
import "github.com/syndtr/goleveldb/leveldb/filter"

type bloom8kGenerator []uint64

func (b *bloom8kGenerator) Add(key []byte) {
	*b = append(*b,farm.Fingerprint64(key))
}
func (b bloom8kGenerator) Generate(w filter.Buffer) {
	var dat [4]uint16
	buf := w.Alloc(1<<13)
	
	for _,h := range b {
		for i := range dat {
			dat[i] = uint16(h)
			h>>=16
		}
		for _,s := range dat {
			buf[s>>3] |= (1<<(s&7))
		}
	}
}

var _ filter.FilterGenerator = (*bloom8kGenerator)(nil)

type bloom8k struct{}
func (b bloom8k) Name() string { return "gonbase.Bloom8K" }
func (b bloom8k) NewGenerator() filter.FilterGenerator { return new(bloom8kGenerator) }
func (b bloom8k) Contains(filter, key []byte) bool {
	if len(filter)<(1<<13) { return true }
	var dat [4]uint16
	h := farm.Fingerprint64(key)
	for i := range dat {
		dat[i] = uint16(h)
		h>>=16
	}
	for _,s := range dat {
		if (filter[s>>3] & (1<<(s&7)))==0 { return false }
	}
	return true
}

var Bloom8K filter.Filter = bloom8k{}

