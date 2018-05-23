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

type Cookie interface{}

/*
Low Level Storage.
*/
type LLS interface{
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	GetInAt(p int, off int64) (n bufferex.Binary, err error)
	
	// zero-copy update support for Memory mapped files.
	// The returned Buffer and Cookie must be passed to .CommitOutAt().
	// The Buffer MUST NOT be freed by the caller, .CommitOutAt() will handle that.
	GetOutAt(p int, off int64) (n bufferex.Binary, c Cookie, err error)
	CommitOutAt(c Cookie,p bufferex.Binary, off int64) (err error)
	
	Stat() (os.FileInfo, error)
	Sync() error
    Truncate(int64) error
	Close() error
	Unwrap(i interface{}) bool
}

