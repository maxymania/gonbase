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


package replidb

import "github.com/vmihailenco/msgpack"
import "path/filepath"
import "os"
import "reflect"
import "bytes"
import "sync"
import "errors"

const (
	Op_Create uint = iota
	Op_Delete
	Op_SetSequence
)

const (
	pLast    = "last"
	pCurrent = "current"
)

var ENoTarget = errors.New("NoTarget")
var EStartupRecoverFailed = errors.New("StartupRecoverFailed")

type RowEvent struct{
	Pair
	Op uint
}
func (r *RowEvent) DecodeMsgpack(dec *msgpack.Decoder) error { return dec.Decode(&r.Op,&r.Pair.Name,&r.Pair.Num) }
func (r *RowEvent) EncodeMsgpack(enc *msgpack.Encoder) error { return enc.Encode( r.Op, r.Pair.Name, r.Pair.Num) }

type LoggedSequence struct{
	Seq *Sequence
	Path string
	
	/* Replication outbound. */
	Replicator func(b []byte) error
	
	MaxLogBeforeMerge int64
	
	buf bytes.Buffer
	enc *msgpack.Encoder
	target *os.File
	
	oplock,tlock sync.Mutex
	
	signal chan struct{}
}
func (l *LoggedSequence) innerReceive(repl []byte) error {
	l.oplock.Lock(); defer l.oplock.Unlock()
	var ev RowEvent
	dec := msgpack.NewDecoder(bytes.NewBuffer(repl))
	vev := reflect.ValueOf(&ev)
	for {
		if dec.DecodeValue(vev)!=nil { break }
		l.perform(ev)
	}
	l.buf.Write(repl)
	return nil
}

/* Replication inbound. */
func (l *LoggedSequence) OnReceive(repl []byte) error {
	err := l.innerReceive(repl)
	if err!=nil { return err }
	return l.Commit()
}

func (l *LoggedSequence) loadAndCommit() bool {
	var ev RowEvent
	db := filepath.Join(l.Path,pLast)
	f,err := os.Open(db)
	if err!=nil { return os.IsNotExist(err) }
	defer f.Close()
	dec := msgpack.NewDecoder(f)
	vev := reflect.ValueOf(&ev)
	for {
		if dec.DecodeValue(vev)!=nil { return true }
		l.perform(ev)
	}
}
func (l *LoggedSequence) startupRecover() bool {
	if !l.loadAndCommit() { return false }
	if l.Seq.BGFlush()!=nil { return false }
	os.Remove(filepath.Join(l.Path,pLast))
	os.Rename(filepath.Join(l.Path,pCurrent),filepath.Join(l.Path,pLast))
	if !l.loadAndCommit() { return false }
	if l.Seq.BGFlush()!=nil { return false }
	os.Remove(filepath.Join(l.Path,pLast))
	return true
}
func (l *LoggedSequence) perform(e RowEvent) {
	switch e.Op {
	case Op_Create: l.Seq.Create(e.Pair)
	case Op_Delete: l.Seq.Delete(e.Pair)
	case Op_SetSequence: l.Seq.SetSequence(e.Pair)
	}
}
func (l *LoggedSequence) client(e RowEvent) {
	l.oplock.Lock(); defer l.oplock.Unlock()
	l.enc.Encode(&e)
	l.perform(e)
}

func (l *LoggedSequence) Create(pair Pair) { l.client(RowEvent{pair,Op_Create}) }
func (l *LoggedSequence) Delete(pair Pair) { l.client(RowEvent{pair,Op_Delete}) }
func (l *LoggedSequence) SetSequence(pair Pair) { l.client(RowEvent{pair,Op_SetSequence}) }
func (l *LoggedSequence) GetSequence(name []byte) uint64 { return l.Seq.GetSequence(name) }
func (l *LoggedSequence) Lowest(name []byte) (uint64,bool) { return l.Seq.Lowest(name) }

func (l *LoggedSequence) Commit() error {
	l.tlock.Lock(); defer l.tlock.Unlock()
	if l.target==nil { return ENoTarget }
	if l.Replicator!=nil {
		err := l.Replicator(l.buf.Bytes())
		if err!=nil { return err }
	}
	_,err := l.buf.WriteTo(l.target)
	if err!=nil { return err }
	err = l.target.Sync()
	if err!=nil { return err }
	
	l.buf.Reset()
	
	/* This tail is optional. Don't propagate a fail! */
	fi,err := l.target.Stat()
	if err!=nil { return nil /* Don't propagate this error. */ }
	if l.MaxLogBeforeMerge <= fi.Size() {
		select {
		case l.signal <- struct{}{}:
		default:
		}
	}
	return nil
}

func (l *LoggedSequence) rename() error {
	var err2 error
	l.tlock.Lock(); defer l.tlock.Unlock()
	l.target.Close()
	l.target = nil
	err := os.Rename(filepath.Join(l.Path,pCurrent),filepath.Join(l.Path,pLast))
	l.target,err2 = os.Create(filepath.Join(l.Path,pCurrent))
	if err2!=nil { l.target=nil }
	return err
}
func (l *LoggedSequence) bgWorker() {
	for {
		<- l.signal
		if _,err := os.Stat(filepath.Join(l.Path,pLast)); err!=nil && os.IsNotExist(err) {
			err := l.rename()
			if err!=nil { continue }
		}
		err := l.Seq.BGFlush()
		if err!=nil { continue }
		os.Remove(filepath.Join(l.Path,pLast))
	}
}
func (l *LoggedSequence) Startup() error {
	var err2 error
	l.enc = msgpack.NewEncoder(&l.buf)
	if !l.startupRecover() { return EStartupRecoverFailed }
	l.signal = make(chan struct{},1)
	l.target,err2 = os.Create(filepath.Join(l.Path,pCurrent))
	if err2!=nil { l.target=nil }
	go l.bgWorker()
	return nil
}



