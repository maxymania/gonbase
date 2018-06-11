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

import "github.com/valyala/batcher"
import "sync"

const (
	Act_Allocate uint = iota
	Act_Rollback
)

type Action struct{
	Act    uint
	Name   []byte
	Number uint64
	WG *sync.WaitGroup
	Free func(a *Action)
}

type SequenceWorker struct{
	LS *LoggedSequence
	B  batcher.Batcher
}
func (sw *SequenceWorker) process(batch []interface{}) {
	for _,elem := range batch {
		act := elem.(*Action)
		switch act.Act {
		case Act_Allocate:
			l,ok := sw.LS.Lowest(act.Name)
			if ok {
				sw.LS.Delete(Pair{act.Name,l})
			} else {
				l = sw.LS.GetSequence(act.Name)
				l++
				sw.LS.SetSequence(Pair{act.Name,l})
			}
			act.Number = l
		case Act_Rollback:
			sw.LS.Create(Pair{act.Name,act.Number})
		}
	}
	sw.LS.Commit()
	for _,elem := range batch {
		act := elem.(*Action)
		if act.WG!=nil {
			act.WG.Done()
		} else if act.Free!=nil {
			act.Free(act)
		}
	}
}
func (sw *SequenceWorker) Start() {
	sw.B.Func = sw.process
	sw.B.Start()
}
func (sw *SequenceWorker) Stop() { sw.B.Stop() }
func (sw *SequenceWorker) Submit(a *Action) {
	if a!=nil { sw.B.Push(a) }
}


