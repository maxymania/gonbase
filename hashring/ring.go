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


package hashring

import avl "github.com/emirpasic/gods/trees/avltree"
import "github.com/emirpasic/gods/utils"
import "github.com/maxymania/gonbase/hashring/dhash"

func next(n *avl.Node) *avl.Node {
	m := n.Next()
	if m!=nil { return m }
	/* Step 1: navigate to root. */
	for {
		m = n.Parent
		if m==nil { break }
		n = m
	}
	/* Step 2: navigate to the leftmost. */
	for {
		m = n.Children[0]
		if m==nil { break }
		n = m
	}
	return n
}

type Ring64 struct {
	tree *avl.Tree
}
func (s *Ring64) Init() {
	s.tree = avl.NewWith(utils.UInt64Comparator)
}
func (s *Ring64) AddNode(u uint64,node interface{}) {
	s.tree.Put(u,node)
}
func (s *Ring64) lookup(u uint64) *avl.Node {
	n,_ := s.tree.Floor(u)
	if n==nil { n = s.tree.Right() }
	if n==nil { panic("illegal state: Ring is empty!") }
	return n
}
func (s *Ring64) MutateStore(key []byte,m Mutator) bool {
	lo := dhash.Hash64(key)
	return m.Mutate(lo,s.lookup(lo).Value)
}

type SeedRing64 struct {
	Ring64
	Seed uint64
}
func (s *SeedRing64) MutateStore(key []byte,m Mutator) bool {
	lo := dhash.Hash64WithSeed(key,s.Seed)
	return m.Mutate(lo,s.lookup(lo).Value)
}

type MultiJumpSeedRing64 struct {
	SeedRing64
	N int
}
func (s *MultiJumpSeedRing64) MutateStore(key []byte,m Mutator) bool {
	d := uint64(0xc3a5c85c97cb3127)
	
	for i := s.N; i>0 ; i-- {
		lo := dhash.Hash64WithSeeds(key,s.Seed,d)
		if m.Mutate(lo,s.lookup(lo).Value) { return true }
		d++
	}
	return false
}

type MultiStepSeedRing64 struct {
	SeedRing64
	N int
}
func (s *MultiStepSeedRing64) MutateStore(key []byte,m Mutator) bool {
	lo := dhash.Hash64WithSeed(key,s.Seed)
	n := s.lookup(lo)
	for i := s.N; i>0 ; i-- {
		if m.Mutate(lo,n.Value) { return true }
		n = next(n)
	}
	return false
}

