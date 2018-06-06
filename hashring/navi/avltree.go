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


package navi


import avl "github.com/emirpasic/gods/trees/avltree"

/*
Explanation:

  B
 / \
A   C

A is the left child of B
C is the right child of B

B is the right parent of A
B is the left parent of C
*/

type AvlDirection uint
const (
	AvlForward  AvlDirection = 1
	AvlBackward AvlDirection = 0
)

func AvlStep(r *avl.Node,dir AvlDirection) *avl.Node {
	dir &= 1
	/* Navigate to the right child.*/
	if r.Children[dir]!=nil {
		r = r.Children[dir]
		/* Navigate to the left-most node in the subtree. */
		for {
			l := r.Children[dir^1]
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first right parent. */
	for {
		p := r.Parent
		if p==nil { break }
		if p.Children[dir]==r { r = p; continue }
		return p
	}
	return nil
}

func AvlFloorRing(tree *avl.Tree,key interface{}) *avl.Node {
	if n,ok := tree.Floor(key); ok { return n }
	return tree.Right()
}
func AvlCeilingRing(tree *avl.Tree,key interface{}) *avl.Node {
	if n,ok := tree.Ceiling(key); ok { return n }
	return tree.Left()
}

func AvlStepRing(r *avl.Node,dir AvlDirection) *avl.Node {
	dir &= 1
	/* Navigate to the right child.*/
	if r.Children[dir]!=nil {
		r = r.Children[dir]
		/* Navigate to the left-most node in the subtree. */
		for {
			l := r.Children[dir^1]
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first right parent. */
	for {
		p := r.Parent
		if p==nil {
			for {
				l := r.Children[dir^1]
				if l==nil { break }
				r = l
			}
			return r
		}
		if p.Children[dir]==r { r = p; continue }
		return p
	}
	return nil
}

