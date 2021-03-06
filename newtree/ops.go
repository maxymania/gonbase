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

type TreeOps interface{
	Consistent(p []byte, q interface{}) bool
	
	Union(P Elements) []byte
	
	Penalty(E1,E2 []byte) float64
	
	// The FirstSplit functions covers three cases.
	// Given FirstSplit(P,maxsize) -> A,B
	//
	// - Case 1: P is small enough to fit in maxsize.
	//           In this case, return P,nil
	// - Case 2: P is small enough so that A and B can fit in maxsize each.
	//           In this case, split P into A and B evenly.
	// - Case 3: P is so large, that eighter only A or B can fit in maxsize.
	//           In this case, return A,B so that A fits in maxsize.
	//
	// Given FirstSplit(P,maxsize) -> A,B   and .Sort() is implemented
	//       P is sorted and A and B are assumed to be sorted.
	FirstSplit(P Elements,maxsize int) (Elements,Elements)
	
	Sort(E Elements)
}

type _fullSpec_TreeOps interface{
	Overlap(p,q []byte) bool
	Consistent(p []byte, q interface{}) bool
	
	Union(P Elements) []byte
	
	Penalty(E1,E2 []byte) float64
	
	FirstSplit(P Elements,maxsize int) (Elements,Elements)
	
	Sort(E Elements)
}



