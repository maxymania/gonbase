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

/*
An implementation of consistent hashing or hash-rings, specifically, static,
non-migrating hash-rings. These rings are static and non-migrating by not
being designed to change at runtime (no insertion or removal of nodes).
This departs this implementation from other hash-ring systems, that are
designed to dynamically distribute data onto a cluster while nodes are
constantly being added and removed.

This implementation is designed with empathis on systems, in which "nodes"
have limited disk-space while storing a dataset that possibly exceeds their
disk-capacity, rendering certain "nodes" incapable to store further records.

In order not to loose those records, techniques such as those similar to
Cuckoo-Hashing are applied.
*/
package hashring

type Mutator interface{
	Mutate(hash uint64,obj interface{}) bool
}

