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
A group of hash functions based on siphash. There are functions with no seed,
with 64 bit seed and with 128 bit seed. All functions call into the siphash
function with the desired output-length.

The siphash implementation for this package is:
	"github.com/dchest/siphash"

*/
package dhash

import "github.com/dchest/siphash"

// Some primes between 2^63 and 2^64 for various uses. Copied from farmhash.
const k0 uint64 = 0xc3a5c85c97cb3127
const k1 uint64 = 0xb492b66fbe98f273
const k2 uint64 = 0x9ae16a3b2f90404f

func Hash64(p []byte) uint64 {
	return siphash.Hash(k0,k1,p)
}

/* Note that 16-byte output is considered experimental by SipHash authors at this time. */
func Hash128(p []byte) (uint64, uint64) {
	return siphash.Hash128(k0,k1,p)
}

func Hash64WithSeed(p []byte, seed0 uint64) uint64 {
	return siphash.Hash(k2,seed0,p)
}

/* Note that 16-byte output is considered experimental by SipHash authors at this time. */
func Hash128WithSeed(p []byte, seed0 uint64) (uint64, uint64) {
	return siphash.Hash128(k2,seed0,p)
}

func Hash64WithSeeds(p []byte, seed0, seed1 uint64) uint64 {
	return siphash.Hash(seed0,seed1,p)
}

/* Note that 16-byte output is considered experimental by SipHash authors at this time. */
func Hash128WithSeeds(p []byte, seed0, seed1 uint64) (uint64, uint64) {
	return siphash.Hash128(seed0,seed1,p)
}


