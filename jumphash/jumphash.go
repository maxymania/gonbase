/*
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>

Alternatively, the following license terms can be used:

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
the jump consistent hash algorithm[1] by John Lamping and Eric Veach.

Reference C++ implementation[1]

 int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
   int64_t b = -1, j = 0;
   while (j < num_buckets) {
     b = j;
     key = key * 2862933555777941757ULL + 1;
     j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
   }
   return b;
 }

This implementation, unlike the C++ reference implementation,
does not make use of floating point arithmetic.

Here is the C++ Pseudo-Code of my non-FPU algorithm.

 int32_t JumpConsistentHash_NonFPU(uint64_t key, int32_t num_buckets) {
   int64_t b = -1, j = 0;
   while (j < num_buckets) {
     b = j;
     key = key * 2862933555777941757ULL + 1;
     j = ((b + 1) << 31) / int64_t((key >> 33) + 1);
   }
   return b;
 }

This implementation runs faster (no FP-arithmetic and less calculations) and does
not suffer from FPU-bugs.

[1] http://arxiv.org/pdf/1406.2294v1.pdf
*/
package jumphash


func Hash(key uint64, buckets int32) int32 {
	var b, j int64
	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = ((b+1)<<31)/int64((key>>33)+1)
	}
	return int32(b)
}

