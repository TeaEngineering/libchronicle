// Copyright 2018 Tea Engineering Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// stub out kx layer (the extern function in k.h) so we can run in main, profile,
// valgrind native code etc.
//
// It's impossible to find the source of memory leaks with the Kx slab allocator
// as valgrind can't hook the individual allocations to record the stack.
//
#ifndef MOCK_K
#define MOCK_K

#define KERR -128

K krr(const S msg);
K orr(const S msg);
K ee(K ignored);
K ki(int i);
K kj(long long i);
K kss(const char* ss);
K dl(void* fnptr, J n);

typedef K (*kfunc_1arg)(K);
typedef K (*kfunc_2arg)(K,K);
typedef K (*kfunc_3arg)(K,K,K);

K dot(K x, K y);
K knk(int n, ...);
K ktn(int type, long long n);// dummy serialiser returns a single byte array [SOH]
K b9(I mode, K obj);
K d9(K obj);
int okx(K obj);
K pe(K x);
void per(K x);
void r0(K x);
K r1(K x);

#endif