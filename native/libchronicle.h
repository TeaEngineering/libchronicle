// Copyright 2021 Tea Engineering Ltd.
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

#ifndef FILE_LIBCHRONICLE_SEEN
#define FILE_LIBCHRONICLE_SEEN

#define _GNU_SOURCE

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <glob.h>

#define MAXDATASIZE 1000 // max number of bytes we can get at once

// Chronicle header special bits
#define HD_UNALLOCATED 0x00000000
#define HD_WORKING     0x80000000
#define HD_METADATA    0x40000000
#define HD_EOF         0xC0000000
#define HD_MASK_LENGTH 0x3FFFFFFF
#define HD_MASK_META   HD_EOF


// Public interface
// your glue code will need to cast COBJ in callbacks, by implmenting
// four callbacks.
typedef void* COBJ;
typedef void* DISPATCH_CTX;
//
// parsedata_f takes void* and returns custom object. Deserialise, memcpy
//              or return same ptr to dispatch ref valid for callback.
// sizeof_f    tells library how many bytes required to serialise user object
// write_f     takes custom object and writes bytes to void*
// dispatch_f  takes custom object and index, delivers to application with user data
typedef COBJ   (*cparse_f)    (unsigned char*,int);
typedef size_t (*csizeof_f)   (COBJ);
typedef size_t (*cappend_f)   (unsigned char*,size_t,COBJ);
typedef int    (*cdispatch_f) (DISPATCH_CTX,uint64_t,COBJ);

// forward definition of queue
typedef struct queue queue_t;
typedef struct tailer tailer_t;

// collect structure - we complete values for the caller
typedef struct {
    COBJ msg;
    size_t sz;
    uint64_t index;
} collected_t;

queue_t*    chronicle_init(char* dir, cparse_f parser, csizeof_f append_sizeof, cappend_f append_write);
int         chronicle_close(queue_t* queue_delete);
const char* chronicle_strerror();

tailer_t*   chronicle_tailer(queue_t *queue, cdispatch_f dispatcher, DISPATCH_CTX dispatch_ctx, uint64_t index);
void        chronicle_tailer_close(tailer_t* tailer);

void        chronicle_peek();
void        chronicle_peek_queue(queue_t *queue);
int         chronicle_peek_tailer(queue_t *queue, tailer_t *tailer);

void        chronicle_debug();
void        chronicle_debug_tailer(queue_t* queue, tailer_t* tailer);

uint64_t    chronicle_append(queue_t *queue, COBJ msg);
uint64_t    chronicle_append_ts(queue_t *queue, COBJ msg, long ms);

COBJ        chronicle_collect(tailer_t *tailer, collected_t *collect);

#endif
