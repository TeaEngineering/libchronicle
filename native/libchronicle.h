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

// chronicle_init flags values (OR values together)
#define CHRONICLE_FLAGS_ANY
#define CHRONICLE_FLAGS_V4
#define CHRONICLE_FLAGS_V5

#define CHRONICLE_FLAGS_RW
#define CHRONICLE_FLAGS_CREATE



// Public interface
// your glue code will need to cast COBJ in callbacks, by implmenting
// four callbacks.
typedef void* COBJ;
typedef void* DISPATCH_CTX;
//
// cparse_f     takes void* and returns custom object. Deserialise, memcpy
//               or return same ptr to dispatch ref valid for callback.
// csizeof_f    tells library how many bytes required to serialise user object
// cappend_f    takes custom object and writes bytes to void*
// cdispatch_f  takes custom object and index, delivers to application with user data
typedef COBJ   (*cparse_f)    (unsigned char*, int);
typedef void   (*cparsefree_f)(COBJ);
typedef size_t (*csizeof_f)   (COBJ);
typedef void   (*cappend_f)   (unsigned char*,COBJ,size_t);
typedef int    (*cdispatch_f) (DISPATCH_CTX,uint64_t,COBJ);

// forward definition of queue
typedef struct queue queue_t;
typedef struct tailer tailer_t;

// return codes exposed via. chronicle_tailer_state
//     0   awaiting next entry
//     1   hit working
//     2   missing queuefile indicated, awaiting advance or creation
//     3   fstat failed
//     4   mmap failed (probably fatal)
//     5   not yet polled
//     6   queuefile at fid needs extending on disk
//     7   a value was collected
typedef enum {TS_AWAITING_ENTRY, TS_BUSY, TS_AWAITING_QUEUEFILE, TS_E_STAT, TS_E_MMAP, TS_PEEK, TS_EXTEND_FAIL, TS_COLLECTED} tailstate_t;

// collect structure - we complete values for the caller
typedef struct {
    COBJ msg;
    size_t sz;
    uint64_t index;
} collected_t;

queue_t*    chronicle_init(char* dir);
void        chronicle_set_version(queue_t* queue, int version);
int         chronicle_set_roll_scheme(queue_t* queue, char* scheme);
int         chronicle_set_roll_dateformat(queue_t* queue, char* scheme);
void        chronicle_set_encoder(queue_t* queue, csizeof_f append_sizeof, cappend_f append_write);
void        chronicle_set_decoder(queue_t* queue, cparse_f parser, cparsefree_f parsefree);
void        chronicle_set_create(queue_t* queue, int create);
int         chronicle_open(queue_t* queue);
int         chronicle_cleanup(queue_t* queue);

COBJ        chronicle_decoder_default_parse(unsigned char*, int);
size_t      chronicle_encoder_default_sizeof(COBJ);
void        chronicle_encoder_default_write(unsigned char*,COBJ,size_t);

int         chronicle_get_version(queue_t* queue);
char*       chronicle_get_roll_scheme(queue_t* queue);
char*       chronicle_get_roll_format(queue_t* queue);
char*       chronicle_get_cycle_fn(queue_t* queue, int cycle);


const char* chronicle_strerror();

tailer_t*   chronicle_tailer(queue_t *queue, cdispatch_f dispatcher, DISPATCH_CTX dispatch_ctx, uint64_t index);
void        chronicle_tailer_close(tailer_t* tailer);
tailstate_t chronicle_tailer_state(tailer_t* tailer);
uint64_t    chronicle_tailer_index(tailer_t* tailer);

void        chronicle_peek();
void        chronicle_peek_queue(queue_t *queue);
int         chronicle_peek_tailer(tailer_t *tailer);

void        chronicle_debug();
void        chronicle_debug_tailer(queue_t* queue, tailer_t* tailer);

uint64_t    chronicle_append(queue_t *queue, COBJ msg);
uint64_t    chronicle_append_ts(queue_t *queue, COBJ msg, long ms);

COBJ        chronicle_collect(tailer_t *tailer, collected_t *collect);
void        chronicle_return(tailer_t *tailer, collected_t *collect);

struct ROLL_SCHEME {
    char*    name;
    char*    formatstr;
    uint32_t roll_length_secs;
    uint32_t entries;
    uint32_t index;
};

extern struct ROLL_SCHEME chronicle_roll_schemes[];

#endif
