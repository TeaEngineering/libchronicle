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

#define _GNU_SOURCE

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <glob.h>
#include <time.h>

#include "k.h"
#include "wire.h"

/**
 * Implementation notes
 * This code is not reentrant, but there is only one main thread, so this is not a problem.
 * Should not be used by additional threads without external locking.
 * Multiple processes can append and tail from a queue concurrently, and one process
 * may read or write from multiple queues.
 *
 * TODO: current iteration requires a Java Chronicle-Queues appender to be writing to the same
 * queue on a periodic timer to roll over the log files correctly and maintain the index structures.
 *
 * It should be possible to append to a queue from a tailer callback on same queue, so the fds
 * and mmaps used for writing are separate to those used in recovery.
 *
 * Most of the appender logic is actually just the tailer logic, followed by cas write-lock.
 * For interesting concurrency parts search 'lock_cmpxchgl' and 'mfence'.
 *
 * To gurantee we can read and write payloads of 'blocksize' length we map
 * 2x blocksize each time, aligning the map offset to be a multiple of blocksize from
 * the start of the file. If the block parser (parse_queue_block) is about to step
 * over the buffer end, it returns asking for advance. There are two outcomes: either
 * we are positioned in 2nd block ('overhang'), and our mmap() will advance one block
 * completing the parse, or having tried that the parser will stop again without moving
 * in which case we double the blocksize.
 *
 * The interesting caller of parse_queue_block is shmipc_peek_tailer_r, which handles
 * splitting the index into cycle and seqnum, determinging the filenames, opening fids,
 * repositioning the buffer and interpreting the return codes from the block parser. It
 * consists of a while (1) loop, repeating until one of the enumerated states
 * prevents it from doing any further work. Reasons are in tailer_state_messages and shown
 * in the debug output. shmipc_peek_tailer_r re-uses existing generated filenames, fids
 * and maps from previous invocations as much as possible.
 *
 * patch_cycles is a variable that controls compatability with Java and is only relevant
 * when opening an old queue that may not have been written to recently. When an appender is
 * started, it starts seeking for the write position from (highetCycle-patch_cycles),
 * which causes a replay to occur internally and ensures that any previous cycles
 * covered by the replay are patched up to end with an EOF marker. For symnetry, if a
 * reader finds itself stuck at an available marker in a queuefile for a
 * cycle < (highestCycle-patch_cycles), it is permitted to skip over the missing EOF.
 * Java acheives something similar using less-elegant timeouts.
 *
 *
 */

// MetaDataKeys `header`index2index`index`roll

#define MAXDATASIZE 1000 // max number of bytes we can get at once

// Chronicle header special bits
#define HD_UNALLOCATED 0x00000000
#define HD_WORKING     0x80000000
#define HD_METADATA    0x40000000
#define HD_EOF         0xC0000000
#define HD_MASK_LENGTH 0x3FFFFFFF
#define HD_MASK_META   HD_EOF

#define KERR -128

// function pointer typedefs
struct queue;
typedef int (*parsedata_f)(unsigned char*,int,uint64_t,void* userdata);
typedef int (*appenddata_f)(unsigned char*,int,int*,K);
typedef K (*encodecheck_f)(struct queue*,K);

typedef struct {
    unsigned char *highest_cycle;
    unsigned char *lowest_cycle;
    unsigned char *modcount;
} dirlist_fields_t;

typedef struct tailer {
    uint64_t          dispatch_after; // for resume support
    int               state;
    K                 callback;
    // to support the 'collect' operation to wait and return next item, ignoring callback
    int               collect;
    K                 collected_value;

    int               mmap_protection; // PROT_READ etc.

    // currently open queue file
    uint64_t          qf_cycle_open;
    char*             qf_fn;
    struct stat       qf_statbuf;
    int               qf_fd;

    uint64_t          qf_tip; // byte position of the next header, or zero if unknown
    uint64_t          qf_index; // seqnum of the header pointed to by qf_tip

    // currently mapped region: buffer, offset (from 0 in file), size
    unsigned char*    qf_buf;
    uint64_t          qf_mmapoff;
    uint64_t          qf_mmapsz;

    struct queue*     queue;
    int               handle;

    struct tailer*    next;
} tailer_t;

typedef struct queue {
    char*             dirname;
    char*             hsymbolp;
    uint              blocksize;

    // directory-listing.cq4t
    char*             dirlist_name;
    int               dirlist_fd;
    struct stat       dirlist_statbuf;
    unsigned char*    dirlist; // mmap base
    dirlist_fields_t  dirlist_fields;

    char*             queuefile_pattern;
    glob_t            queuefile_glob; // last glob of data files, refreshed by poll on modcount

    // values observed from directory-listing poll
    uint64_t          highest_cycle;
    uint64_t          lowest_cycle;
    uint64_t          modcount;

    // roll config populated from (any) queuefile header
    int               roll_length;
    int               roll_epoch;
    char*             roll_format;
    int               index_count;
    int               index_spacing;

    int               cycle_shift;
    uint64_t          seqnum_mask;

    char* (*cycle2file_fn)(struct queue*,int);

    parsedata_f       parser;
    appenddata_f      encoder;
    encodecheck_f     encodecheck;

    tailer_t*         tailers;

    // the appender is a shared tailer, polled by append[], with writing logic
    // and no callback to user code for events
    tailer_t*         appender;

    struct queue*     next;
} queue_t;

// paramaters that control behavior, not exposed for modification
uint32_t patch_cycles = 3;
long int qf_disk_sz = 83754496L;

// globals
char* debug = NULL;
uint32_t pid_header = 0;
queue_t* queue_head = NULL;
tailer_t** tailer_handles;
int tailer_handles_count = 0;

// forward declarations
void parse_dirlist(queue_t*);
void parse_queuefile_meta(unsigned char*, int, queue_t*);
void parse_queuefile_data(unsigned char*, int, queue_t*, tailer_t*, uint64_t);
int shmipc_peek_tailer(queue_t*, tailer_t*);
void shmipc_peek_queue(queue_t*);
void shmipc_debug_tailer(queue_t*, tailer_t*);
K shmipc_append_ts(K, K, K);
K queuefile_init(char*, queue_t*);
K directory_listing_reopen(queue_t*, int, int);

// compare and swap, 32 bits, addressed by a 64bit pointer
static inline uint32_t lock_cmpxchgl(unsigned char *mem, uint32_t newval, uint32_t oldval) {
    __typeof (*mem) ret;
    __asm __volatile ("lock; cmpxchgl %2, %1"
    : "=a" (ret), "=m" (*mem)
    : "r" (newval), "m" (*mem), "0" (oldval));
    return (uint32_t) ret;
}

static inline uint32_t lock_xadd(unsigned char* mem, uint32_t val) {
    __asm__ volatile("lock; xaddl %0, %1"
    : "+r" (val), "+m" (*mem) // input+output
    : // No input-only
    : "memory"
    );
    return (uint32_t)val;
}

char* get_cycle_fn_yyyymmdd(queue_t* queue, int cycle) {
    // TODO: replace with https://ideone.com/7BADb as gmtime_r leaks
    char* buf;
    // time_t aka long. seconds since midnight 1970
    time_t rawtime = cycle * 60*60*24;
    struct tm info;
    gmtime_r(&rawtime, &info);

    // tail of this buffer is overwritten by the strftime below
    int bufsz = asprintf(&buf, "%s/yyyymmdd.cq4", queue->dirname);
    strftime(buf+bufsz-12, 13, "%Y%m%d.cq4", &info);
    return buf;
}

void queue_double_blocksize(queue_t* queue) {
    uint new_blocksize = queue->blocksize << 1;
    printf("shmipc:  doubling blocksize from %x to %x\n", queue->blocksize, new_blocksize);
    queue->blocksize = new_blocksize;
}

// return 0 to continue dispaching, 7 to signal collected item
int dispatch_callback(tailer_t* tailer, uint64_t index, K obj) {
    K arg = knk(2, kj(index), obj);
    if (tailer->collect) {
        tailer->collected_value = arg;
        return 7;
    }

    K r = dot(tailer->callback, arg);
    r0(arg);
    if (r == NULL) {
        printf(" shmipc: caution, error signalled by callback (returned NULL)\n");
        return 0;
    } else if (r && r->t == KERR) {
        printf(" shmipc: callback error string: %s\n", r->s);
    }
    r0(r);
    return 0;
}

int parse_data_text(unsigned char* base, int lim, uint64_t index, void* userdata) {
    tailer_t* tailer = (tailer_t*)userdata;
    if (debug) printf(" text: %" PRIu64 " '%.*s'\n", index, lim, base);

    // prep args and fire callback
    if (tailer->callback && index > tailer->dispatch_after) {
        K msg = ktn(KC, lim); // don't free this, handed over to q interp
        memcpy((char*)msg->G0, base, lim);
        return dispatch_callback(tailer, index, msg);
    }
    return 0;
}

int append_data_text(unsigned char* base, int lim, int* sz, K msg) {
    return 0;
}

K append_check_text(queue_t* queue, K msg) {
    if (msg->t != KC) return krr("msg must be KC");
    while (msg->n > queue->blocksize)
        queue_double_blocksize(queue);
    return msg;
}

int parse_data_kx(unsigned char* base, int lim, uint64_t index, void* userdata) {
    tailer_t* tailer = (tailer_t*)userdata;

    // prep args and fire callback
    if (tailer->callback && index > tailer->dispatch_after) {
        K msg = ktn(KG, lim);
        memcpy((char*)msg->G0, base, lim);
        int ok = okx(msg);
        if (ok) {
            K out = d9(msg);
            r0(msg);
            return dispatch_callback(tailer, index, out);
        } else {
            if (debug) printf("shmipc: caution index %" PRIu64 " bytes !ok as kx, skipping\n", index);
        }
    }
    return 0;
}

int append_data_kx(unsigned char* base, int lim, int* sz, K msg) {
    r0(msg);
    return 0;
}

K append_check_kx(queue_t* queue, K msg) {
    K r = b9(3, msg);
    if (r == NULL) {
        printf("shmipc: failed to serialise msg using b9 - aborting\n");
        abort();
    }
    if (debug) printf("shmipc: kx persist needs %lld bytes\n", r->n);
    while (r->n > queue->blocksize)
        queue_double_blocksize(queue);
    return r;
}

K shmipc_init(K dir, K parser) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle (starts with :)");
    if (parser->t != -KS) return krr("parser is not symbol");

    debug = getenv("SHMIPC_DEBUG");
    wire_trace = getenv("SHMIPC_WIRETRACE");

    pid_header = (getpid() & HD_MASK_LENGTH);

    printf("shmipc: opening dir %s format %s\n", dir->s, parser->s);

    // check if queue already open
    queue_t* queue = queue_head;
    while (queue != NULL) {
        if (queue->hsymbolp == dir->s) return krr("shmipc dir dupe init");
        queue = queue->next;
    }

    // allocate struct, we'll link if all checks pass
    queue = malloc(sizeof(queue_t));
    if (queue == NULL) return krr("m fail");
    bzero(queue, sizeof(queue_t));

    // unsafe to use ref to q symbol here (seen it go out of scope), so dup
    queue->dirname = strdup(&dir->s[1]);
    queue->blocksize = 1024*1024; // must be a power of two (single 1 bit)

    // Is this a directory
    struct stat statbuf;
    if (stat(queue->dirname, &statbuf) != 0) return krr("stat fail");
    if (!S_ISDIR(statbuf.st_mode)) return krr("dir is not a directory");

    // Does it contain some .cq4 files?
    glob_t *g = &queue->queuefile_glob;
    g->gl_offs = 0;

    asprintf(&queue->queuefile_pattern, "%s/*.cq4", queue->dirname);
    glob(queue->queuefile_pattern, GLOB_ERR, NULL, g);
    if (g->gl_pathc < 1) {
        return krr("no queue files - java run?");
    }
    if (debug) {
        printf("shmipc: glob %zu queue files found\n", g->gl_pathc);
        for (int i = 0; i < g->gl_pathc;i++) {
            printf("   %s\n", g->gl_pathv[i]);
        }
    }

    // Can we map the directory-listing.cq4t file
    asprintf(&queue->dirlist_name, "%s/directory-listing.cq4t", queue->dirname);
    K x = ee(directory_listing_reopen(queue, O_RDONLY, PROT_READ));
    if (x && x->t == KERR && x->s) {
        printf("shmipc: dir listing %p %s\n", x, x->s); return krr(x->s);
    }

    // read a 'queue' header from any one of the datafiles to get the
    // rollover configuration. We don't know how to generate a filename from a cycle code
    // yet, so this needs to use the directory listing.
    int               queuefile_fd;
    struct stat       queuefile_statbuf;
    uint64_t          queuefile_extent;
    unsigned char*    queuefile_buf;

    char* fn = queue->queuefile_glob.gl_pathv[0];
    // find size of dirlist and mmap
    if ((queuefile_fd = open(fn, O_RDONLY)) < 0) {
        return orr("qfi open");
    }
    if (fstat(queuefile_fd, &queuefile_statbuf) < 0)
        return krr("qfi fstat");

    // only need the first block
    queuefile_extent = queuefile_statbuf.st_size < queue->blocksize ? queuefile_statbuf.st_size : queue->blocksize;

    if ((queuefile_buf = mmap(0, queuefile_extent, PROT_READ, MAP_SHARED, queuefile_fd, 0)) == MAP_FAILED)
        return krr("qfi mmap fail");

    // we don't need a data-parser at this stage as only need values from the header
    if (debug) printf("shmipc: parsing queuefile %s 0..%" PRIu64 "\n", fn, queuefile_extent);
    parse_queuefile_meta(queuefile_buf, queuefile_extent, queue);

    // close queuefile
    munmap(queuefile_buf, queuefile_extent);
    close(queuefile_fd);

    // check we loaded some rollover settings from queuefile
    if (queue->roll_length == 0) krr("qfi roll_length fail");
    if (queue->index_count == 0) krr("qfi index_count fail");
    if (queue->index_spacing == 0) krr("qfi index_spacing fail");
    if (queue->roll_format == 0) krr("qfi roll_format fail");

    // TODO check yyyymmdd
    queue->cycle2file_fn = &get_cycle_fn_yyyymmdd;
    queue->cycle_shift = 32;
    queue->seqnum_mask = 0x00000000FFFFFFFF;
    // TODO: Logic from RollCycles.java ensures rollover occurs before we run out of index2index pages?
    //  cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));

    // verify user-specified parser for data segments
    if (strncmp(parser->s, "text", parser->n) == 0) {
        queue->parser = &parse_data_text;
        queue->encoder = &append_data_text;
        queue->encodecheck = &append_check_text;
    } else if (strncmp(parser->s, "kx", parser->n) == 0) {
        queue->parser = &parse_data_kx;
        queue->encoder = &append_data_kx;
        queue->encodecheck = &append_check_kx;
    } else {
        return krr("bad format: supports `kx and `text");
    }
    if (debug) printf("shmipc: format set to %.*s\n", (int)parser->n, parser->s);

    // Good to use
    queue->next = queue_head;
    queue->hsymbolp = dir->s;
    queue_head = queue;

    // avoids a tailer registration before we have a minimum cycle
    shmipc_peek_queue(queue);
    if (debug) printf("shmipc: init complete\n");

    return (K)NULL;

    // wip: kernel style unwinder
//unwind_1:
    free(queue->dirlist_name);
    free(queue->queuefile_pattern);
    close(queue->dirlist_fd);
    globfree(&queue->queuefile_glob);
    // unlink LL if entered
    munmap(queue->dirlist, queue->dirlist_statbuf.st_size);
//unwind_2:
    free(queue);

    return (K)NULL;

}

// return codes
//    0  awaiting at &base
//    1  we hit working
//    2  we hit EOF
//    3  data extent will cross base+limit
//    4  hit data with no data parser
//   (7  collected value - from parse_data)
// if any entries are read the values at basep and indexp are updated
// if parse_data returns non-zero, we pause parsing after the current item
int parse_queue_block(unsigned char** basep, uint64_t *indexp, unsigned char* extent, wirecallbacks_t* hcbs, parsedata_f parse_data, void* userdata) {
    uint32_t header;
    int sz;
    unsigned char* base = *basep;
    uint64_t index = *indexp;
    int pd = 0;
    while (!pd) {
        if (base+4 >= extent) return 3;
        memcpy(&header, base, sizeof(header)); // relax, fn optimised away
        // no speculative fetches before the header is read
        asm volatile ("mfence" ::: "memory");

        if (header == HD_UNALLOCATED) {
            if (debug) printf(" %" PRIu64 " @%p unallocated\n", index, base);
            return 0;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            if (debug) printf(" @%p locked for writing by pid %d\n", base, header & HD_MASK_LENGTH);
            return 1;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            if (debug) printf(" @%p metadata size %x\n", base, sz);
            if (base+4+sz >= extent) return 3;
            parse_wire(base+4, sz, 0, hcbs);
            // EventName  header
            //   switch to header parser
        } else if ((header & HD_MASK_META) == HD_EOF) {
            if (debug) printf(" @%p EOF\n", base);
            return 2;
        } else {
            sz = (header & HD_MASK_LENGTH);
            if (debug) printf(" %" PRIu64 " @%p data size %x\n", index, base, sz);
            if (parse_data) {
                if (base+4+sz >= extent) return 3;
                pd = parse_data(base+4, sz, index, userdata);
            } else {
                // bail at first data message
                return 4;
            }
            index++;
            *indexp = index;
        }

        base = base + 4 + sz;
        *basep = base;
    }
    return pd;
}

void handle_dirlist_ptr(char* buf, int sz, unsigned char *dptr, wirecallbacks_t* cbs) {
    // we are preserving *pointers* within the shared directory data page
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "listing.highestCycle", sz) == 0) {
        queue->dirlist_fields.highest_cycle = dptr;
    } else if (strncmp(buf, "listing.lowestCycle", sz) == 0) {
        queue->dirlist_fields.lowest_cycle = dptr;
    } else if (strncmp(buf, "listing.modCount", sz) == 0) {
        queue->dirlist_fields.modcount = dptr;
    }
}

void handle_qf_uint32(char* buf, int sz, uint32_t data, wirecallbacks_t* cbs){
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "length", sz) == 0) {
        queue->roll_length = data;
    }
}

void handle_qf_uint16(char* buf, int sz, uint16_t data, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexCount", sz) == 0) {
        queue->index_count = data;
    }
}

void handle_qf_uint8(char* buf, int sz, uint8_t data, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexSpacing", sz) == 0) {
        queue->index_spacing = data;
    }
}

void handle_qf_text(char* buf, int sz, char* data, int dsz, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "format", sz) == 0) {
        queue->roll_format = strndup(data, dsz);
    }
}

void parse_dirlist(queue_t* queue) {
    // dirlist mmap is the size of the fstat
    int lim = queue->dirlist_statbuf.st_size;
    unsigned char* base = queue->dirlist;
    uint64_t index = 0;

    wirecallbacks_t cbs;
    bzero(&cbs, sizeof(cbs));
    cbs.ptr_uint64 = &handle_dirlist_ptr;
    cbs.userdata = queue;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    parse_queue_block(&base, &index, base+lim, &hcbs, &parse_wire_data, &cbs);
}

void parse_queuefile_meta(unsigned char* base, int limit, queue_t* queue) {
    uint64_t index = 0;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    hcbs.field_uint32 = &handle_qf_uint32;
    hcbs.field_uint16 = &handle_qf_uint16;
    hcbs.field_uint8 =  &handle_qf_uint8;
    hcbs.field_char = &handle_qf_text;
    hcbs.userdata = queue;
    parse_queue_block(&base, &index, base+limit, &hcbs, NULL, NULL);
}

K shmipc_peek(K x) {
    queue_t *queue = queue_head;
    while (queue != NULL) {
        shmipc_peek_queue(queue);
        queue = queue->next;
    }
    return (K)0;
}

void peek_queue_modcount(queue_t* queue) {
    // poll shared directory for modcount
    uint64_t modcount;
    memcpy(&modcount, queue->dirlist_fields.modcount, sizeof(modcount));

    if (queue->modcount != modcount) {
        printf("shmipc: %s modcount changed from %" PRIu64 " to %" PRIu64 "\n", queue->hsymbolp, queue->modcount, modcount);
        // slowpath poll
        memcpy(&queue->modcount, queue->dirlist_fields.modcount, sizeof(modcount));
        memcpy(&queue->lowest_cycle, queue->dirlist_fields.lowest_cycle, sizeof(modcount));
        memcpy(&queue->highest_cycle, queue->dirlist_fields.highest_cycle, sizeof(modcount));
    }
}

void poke_queue_modcount(queue_t* queue) {
    // push modifications to lowestCycle, highestCycle to directory-listing mmap
    // and atomically increment the modcount
    uint64_t modcount;
    memcpy(queue->dirlist_fields.highest_cycle, &queue->highest_cycle, sizeof(modcount));
    memcpy(queue->dirlist_fields.lowest_cycle, &queue->lowest_cycle, sizeof(modcount));
    lock_xadd(queue->dirlist_fields.modcount, 1);
    printf("shmipc: bumped modcount\n");
}

void shmipc_peek_queue(queue_t *queue) {
    if (debug) printf("peeking at %s\n", queue->hsymbolp);
    peek_queue_modcount(queue);

    tailer_t *tailer = queue->tailers;
    while (tailer != NULL) {
        shmipc_peek_tailer(queue, tailer);

        tailer = tailer->next;
    }
}

// return codes exposed via. tailer-> state
//     0   awaiting next entry
//     1   hit working
//     2   missing queuefile indicated, awaiting advance or creation
//     3   fstat failed
//     4   mmap failed (probably fatal)
//     5   not yet polled
//     6   queuefile at fid needs extending on disk
//     7   a value was collected
const char* tailer_state_messages[] = {"AWAITING_ENTRY", "BUSY", "AWAITING_QUEUEFILE", "E_STAT", "E_MMAP", "PEEK?", "EXTEND_FAIL", "COLLECTED"};

int shmipc_peek_tailer_r(queue_t *queue, tailer_t *tailer) {
    // for each cycle file { for each block { for each entry { emit }}}
    // this method run like a generator, suspended in the innermost
    // iteration when we hit the end of the file and pick up at the next peek()
    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));

    while (1) {

        uint64_t cycle = tailer->qf_index >> queue->cycle_shift;
        if (cycle != tailer->qf_cycle_open || tailer->qf_fn == NULL) {
            // free fn, mmap and fid
            if (tailer->qf_fn) {
                free(tailer->qf_fn);
            }
            if (tailer->qf_buf) {
                munmap(tailer->qf_buf, tailer->qf_mmapsz);
                tailer->qf_buf = NULL;
            }
            if (tailer->qf_fd > 0) { // close the fid if open
                close(tailer->qf_fd);
            }
            tailer->qf_fn = queue->cycle2file_fn(queue, cycle);
            tailer->qf_tip = 0;

            printf("shmipc: opening cycle %" PRIu64 " filename %s\n", cycle, tailer->qf_fn);
            int fopen_flags = O_RDONLY;
            if (tailer->mmap_protection != PROT_READ) fopen_flags = O_RDWR;
            if ((tailer->qf_fd = open(tailer->qf_fn, fopen_flags)) < 0) {
                printf("shmipc:  awaiting queuefile for %s open errno=%d %s\n", tailer->qf_fn, errno, strerror(errno));

                // if our cycle < highCycle, permitted to skip a missing file rather than wait
                if (cycle < queue->highest_cycle) {
                    uint64_t skip_to_index = (cycle + 1) << queue->cycle_shift;
                    printf("shmipc:  skipping queuefile (cycle < highest_cycle), bumping next_index from %" PRIu64 " to %" PRIu64 "\n", tailer->qf_index, skip_to_index);
                    tailer->qf_index = skip_to_index;
                    continue;
                }
                return 2;
            }
            tailer->qf_cycle_open = cycle;

            // renew the stat
            if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0) return 3;
        }

        // assert: we have open fid

        //                        qf_tip
        //    file  0               v          stat.sz
        //          [-------------#######-------]
        //    map                 #######
        //                        ^      ^
        //                     mmapoff  +mmapsz
        //                          | basep
        //  addr               qf_buf
        // assign mmap limit and offset from tip and blocksize

        // note: blocksize may have changed, unroll this to a constant with care
        uint64_t blocksize_mask = ~(queue->blocksize-1);
        uint64_t mmapoff = tailer->qf_tip & blocksize_mask;

        // renew stat if we would otherwise map less than 2* blocksize
        // TODO: write needs to extend file here!
        if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize) {
            if (debug) printf("shmmain: approaching file size limit, less than two blocks remain\n");
            if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0)
                return 3;
            // signal to extend queuefile iff we are an appending tailer
            if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize && tailer->mmap_protection != PROT_READ) {
                return 6;
            }
        }

        int limit = tailer->qf_statbuf.st_size - mmapoff > 2*queue->blocksize ? 2*queue->blocksize : tailer->qf_statbuf.st_size - mmapoff;
        if (debug) printf("shmipc:  tip %" PRIu64 " -> mmapoff %" PRIu64 " size 0x%x  blocksize_mask 0x%" PRIx64 "\n", tailer->qf_tip, mmapoff, limit, blocksize_mask);

        // only re-mmap if desired window has changed since last scan
        if (tailer->qf_buf == NULL || mmapoff != tailer->qf_mmapoff || limit != tailer->qf_mmapsz) {
            if ((tailer->qf_buf)) {
                munmap(tailer->qf_buf, tailer->qf_mmapsz);
                tailer->qf_buf = NULL;
            }

            tailer->qf_mmapsz = limit;
            tailer->qf_mmapoff = mmapoff;
            if ((tailer->qf_buf = mmap(0, tailer->qf_mmapsz, tailer->mmap_protection, MAP_SHARED, tailer->qf_fd, tailer->qf_mmapoff)) == MAP_FAILED) {
                printf("shmipc:  mmap failed %s %" PRIx64 " size %" PRIx64 " error=%s\n", tailer->qf_fn, tailer->qf_mmapoff, tailer->qf_mmapsz, strerror(errno));
                tailer->qf_buf = NULL;
                return 4;
            }
            printf("shmipc:  mmap offset %" PRIx64 " size %" PRIx64 " base=%p extent=%p\n", tailer->qf_mmapoff, tailer->qf_mmapsz, tailer->qf_buf, tailer->qf_buf+tailer->qf_mmapsz);
        }

        unsigned char* basep = (tailer->qf_tip - tailer->qf_mmapoff) + tailer->qf_buf; // basep within mmap
        unsigned char* basep_old = basep;
        unsigned char* extent = tailer->qf_buf+tailer->qf_mmapsz;
        uint64_t index = tailer->qf_index;

        //    0  awaiting at &base  (pass)
        //    1  we hit working     (pass)
        //    2  we hit EOF         (handle)
        //    3  data extent will cross base+limit (handle)
        //    4  hit data with no data parser (won't happen)
        //    7  collected item
        // if any entries are read the values at basep and indexp are updated
        int s = parse_queue_block(&basep, &index, extent, &hcbs, queue->parser, tailer);
        //printf("shmipc: block parser result %d, shm %p to %p\n", s, basep_old, basep);

        if (s == 3 && basep == basep_old) {
            queue_double_blocksize(queue);
        }

        if (basep != basep_old) {
            // commit result of parsing to the tailer, adjusting for the window
            uint64_t new_tip = basep-tailer->qf_buf + tailer->qf_mmapoff;
            if (debug) printf("shmipc:  parser moved shm %p to %p, file %" PRIu64 " -> %" PRIu64 ", index %" PRIu64 " to %" PRIu64 "\n", basep_old, basep, tailer->qf_tip, new_tip, tailer->qf_index, index);
            tailer->qf_tip = new_tip;
            tailer->qf_index = index;
        }

        if (s == 1 || s == 7) return s;

        if (s == 0) { // awaiting at end of queuefile
            if (cycle < queue->highest_cycle-patch_cycles) { // allowed to fast-forward
                uint64_t skip_to_index = (cycle + 1) << queue->cycle_shift;
                printf("shmipc:  missing EOF for queuefile (cycle < highest_cycle-patch_cycles), bumping next_index from %" PRIu64 " to %" PRIu64 "\n", tailer->qf_index, skip_to_index);
                tailer->qf_index = skip_to_index;
                continue;
            }
            return s;
        }

        if (s == 2) {
            // we've read an EOF marker, so the next expected index is cycle++, seqnum=0
            uint64_t eof_cycle = ((tailer->qf_index >> queue->cycle_shift) + 1) << queue->cycle_shift;
            printf("shmipc:  hit EOF marker, setting next_index from %" PRIu64 " to %" PRIu64 "\n", tailer->qf_index, eof_cycle);
            tailer->qf_index = eof_cycle;
        }
    }
    return 0;
}

int shmipc_peek_tailer(queue_t *queue, tailer_t *tailer) {
    return tailer->state = shmipc_peek_tailer_r(queue, tailer);
}

K shmipc_debug(K x) {
    printf("shmipc: open handles\n");

    queue_t *current = queue_head;
    while (current != NULL) {
        printf(" handle              %s\n",   current->hsymbolp);
        printf("  blocksize          %x\n",   current->blocksize);
        printf("  dirlist_name       %s\n",   current->dirlist_name);
        printf("  dirlist_fd         %d\n",   current->dirlist_fd);
        printf("  dirlist_sz         %" PRIu64 "\n", (uint64_t)current->dirlist_statbuf.st_size);
        printf("  dirlist            %p\n",   current->dirlist);
        printf("    cycle-low        %" PRIu64 "\n", current->lowest_cycle);
        printf("    cycle-high       %" PRIu64 "\n", current->highest_cycle);
        printf("    modcount         %" PRIu64 "\n", current->modcount);
        printf("  queuefile_pattern  %s\n",   current->queuefile_pattern);
        printf("    cycle_shift      %d\n",   current->cycle_shift);
        printf("    roll_epoch       %d\n",   current->roll_epoch);
        printf("    roll_length (ms) %d\n",   current->roll_length);
        printf("    roll_format      %s\n",   current->roll_format);
        printf("    index_count      %d\n",   current->index_count);
        printf("    index_spacing    %d\n",   current->index_spacing);

        printf("  tailers:\n");
        tailer_t *tailer = current->tailers; // shortcut to save both collections
        while (tailer != NULL) {
            shmipc_debug_tailer(current, tailer);
            tailer = tailer->next;
        }
        printf("  appender:\n");
        if (current->appender)
            shmipc_debug_tailer(current, current->appender);

        current = current->next;
    }
    return 0;
}

void shmipc_debug_tailer(queue_t* queue, tailer_t* tailer) {
    const char* state_text = tailer_state_messages[tailer->state];
    printf("    callback         %p\n",   tailer->callback);
    uint cycle = tailer->dispatch_after >> queue->cycle_shift;
    uint seqnum = tailer->dispatch_after & queue->seqnum_mask;
    printf("    dispatch_after   %" PRIu64 " (cycle %u, seqnum %u)\n", tailer->dispatch_after, cycle, seqnum);
    printf("    state            %d - %s\n", tailer->state, state_text);
    printf("    qf_fn            %s\n",   tailer->qf_fn);
    printf("    qf_fd            %d\n",   tailer->qf_fd);
    printf("    qf_statbuf_sz    %" PRIu64 "\n", (uint64_t)tailer->qf_statbuf.st_size);
    printf("    qf_tip           %" PRIu64 "\n", tailer->qf_tip);
    cycle = tailer->qf_index >> queue->cycle_shift;
    seqnum = tailer->qf_index & queue->seqnum_mask;
    printf("    qf_index         %" PRIu64 " (cycle %u, seqnum %u)\n", tailer->qf_index, cycle, seqnum);
    printf("    qf_buf           %p\n",   tailer->qf_buf);
    printf("      extent         %p\n",   tailer->qf_buf+tailer->qf_mmapsz);
    printf("    qf_mmapsz        %" PRIx64 "\n", tailer->qf_mmapsz);
    printf("    qf_mmapoff       %" PRIx64 "\n", tailer->qf_mmapoff);
}

K shmipc_append(K dir, K msg) {
    return shmipc_append_ts(dir,msg,NULL);
}

K shmipc_append_ts(K dir, K msg, K ms) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");
    if (ms != NULL && ms->t != -KJ) return krr("ms NULL or J milliseconds");

    // Appending logic
    // 0) catch up to the end of the current file.
    //   if hit EOF then we need to wait for creation of next file, poll modcount
    //   then try opening filehandle
    // Else we may be only writer
    // a) determine if we need to roll to a new file - other writers may be idle
    //     call gtod and calculate current cycle
    //
    //    - CAS loop to write EOF marker
    //     create new queue file
    //     update maxcycle and then increment modcount
    // or
    // b) write to current file
    //     Is this entry indexable?
    //       Is the index currently full
    //     Write Entry
    //        if mod % index, write back to index
    //        if index full, write new index page, write back to index2index
    //        when update index page, then write data
    //   Before write, CAS operation to put working indicator on last entry
    //    if fail, loop waiting for unlock. If EOF then fail is broken, retry
    //    if data or index, skip over them and attempt again on newest page

    // check if queue already open
    queue_t *queue = queue_head;
    while (queue != NULL) {
        if (queue->hsymbolp == dir->s) break;
        queue = queue->next;
    }
    if (queue == NULL) return krr("dir must be shmipc.init[] first");

    // caution: encodecheck may tweak blocksize, do not redorder below shmipc_peek_tailer
    msg = queue->encodecheck(queue, msg); // abort write if r==0
    if (msg == NULL) return msg;

    // refresh highest and lowest, allowing our appender to follow another appender
    peek_queue_modcount(queue);

    // build a special tailer with the protection bits and file descriptor set to allow
    // writing.
    if (queue->appender == NULL) {
        tailer_t* tailer = malloc(sizeof(tailer_t));
        if (tailer == NULL) return krr("am fail");
        bzero(tailer, sizeof(tailer_t));

        // compat: writers do an extended lookback to patch missing EOFs
        tailer->qf_index = (queue->highest_cycle - patch_cycles) << queue->cycle_shift;
        tailer->callback = NULL;
        tailer->state = 5;
        tailer->mmap_protection = PROT_READ | PROT_WRITE;

        queue->appender = tailer;

        // re-open directory-listing mapping in read-write mode
        K x = ee(directory_listing_reopen(queue, O_RDWR, PROT_READ | PROT_WRITE));
        if (x && x->t == KERR && x->s) {
            printf("shmipc: rw dir listing %p %s\n", x, x->s); return krr(x->s);
        }
        if (debug) printf("shmipc: appender created\n");
    }
    tailer_t* appender = queue->appender;


    // if given a clock, use this to switch cycle
    // which may be higher that maxCycle, in which case we need to poke the directory-listing
    // TODO: move inside write loop to trigger EOF
    if (ms) {
        uint64_t cyc = (ms->j - queue->roll_epoch) / queue->roll_length;
        if (cyc != appender->qf_index >> queue->cycle_shift) {
            printf("shmipc: appender setting cycle from timestamp: current %" PRIu64 " proposed %" PRIu64 "\n", appender->qf_index >> queue->cycle_shift, cyc);
            appender->qf_index = cyc << queue->cycle_shift;
        }
    }

    // poll the appender
    while (1) {
        int r = shmipc_peek_tailer(queue, appender);
        // TODO: 2nd call defensive to ensure 1 whole blocksize is available to put
        r = shmipc_peek_tailer(queue, appender);
        if (debug) printf("shmipc: writeloop appender in state %d\n", r);

        if (r == 2) {
            // our cycle is pointing to a queuefile that does not exist
            // as we are writer, create it with temporary filename, atomically
            // move it to the desired name, then bump the global highest_cycle
            // value if rename succeeded
            char* fn_buf;
            asprintf(&fn_buf, "%s.%d.tmp", appender->qf_fn, pid_header);

            // if queuefile_init fails via. krr, re-throw the error and abort the write
            K x = ee(queuefile_init(fn_buf, queue));
            if (x && x->t == KERR && x->s) {
                printf("shmipc: queuefile_init error %p %s\n", x, x->s); return krr(x->s);
            }

            if (rename(fn_buf, appender->qf_fn) != 0) {
                // rename failed, maybe raced with another writer, delay and try again
                printf("shmipc: create queuefile %s failed at rename, errno %d\n", fn_buf, errno);
                sleep(1);
                continue;
            }
            printf("renamed %s to %s\n", fn_buf, appender->qf_fn);
            free(fn_buf);

            // rename worked, we can now re-try the peek_tailer
            continue;
        }

        if (r == 6) {
            // current queuefile has less than two blocks remaining, needs extending
            // should the extend fail, we are having disk issues, wait until fixed
            uint64_t extend_to = appender->qf_statbuf.st_size + qf_disk_sz;
            if (lseek(appender->qf_fd, extend_to - 1, SEEK_SET) == -1) {
                printf("shmmain: extend queuefile %s failed at lseek: %s\n", appender->qf_fn, strerror(errno));
                sleep(1);
                continue;
            }
            if (write(appender->qf_fd, "", 1) != 1) {
                printf("shmmain: extend queuefile %s failed at write: %s\n", appender->qf_fn, strerror(errno));
                sleep(1);
                continue;
            }
            printf("shmmain: extended queuefile %s to %" PRIu64 " bytes\n", appender->qf_fn, extend_to);
            continue;
        }

        // If the tailer returns 0, we are all set pointing to the next unwritten entry.
        // if we write to qf_buf and the state is not zero we'll hit sigbus etc, so sleep
        // and wait for availability.
        if (r != 0) {
            printf("shmipc: Cannot write in state %d, sleeping\n", r);
            sleep(1);
            continue;
        }

        // TODO: encoder
        uint32_t msz = msg->n;
        if (msz > HD_MASK_META) return krr("`shm msg sz > 30bit");
        if ((appender->qf_tip - appender->qf_mmapoff) + msz > appender->qf_mmapsz) {
            printf("aborting on bug: write would segfault buffer!\n");
            abort();
        }

        // Since appender->buf is pointing at the queue head, so we can
        // LOCK CMPXCHG the working bit directly. If the cas failed, another writer
        // has beaten us to it, we sleep poll the tailer and try again
        // If the file has gone EOF, we re-visit the tailer logic which will adjust
        // the maps and switch to the new file.

        // Note that we do not extended qf_buf or qf_index after the write. Let the
        // tailer log handle the entry we've just written in the normal way, since that will
        // adjust the buffer window/mmap for us.
        unsigned char* ptr = (appender->qf_tip - appender->qf_mmapoff) + appender->qf_buf;
        uint32_t ret = lock_cmpxchgl(ptr, HD_UNALLOCATED, HD_WORKING);

        // cmpxchg returns the original value in memory, so we can tell if we succeeded
        // by looking for HD_UNALLOCATED. If we read a working bit or finished size, we lost.
        if (ret == HD_UNALLOCATED) {
            asm volatile ("mfence" ::: "memory");

            // Java does not patch EOF on prev files if it is down during the roll, it
            // just starts a new file. As a workaround their readers 'timeout' the wait for EOF
            // if a higher cycle is known for the queue. I'd like to be as correct as possible, so
            // we'll patch missing EOFs during our writes if we hold the lock. This will nudge on any
            // readers who haven't noticed the roll.
            if (appender->qf_index < queue->highest_cycle << queue->cycle_shift) {
                printf("shmipc: got write lock, but about to write to queuefile < maxcycle, writing EOF.");
                uint32_t header = HD_EOF;
                memcpy(ptr, &header, sizeof(header));
                continue; // retry write in next queuefile
            }

            // TODO - use encoder
            memcpy(ptr+4, (char*)msg->G0, msz);

            asm volatile ("mfence" ::: "memory");
            uint32_t header = msz & HD_MASK_LENGTH;
            memcpy(ptr, &header, sizeof(header));

            break;
        }

        printf("shmipc: write lock failed, peeking again\n");
        sleep(1);
    }

    // if we've rolled a new file,inform listeners by bumping modcount
    uint64_t cyc = appender->qf_index >> queue->cycle_shift;
    if (cyc > queue->highest_cycle) {
        queue->highest_cycle = cyc;
        poke_queue_modcount(queue);
    }

    if (debug) printf("shmipc: wrote %lld bytes as index %" PRIu64 "\n", msg->n, appender->qf_index);

    return kj(appender->qf_index);
}

K shmipc_tailer(K dir, K cb, K kindex) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");
    if (cb->t != 100) return krr("cb is not function");
    if (kindex->t != -KJ) return krr("index must be J");

    // check if queue already open
    queue_t *queue = queue_head;
    while (queue != NULL) {
        if (queue->hsymbolp == dir->s) break;
        queue = queue->next;
    }
    if (queue == NULL) return krr("dir must be shmipc.init[] first");

    // decompose index into cycle (file) and seqnum within file
    uint64_t index = kindex->j;
    int cycle = index >> queue->cycle_shift;
    int seqnum = index & queue->seqnum_mask;

    printf("shmipc: tailer added index=%" PRIu64 " (cycle=%d seqnum=%d)\n", index, cycle, seqnum);
    if (cycle < queue->lowest_cycle) {
        index = queue->lowest_cycle << queue->cycle_shift;
    }
    if (cycle > queue->highest_cycle) {
        index = queue->highest_cycle << queue->cycle_shift;
    }

    // allocate struct, we'll link if all checks pass
    tailer_t* tailer = malloc(sizeof(tailer_t));
    if (tailer == NULL) return krr("tm fail");
    bzero(tailer, sizeof(tailer_t));

    tailer->dispatch_after = index - 1;
    tailer->qf_index = index & ~queue->seqnum_mask; // start replay from first entry in file
    tailer->callback = cb;
    tailer->state = 5;
    tailer->mmap_protection = PROT_READ;

    tailer->next = queue->tailers; // linked list
    queue->tailers = tailer;

    tailer->queue = queue; // parent pointer

    // maintain array for index lookup
    tailer->handle = tailer_handles_count;
    tailer_handles = realloc(tailer_handles, ++tailer_handles_count * sizeof(tailer_t*));
    tailer_handles[tailer->handle] = tailer;
    return ki(tailer->handle);
}

K shmipc_collect(K idx) {
    if (idx->t != -KI) return krr("idx is not int");
    if (idx->i < 0 || idx->i >= tailer_handles_count) return krr("idx out of range");
    tailer_t* tailer = tailer_handles[idx->i];
    tailer->collect = 1;

    uint64_t delaycount = 0;
    peek_queue_modcount(tailer->queue);
    while (1) {
        int r = shmipc_peek_tailer(tailer->queue, tailer);
        if (debug) printf("collect value returns %d and object %p\n", r, tailer->collected_value);
        if (r == 7) {
            break;
        }
        if (delaycount++ >> 20) usleep(delaycount >> 20);
    }
    K x = tailer->collected_value;
    tailer->collected_value = NULL;
    return x;
}

void tailer_close(tailer_t* tailer) {
    if (tailer->qf_fn) { // if next filename cached...
        free(tailer->qf_fn);
    }
    if (tailer->qf_buf) { // if mmap() open...
        munmap(tailer->qf_buf, tailer->qf_mmapsz);
    }
    if (tailer->qf_fd) { // if open() open...
        close(tailer->qf_fd);
    }
    free(tailer);
}

K shmipc_close(K dir) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");

    // check if queue already open
    queue_t **parent = &queue_head; // pointer to a queue_t pointer
    queue_t *queue = queue_head;

    while (queue != NULL) {
        if (queue->hsymbolp == dir->s) {
            *parent = queue->next; // unlink

            // delete tailers
            tailer_t *tailer = queue->tailers; // shortcut to save both collections
            while (tailer != NULL) {
                tailer_t* next_tmp = tailer->next;
                tailer_close(tailer);
                tailer = next_tmp;
            }
            queue->tailers = NULL;

            if (queue->appender) tailer_close(queue->appender);

            // kill queue
            munmap(queue->dirlist, queue->dirlist_statbuf.st_size);
            close(queue->dirlist_fd);
            free(queue->dirlist_name);
            free(queue->dirname);
            free(queue->queuefile_pattern);
            free(queue->roll_format);
            globfree(&queue->queuefile_glob);
            free(queue);

            return (K)NULL;
        }
        parent = &queue->next;
        queue = queue->next;
    }
    return krr("shmipc: close: queue does not exist");
}

K queuefile_init(char* fn, queue_t* queue) {
    int fd;
    int mode = 0777;

    printf("Creating %s\n", fn);

    // open/create the output file
    if ((fd = open(fn, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0) {
        printf("can't create %s for writing", fn);
        return krr("shmipc: create tmp queuefile err");
    }

    // go to the location corresponding to the last byte
    if (lseek(fd, qf_disk_sz - 1, SEEK_SET) == -1) {
        return krr("shmipc: lseek error");
    }

    // write a dummy byte at the last location
    if (write(fd, "", 1) != 1) {
        return krr("shmipc: write error");
    }

    // TODO: write header
    // TODO: write index2index
    printf("Created %s\n", fn);

    close(fd);
    return NULL;
}

K directory_listing_reopen(queue_t* queue, int open_flags, int mmap_prot) {

    if ((queue->dirlist_fd = open(queue->dirlist_name, open_flags)) < 0) {
        return orr("dirlist open");
    }

    // find size of dirlist and mmap
    if (fstat(queue->dirlist_fd, &queue->dirlist_statbuf) < 0)
        return krr("dirlist fstat");
    if ((queue->dirlist = mmap(0, queue->dirlist_statbuf.st_size, mmap_prot, MAP_SHARED, queue->dirlist_fd, 0)) == MAP_FAILED)
        return krr("dirlist mmap fail");

    if (debug) printf("shmipc: parsing dirlist\n");
    parse_dirlist(queue);

    // check the polled fields in header section were all resolved to pointers within the map
    if (queue->dirlist_fields.highest_cycle == NULL || queue->dirlist_fields.lowest_cycle == NULL ||
        queue->dirlist_fields.modcount == NULL) {
        return krr("dirlist parse hdr ptr fail");
    }
    return (K)NULL;
}
