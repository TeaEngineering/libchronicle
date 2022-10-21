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
#include <sys/time.h>
#include <time.h>
#include <libchronicle.h>

#include "wire.h"
#include "buffer.h"

/**
 * Implementation notes
 * This code is not reentrant, nor does it create any threads, so this is not a problem.
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

// error handling
const char* cerr_msg;
// error handling - trigger
int chronicle_err(const char* msg) {
    printf("chronicle error: '%s\n", msg);
    errno = -1;
    cerr_msg = msg;
    return -1;
}
void* chronicle_perr(const char* msg) {
    chronicle_err(msg);
    return (void*)NULL;
}
// error handling - retrieve
const char* chronicle_strerror() {
    return cerr_msg;
}

const char* tailer_state_messages[] = {"AWAITING_ENTRY", "BUSY", "AWAITING_QUEUEFILE", "E_STAT", "E_MMAP", "PEEK?", "EXTEND_FAIL", "COLLECTED"};

// structures

typedef struct {
    unsigned char *highest_cycle;
    unsigned char *lowest_cycle;
    unsigned char *modcount;
} dirlist_fields_t;

typedef struct tailer {
    uint64_t          dispatch_after; // for resume support
    tailstate_t       state;
    cdispatch_f       dispatcher;
    void*             dispatch_ctx;
    // to support the 'collect' operation to wait and return next item, ignoring callback
    collected_t*      collect;

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

    struct tailer*    next;
    struct tailer*    prev;
} tailer_t;

typedef struct queue {
    char*             dirname;
    uint              blocksize;
    uint8_t           version;
    uint8_t           create;

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

    // roll config populated from (any) queuefile header or set on creation
    int               roll_length;
    int               roll_epoch;
    char*             roll_format;
    char*             roll_name;
    char*             roll_strftime;
    int               index_count;
    int               index_spacing;

    int               cycle_shift;
    uint64_t          seqnum_mask;

    char* (*cycle2file_fn)(struct queue*,int);

    cparse_f          parser;
    csizeof_f         append_sizeof;
    cappend_f         append_write;

    tailer_t*         tailers;

    // the appender is a shared tailer, polled by append[], with writing logic
    // and no callback to user code for events
    tailer_t*         appender;

    struct queue*     next;
} queue_t;


typedef enum {QB_AWAITING_ENTRY, QB_BUSY, QB_REACHED_EOF, QB_NEED_EXTEND, QB_NULL_ITEM, QB_COLLECTED} parseqb_state_t;

typedef parseqb_state_t (*datacallback_f)(unsigned char*,int,uint64_t,void* userdata);


// paramaters that control behavior, not exposed for modification
uint32_t patch_cycles = 3;
long int qf_disk_sz = 83754496L;

// globals
int debug = 0;
uint32_t pid_header = 0;
queue_t* queue_head = NULL;


// forward declarations
void parse_dirlist(queue_t*);
void parse_queuefile_meta(unsigned char*, int, queue_t*);
void parse_queuefile_data(unsigned char*, int, queue_t*, tailer_t*, uint64_t);
int queuefile_init(char*, queue_t*);
int directory_listing_reopen(queue_t*, int, int);
int directory_listing_init(queue_t*, uint64_t cycle);
long chronicle_clock_ms(queue_t*);
uint64_t chronicle_cycle_from_ms(queue_t*, long);

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

char* chronicle_get_cycle_fn(queue_t* queue, int cycle) {
    // TODO: replace with https://ideone.com/7BADb as gmtime_r leaks
    // time_t aka long. seconds since midnight 1970
    time_t rawtime = cycle * (queue->roll_length / 1000);
    struct tm info;
    gmtime_r(&rawtime, &info);

    // format datetime component using prebuilt pattern
    char* strftime_buf = strdup(queue->roll_format);
    strftime(strftime_buf, strlen(strftime_buf)+1, queue->roll_strftime, &info);

    // join with dirname and file suffix
    char* fnbuf;
    int bufsz = asprintf(&fnbuf, "%s/%s.cq4", queue->dirname, strftime_buf);
    free(strftime_buf);
    if (bufsz < 0) return NULL;
    return fnbuf;
}

void queue_double_blocksize(queue_t* queue) {
    uint new_blocksize = queue->blocksize << 1;
    printf("shmipc:  doubling blocksize from %x to %x\n", queue->blocksize, new_blocksize);
    queue->blocksize = new_blocksize;
}


queue_t* chronicle_init(char* dir) {
    char* debug_env = getenv("SHMIPC_DEBUG");
    debug = (debug_env == NULL) ? 0 : strcmp(debug_env, "1") == 0;
    char* wiretrace_env = getenv("SHMIPC_WIRETRACE");
    wire_trace = (wiretrace_env == NULL) ? 0 : strcmp(wiretrace_env, "1") == 0;

    pid_header = (getpid() & HD_MASK_LENGTH);

    // allocate struct, we'll link if all checks pass
    queue_t* queue = malloc(sizeof(queue_t));
    if (queue == NULL) return chronicle_perr("m fail");
    bzero(queue, sizeof(queue_t));

    // wire up default 'text' parsers for data segments
    queue->parser = &chronicle_decoder_default_parse;
    queue->append_sizeof = &chronicle_encoder_default_sizeof;
    queue->append_write = &chronicle_encoder_default_write;

    // unsafe to use ref here in case caller doesn't keep in scope, so dup
    queue->dirname = strdup(dir);
    queue->blocksize = 1024*1024; // must be a power of two (single 1 bit)
    queue->roll_epoch = -1;

    // Good to use
    queue->next = queue_head;
    queue_head = queue;

    return queue;
}

int chronicle_readable(char* dirname, char* suffix) {
    char* dirlist_name;
    int dirlist_fd;

    // probe v4 directory-listing.cq4t
    asprintf(&dirlist_name, "%s/%s", dirname, suffix);
    if ((dirlist_fd = open(dirlist_name, O_RDONLY)) > 0) {
        close(dirlist_fd);
        free(dirlist_name);
        return 1;
    }
    free(dirlist_name);
    return 0;
}

int chronicle_version_detect(queue_t* queue) {
    if (chronicle_readable(queue->dirname, "directory-listing.cq4t")) return 4;
    if (chronicle_readable(queue->dirname, "metadata.cq4t")) return 5;
    return 0;
}

int chronicle_open(queue_t* queue) {

    if (debug) printf("shmipc: opening dir %s\n", queue->dirname);

    // Is this a directory
    struct stat statbuf;
    if (stat(queue->dirname, &statbuf) != 0) return chronicle_err("dir stat fail");
    if (!S_ISDIR(statbuf.st_mode)) return chronicle_err("dir is not a directory");

    // autodetect version
    int auto_version = chronicle_version_detect(queue);
    printf("chronicle: detected version v%d\n", auto_version);

    // Does queue dir contain some .cq4 files?
    // for V5 it is OK to have empty directory
    glob_t *g = &queue->queuefile_glob;
    g->gl_offs = 0;

    asprintf(&queue->queuefile_pattern, "%s/*.cq4", queue->dirname);
    glob(queue->queuefile_pattern, GLOB_ERR, NULL, g);
    if (debug) {
        printf("shmipc: glob %zu queue files found\n", g->gl_pathc);
        for (int i = 0; i < g->gl_pathc;i++) {
            printf("   %s\n", g->gl_pathv[i]);
        }
    }

    if (auto_version == 0) {
        if (queue->create == 0) return chronicle_err("queue should exist (no permission to create), but version detect failed");
        if (queue->version == 0) return chronicle_err("queue create requires chronicle_set_version()");
        if (g->gl_pathc != 0) return chronicle_err("queue create requires empty destination directory");
        if (queue->roll_name == NULL) return chronicle_err("queue create requires chronicle_set_roll_scheme()");

    } else if (queue->version != 0 && queue->version != auto_version) {
        return chronicle_err("queue version detected does not match expected set via. chronicle_set_version()");
    } else {
        queue->version = auto_version;
    }

    // populate dirlist
    asprintf(&queue->dirlist_name, queue->version == 4 ? "%s/directory-listing.cq4t" : "%s/metadata.cq4t", queue->dirname);
    if (queue->create && auto_version == 0) {
        uint64_t cycle = chronicle_cycle_from_ms(queue, chronicle_clock_ms(queue));
        int rc = directory_listing_init(queue, cycle);
        if(rc != 0) return rc;
    }

    // parse the directory listing and sanity-check all required fields exist
    int rc = directory_listing_reopen(queue, O_RDONLY, PROT_READ);
    if(rc != 0) return rc;

    if (queue->version == 4) {
        // For v4, we need to read a 'queue' header from any one of the datafiles to get the
        // rollover configuration. We don't know how to generate a filename from a cycle code
        // yet, so this needs to use the directory listing.

        if (g->gl_pathc < 1) {
            return chronicle_err("V4 and no queue files found so cannot initialise. Call chronicle_set_create(queue, 1) to allow queue creation");
        }

        int               queuefile_fd;
        struct stat       queuefile_statbuf;
        uint64_t          queuefile_extent;
        unsigned char*    queuefile_buf;

        char* fn = queue->queuefile_glob.gl_pathv[0];
        // find length of queuefile and mmap
        if ((queuefile_fd = open(fn, O_RDONLY)) < 0) {
            return chronicle_err("qfi open");
        }
        if (fstat(queuefile_fd, &queuefile_statbuf) < 0)
            return chronicle_err("qfi fstat");

        // only need the first block
        queuefile_extent = queuefile_statbuf.st_size < queue->blocksize ? queuefile_statbuf.st_size : queue->blocksize;

        if ((queuefile_buf = mmap(0, queuefile_extent, PROT_READ, MAP_SHARED, queuefile_fd, 0)) == MAP_FAILED)
            return chronicle_err("qfi mmap fail");

        // we don't need a data-parser at this stage as only need values from the header
        if (debug) printf("shmipc: parsing queuefile %s 0..%" PRIu64 "\n", fn, queuefile_extent);
        parse_queuefile_meta(queuefile_buf, queuefile_extent, queue);

        // close queuefile
        munmap(queuefile_buf, queuefile_extent);
        close(queuefile_fd);
    }

    // check we loaded roll settings from queuefile or metadata
    if (queue->version == 0) return chronicle_err("qfi version detect fail");
    if (queue->roll_format == 0) return chronicle_err("qfi roll_format fail");
    if (queue->roll_length == 0) return chronicle_err("qfi roll_length fail");
    if (queue->roll_epoch == -1) return chronicle_err("qfi roll_epoch fail");

    char* roll_format_auto = queue->roll_format;
    // defer this until queuefile access
    chronicle_set_roll_dateformat(queue, roll_format_auto);
    free(roll_format_auto);

    //if (queue->index_count == 0) return chronicle_err("qfi index_count fail");
    //if (queue->index_spacing == 0) return chronicle_err("qfi index_spacing fail");
    if (queue->roll_name == NULL) return chronicle_err("qfi roll scheme unknown");

    queue->cycle2file_fn = &chronicle_get_cycle_fn;
    queue->cycle_shift = 32;
    queue->seqnum_mask = 0x00000000FFFFFFFF;

    // TODO: Logic from RollCycles.java ensures rollover occurs before we run out of index2index pages?
    //  cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));

    // avoids a tailer registration before we have a minimum cycle
    chronicle_peek_queue(queue);
    if (debug) printf("shmipc: chronicle_open() OK\n");

    return 0;
}

void chronicle_set_decoder(queue_t *queue, cparse_f parser) {
    if (debug & !parser) printf("chronicle: setting NULL parser");
    queue->parser = parser;
}

void chronicle_set_encoder(queue_t *queue, csizeof_f append_sizeof, cappend_f append_write) {
    if (debug & !append_sizeof) printf("chronicle: setting NULL append_sizeof");
    if (debug & !append_write) printf("chronicle: setting NULL append_write");
    queue->append_sizeof = append_sizeof;
    queue->append_write = append_write;
}

struct ROLL_SCHEME chronicle_roll_schemes[] = {
    // in use by cq5
    {"FIVE_MINUTELY",        "yyyyMMdd-HHmm'V'",        5*60,  2<<10,   256},
    {"TEN_MINUTELY",         "yyyyMMdd-HHmm'X'",       10*60,  2<<10,   256},
    {"TWENTY_MINUTELY",      "yyyyMMdd-HHmm'XX'",      20*60,  2<<10,   256},
    {"HALF_HOURLY",          "yyyyMMdd-HHmm'H'",       30*60,  2<<10,   256},
    {"FAST_HOURLY",          "yyyyMMdd-HH'F'",         60*60,  4<<10,   256},
    {"TWO_HOURLY",           "yyyyMMdd-HH'II'",      2*60*60,  4<<10,   256},
    {"FOUR_HOURLY",          "yyyyMMdd-HH'IV'",      4*60*60,  4<<10,   256},
    {"SIX_HOURLY",           "yyyyMMdd-HH'VI'",      6*60*60,  4<<10,   256},
    {"FAST_DAILY",           "yyyyMMdd'F'",         24*60*60,  4<<10,   256},
    // used historically by cq4
    {"MINUTELY",             "yyyyMMdd-HHmm",             60,  2<<10,    16},
    {"HOURLY",               "yyyyMMdd-HH",            60*60,  4<<10,    16},
    {"DAILY",                "yyyyMMdd",            24*60*60,  8<<10,    64},
    // minimal rolls with resulting large queue files
    {"LARGE_HOURLY",         "yyyyMMdd-HH'L'",         60*60,  8<<10,    64},
    {"LARGE_DAILY",          "yyyyMMdd'L'",         24*60*60, 32<<10,   128},
    {"XLARGE_DAILY",         "yyyyMMdd'X'",         24*60*60, 32<<10,   256},
    {"HUGE_DAILY",           "yyyyMMdd'H'",         24*60*60, 32<<10,  1024},
    // for tests and benchmarking with nearly no indexing
    {"SMALL_DAILY",          "yyyyMMdd'S'",         24*60*60,  8<<10,     8},
    {"LARGE_HOURLY_SPARSE",  "yyyyMMdd-HH'LS'",        60*60,  4<<10,  1024},
    {"LARGE_HOURLY_XSPARSE", "yyyyMMdd-HH'LX'",        60*60,  2<<10, 1<<20},
    {"HUGE_DAILY_XSPARSE",   "yyyyMMdd'HX'",        24*60*60, 16<<10, 1<<20},
    // for tests to create smaller queue files
    {"TEST_SECONDLY",        "yyyyMMdd-HHmmss'T'",         1, 32<<10,     4},
    {"TEST4_SECONDLY",       "yyyyMMdd-HHmmss'T4'",        1,     32,     4},
    {"TEST_HOURLY",          "yyyyMMdd-HH'T'",         60*60,     16,     4},
    {"TEST_DAILY",           "yyyyMMdd'T1'",        24*60*60,      8,     1},
    {"TEST2_DAILY",          "yyyyMMdd'T2'",        24*60*60,     16,     2},
    {"TEST4_DAILY",          "yyyyMMdd'T4'",        24*60*60,     32,     4},
    {"TEST8_DAILY",          "yyyyMMdd'T8'",        24*60*60,    128,     8},
};

void chronicle_apply_roll_scheme(queue_t* queue, struct ROLL_SCHEME x) {
    if (debug) printf("chronicle: chronicle_set_roll_scheme applying %s\n", x.name);

    queue->roll_name   = x.name;
    queue->roll_format = x.formatstr;
    queue->roll_length = x.roll_length_secs * 1000;

    // remove appostrophe from java's format string and build
    // the equivelent strftime string together
    char* p = strdup(queue->roll_format);
    int px = 0;
    char* f = queue->roll_format;
    int fi = 0;
    int inquote = 0;
    while (fi < strlen(queue->roll_format)) {
        if (debug) {
            printf(" rs parser fi=%d px=%d inquote=%d buffer='%s'\n", fi, px, inquote, p);
        }
        if (inquote == 1 && f[fi] != '\'') {
            // copy quoted literal
            p[px++] = f[fi++];
        } else if (f[fi] == '-') {
            // copy literal dash
            p[px++] = f[fi++];
        } else if (f[fi] == '\'') {
            // ignore quotes, toggle flag
            inquote = (inquote + 1) % 2;
            fi++;
        } else if (strncmp(&f[fi], "yyyy", 4) == 0) {
            p[px++] = '%';
            p[px++] = 'Y';
            fi += 4;
        } else if (strncmp(&f[fi], "MM", 2) == 0) {
            p[px++] = '%';
            p[px++] = 'm';
            fi += 2;
        } else if (strncmp(&f[fi], "dd", 2) == 0) {
            p[px++] = '%';
            p[px++] = 'd';
            fi += 2;
        } else if (strncmp(&f[fi], "HH", 2) == 0) {
            p[px++] = '%';
            p[px++] = 'H';
            fi += 2;
        } else if (strncmp(&f[fi], "mm", 2) == 0) {
            p[px++] = '%';
            p[px++] = 'M';
            fi += 2;
        } else {
            printf("chronicle: parser conversion of %s exploded at fi=%d px=%d inquote=%d buffer='%s'\n", queue->roll_format, fi, px, inquote, p);
            return;
        }
    }
    p[px++] = 0;
    if (debug) {
        printf(" rs parser result='%s'\n", p);
    }
    queue->roll_strftime = p;
}

int chronicle_set_roll_scheme(queue_t* queue, char* scheme) {
    queue->roll_name   = NULL;
    queue->roll_format = NULL;
    for (int i = 0; i < sizeof(chronicle_roll_schemes)/sizeof(chronicle_roll_schemes[0]); i++) {
        struct ROLL_SCHEME x = chronicle_roll_schemes[i];
        if (strcmp(scheme, x.name) == 0) {
            chronicle_apply_roll_scheme(queue, x);
        }
    }
    return 0;
}

char* chronicle_get_roll_scheme(queue_t* queue) {
    return queue->roll_name;
}

char* chronicle_get_roll_format(queue_t* queue) {
    return queue->roll_format;
}

int chronicle_set_roll_dateformat(queue_t* queue, char* dateformat) {
    queue->roll_name   = NULL;
    queue->roll_format = NULL;
    for (int i = 0; i < sizeof(chronicle_roll_schemes)/sizeof(chronicle_roll_schemes[0]); i++) {
        struct ROLL_SCHEME x = chronicle_roll_schemes[i];
        if (strcmp(dateformat, x.formatstr) == 0) {
            chronicle_apply_roll_scheme(queue, x);
        }
    }
    return 0;
}

void chronicle_set_create(queue_t* queue, int create) {
    queue->create = create;
}

void chronicle_set_version(queue_t* queue, int version) {
    if (version == 4) {
        queue->version = 4;
    } else if (version == 5) {
        queue->version = 5;
    }
}

int chronicle_get_version(queue_t* queue) {
    return queue->version;
}

long chronicle_clock_ms(queue_t* queue) {
    // switch to custom clock etc.
    // default case
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000 ;
}

uint64_t chronicle_cycle_from_ms(queue_t* queue, long ms) {
    return (ms - queue->roll_epoch) / queue->roll_length;
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
// typedef enum {QB_AWAITING_ENTRY, QB_BUSY, QB_REACHED_EOF, QB_NEED_EXTEND, QB_NULL_ITEM, QB_COLLECTED} parseqb_state_t;

parseqb_state_t parse_queue_block(queue_t *queue, unsigned char** basep, uint64_t *indexp, unsigned char* extent, wirecallbacks_t* hcbs, datacallback_f parse_data, void* userdata) {
    uint32_t header;
    int sz;
    unsigned char* base = *basep;
    uint64_t index = *indexp;
    parseqb_state_t pd = QB_AWAITING_ENTRY;
    while (pd == QB_AWAITING_ENTRY) {
        if (base+4 >= extent) return 3;
        memcpy(&header, base, sizeof(header)); // relax, fn optimised away
        // no speculative fetches before the header is read
        asm volatile ("mfence" ::: "memory");

        if (header == HD_UNALLOCATED) {
            if (debug) printf(" %" PRIu64 " @%p unallocated\n", index, base);
            return QB_AWAITING_ENTRY;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            if (debug) printf(" @%p locked for writing by pid %d\n", base, header & HD_MASK_LENGTH);
            return QB_BUSY;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            if (debug) printf(" @%p metadata size %x\n", base, sz);
            if (base+4+sz >= extent) return QB_NEED_EXTEND;
            wire_parse(base+4, sz, hcbs);
            // EventName  header
            //   switch to header parser
        } else if ((header & HD_MASK_META) == HD_EOF) {
            if (debug) printf(" @%p EOF\n", base);
            return QB_REACHED_EOF;
        } else {
            sz = (header & HD_MASK_LENGTH);
            if (debug) printf(" %" PRIu64 " @%p data size %x\n", index, base, sz);
            if (parse_data) {
                if (base+4+sz >= extent) return QB_NEED_EXTEND;
                pd = parse_data(base+4, sz, index, userdata);
            } else {
                // bail at first data message
                return QB_NULL_ITEM;
            }
            index++;
            *indexp = index;
        }
        int pad4 = (queue->version < 5) ? 0 : -sz & 0x03;
        base = base + 4 + sz + pad4;
        *basep = base;
    }
    return pd;
}

// parse data callback dispatching to wire.h parser
parseqb_state_t parse_wire_data(unsigned char* base, int lim, uint64_t index, void* cbs) {
    wire_parse(base, lim, (wirecallbacks_t*)cbs);
    return QB_AWAITING_ENTRY;
}


// return AWAITING_ENTRY to continue dispaching, COLLECTED to signal collected item
parseqb_state_t parse_data_cb(unsigned char* base, int lim, uint64_t index, void* userdata) {
    tailer_t* tailer = (tailer_t*)userdata;
    if (debug) printbuf((char*)base, lim);
    // prep args and fire callback
    if (index > tailer->dispatch_after) {
        COBJ msg = tailer->queue->parser(base, lim);
        if (debug && msg==NULL) printf("chronicle: caution at index %" PRIu64 " parse function returned NULL, skipping\n", index);

        // if asked to return inline, we skip dispatcher callback
        if (tailer->collect) {
            tailer->collect->msg = msg;
            tailer->collect->index = index;
            tailer->collect->sz = lim;
            return QB_COLLECTED;
        }
        if (tailer->dispatcher) {
            return tailer->dispatcher(tailer->dispatch_ctx, index, msg);
        }
    }
    return QB_AWAITING_ENTRY;
}


void handle_dirlist_ptr(char* buf, int sz, unsigned char *dptr, wirecallbacks_t* cbs) {
    // we are preserving *pointers* within the shared directory data page
    // we keep the underlying mmap for life of queue
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "listing.highestCycle", sz) == 0) {
        queue->dirlist_fields.highest_cycle = dptr;
    } else if (strncmp(buf, "listing.lowestCycle", sz) == 0) {
        queue->dirlist_fields.lowest_cycle = dptr;
    } else if (strncmp(buf, "listing.modCount", sz) == 0) {
        queue->dirlist_fields.modcount = dptr;
    }
}

void handle_dirlist_uint32(char* buf, int sz, uint32_t data, wirecallbacks_t* cbs){
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "length", sz) == 0) {
        if (debug) printf("  v5 roll_length set to %x\n", data);
        queue->roll_length = data;
    }
}

void handle_dirlist_uint8(char* buf, int sz, uint8_t data, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "epoch", sz) == 0) {
        if (debug) printf("  v5 roll_epoch set to %x\n", data);
        queue->roll_epoch = data;
    }
}

void handle_dirlist_text(char* buf, int sz, char* data, int dsz, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "format", sz) == 0) {
        if (debug) printf("  v5 roll_format set to %.*s\n", dsz, data);
        queue->roll_format = strndup(data, dsz);
    }
}

void handle_qf_uint32(char* buf, int sz, uint32_t data, wirecallbacks_t* cbs){
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "length", sz) == 0) {
        if (debug) printf(" v4 roll_length set to %x\n", data);
        queue->roll_length = data;
    }
}

void handle_qf_uint16(char* buf, int sz, uint16_t data, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexCount", sz) == 0) {
        queue->index_count = data;
    } else if (strncmp(buf, "indexSpacing", sz) == 0) {
        queue->index_spacing = data;
    }
}

void handle_qf_uint8(char* buf, int sz, uint8_t data, wirecallbacks_t* cbs) {
    queue_t* queue = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexSpacing", sz) == 0) {
        queue->index_spacing = data;
    } else if (strncmp(buf, "epoch", sz) == 0) {
        if (debug) printf(" v4 roll_epoch set to %x\n", data);
        queue->roll_epoch = data;
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
    // used to dump out the test data for test_wire.c
    // printbuf((char*)base, lim);

    wirecallbacks_t cbs;
    bzero(&cbs, sizeof(cbs));
    cbs.ptr_uint64 = &handle_dirlist_ptr;
    cbs.userdata = queue;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    hcbs.field_uint32 = &handle_dirlist_uint32;
    hcbs.field_uint8 = &handle_dirlist_uint8;
    hcbs.field_char = &handle_dirlist_text;
    hcbs.userdata = queue;
    parse_queue_block(queue, &base, &index, base+lim, &hcbs, &parse_wire_data, &cbs);
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
    parse_queue_block(queue, &base, &index, base+limit, &hcbs, NULL, NULL);
}

void chronicle_peek() {
    queue_t *queue = queue_head;
    while (queue != NULL) {
        chronicle_peek_queue(queue);
        queue = queue->next;
    }
}

void peek_queue_modcount(queue_t* queue) {
    // poll shared directory for modcount
    uint64_t modcount;
    memcpy(&modcount, queue->dirlist_fields.modcount, sizeof(modcount));

    if (queue->modcount != modcount) {
        printf("shmipc: %s modcount changed from %" PRIu64 " to %" PRIu64 "\n", queue->dirname, queue->modcount, modcount);
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

void chronicle_peek_queue(queue_t *queue) {
    if (debug) printf("peeking at %s\n", queue->dirname);
    peek_queue_modcount(queue);

    tailer_t *tailer = queue->tailers;
    while (tailer != NULL) {
        chronicle_peek_tailer(queue, tailer);

        tailer = tailer->next;
    }
}

tailstate_t chronicle_peek_tailer_r(queue_t *queue, tailer_t *tailer) {
    // for each cycle file { for each block { for each entry { emit }}}
    // this method runs like a generator, suspended in the innermost
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
                return TS_AWAITING_QUEUEFILE;
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
                return TS_E_STAT;
            // signal to extend queuefile iff we are an appending tailer
            if (tailer->qf_statbuf.st_size - mmapoff < 2*queue->blocksize && tailer->mmap_protection != PROT_READ) {
                return TS_EXTEND_FAIL;
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
                return TS_E_MMAP;
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
        parseqb_state_t s = parse_queue_block(queue, &basep, &index, extent, &hcbs, parse_data_cb, tailer);
        //printf("shmipc: block parser result %d, shm %p to %p\n", s, basep_old, basep);

        if (s == QB_NEED_EXTEND && basep == basep_old) {
            queue_double_blocksize(queue);
        }

        if (basep != basep_old) {
            // commit result of parsing to the tailer, adjusting for the window
            uint64_t new_tip = basep-tailer->qf_buf + tailer->qf_mmapoff;
            if (debug) printf("shmipc:  parser moved shm %p to %p, file %" PRIu64 " -> %" PRIu64 ", index %" PRIu64 " to %" PRIu64 "\n", basep_old, basep, tailer->qf_tip, new_tip, tailer->qf_index, index);
            tailer->qf_tip = new_tip;
            tailer->qf_index = index;
        }

        if (s == QB_BUSY) return TS_BUSY;
        if (s == QB_COLLECTED) return TS_COLLECTED;

        if (s == QB_AWAITING_ENTRY) { // awaiting at end of queuefile
            if (cycle < queue->highest_cycle-patch_cycles) { // allowed to fast-forward
                uint64_t skip_to_index = (cycle + 1) << queue->cycle_shift;
                printf("shmipc:  missing EOF for queuefile (cycle < highest_cycle-patch_cycles), bumping next_index from %" PRIu64 " to %" PRIu64 "\n", tailer->qf_index, skip_to_index);
                tailer->qf_index = skip_to_index;
                continue;
            }
            return TS_AWAITING_ENTRY;
        }

        if (s == QB_REACHED_EOF) {
            // we've read an EOF marker, so the next expected index is cycle++, seqnum=0
            uint64_t eof_cycle = ((tailer->qf_index >> queue->cycle_shift) + 1) << queue->cycle_shift;
            printf("shmipc:  hit EOF marker, setting next_index from %" PRIu64 " to %" PRIu64 "\n", tailer->qf_index, eof_cycle);
            tailer->qf_index = eof_cycle;
        }
    }
}

int chronicle_peek_tailer(queue_t *queue, tailer_t *tailer) {
    return tailer->state = chronicle_peek_tailer_r(queue, tailer);
}

void chronicle_debug() {
    printf("shmipc: open handles\n");

    queue_t *current = queue_head;
    while (current != NULL) {
        printf(" directory           %s\n",   current->dirname);
        printf("  handle             %p\n",   current);
        printf("  blocksize          %x\n",   current->blocksize);
        printf("  version            %d\n",   current->version);
        printf("  create             %d\n",   current->create);
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
        printf("    roll_name        %s\n",   current->roll_name);
        printf("    roll_strftime    %s\n",   current->roll_strftime);
        printf("    index_count      %d\n",   current->index_count);
        printf("    index_spacing    %d\n",   current->index_spacing);

        printf("  tailers:\n");
        tailer_t *tailer = current->tailers; // shortcut to save both collections
        while (tailer != NULL) {
            chronicle_debug_tailer(current, tailer);
            tailer = tailer->next;
        }
        printf("  appender:\n");
        if (current->appender)
            chronicle_debug_tailer(current, current->appender);

        current = current->next;
    }
}

void chronicle_debug_tailer(queue_t* queue, tailer_t* tailer) {
    const char* state_text = tailer_state_messages[tailer->state];
    printf("    dispatcher       %p\n",   tailer->dispatcher);
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

uint64_t chronicle_append(queue_t *queue, COBJ msg) {
    long ms = chronicle_clock_ms(queue);
    return chronicle_append_ts(queue, msg, ms);
}

uint64_t chronicle_append_ts(queue_t *queue, COBJ msg, long ms) {
    if (queue == NULL) return chronicle_err("queue is NULL");

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

    // caution: encodecheck may tweak blocksize, do not redorder below shmipc_peek_tailer
    size_t write_sz = queue->append_sizeof(msg);
    if (write_sz < 0) return 0;
    if (write_sz > HD_MASK_META) return chronicle_err("`shm msg sz > 30bit");
    while (write_sz > queue->blocksize)
        queue_double_blocksize(queue);

    // refresh highest and lowest, allowing our appender to follow another appender
    peek_queue_modcount(queue);

    // build a special tailer with the protection bits and file descriptor set to allow
    // writing.
    if (queue->appender == NULL) {
        tailer_t* tailer = malloc(sizeof(tailer_t));
        if (tailer == NULL) return chronicle_err("am fail");
        bzero(tailer, sizeof(tailer_t));

        // compat: writers do an extended lookback to patch missing EOFs
        tailer->qf_index = (queue->highest_cycle - patch_cycles) << queue->cycle_shift;
        tailer->dispatcher = NULL;
        tailer->state = 5;
        tailer->mmap_protection = PROT_READ | PROT_WRITE;
        tailer->queue = queue;
        queue->appender = tailer;

        // re-open directory-listing mapping in read-write mode
        int x = directory_listing_reopen(queue, O_RDWR, PROT_READ | PROT_WRITE);
        if (x != 0) {
            printf("shmipc: rw dir listing %d %s\n", x, cerr_msg);
            return -1;
        }
        if (debug) printf("shmipc: appender created\n");
    }
    tailer_t* appender = queue->appender;

    // poll the appender
    while (1) {
        int r = chronicle_peek_tailer(queue, appender);
        // TODO: 2nd call defensive to ensure 1 whole blocksize is available to put
        r = chronicle_peek_tailer(queue, appender);
        if (debug) printf("shmipc: writeloop appender in state %d\n", r);

        if (r == TS_AWAITING_QUEUEFILE) {
            // our cycle is pointing to a queuefile that does not exist
            // as we are writer, create it with temporary filename, atomically
            // move it to the desired name, then bump the global highest_cycle
            // value if rename succeeded
            char* fn_buf;
            asprintf(&fn_buf, "%s.%d.tmp", appender->qf_fn, pid_header);

            // if queuefile_init fails, re-throw the error and abort the write
            if (queuefile_init(fn_buf, queue) != 0) return -1;

            if (rename(fn_buf, appender->qf_fn) != 0) {
                // rename failed, maybe raced with another writer, delay and try again
                printf("shmipc: create queuefile %s failed at rename, errno %d\n", fn_buf, errno);
                sleep(1);
                continue;
            }
            printf("renamed %s to %s\n", fn_buf, appender->qf_fn);
            free(fn_buf);

            // if our new file higher than highest_cycle, inform listeners by bumping modcount
            uint64_t cyc = appender->qf_index >> queue->cycle_shift;
            if (cyc > queue->highest_cycle) {
                queue->highest_cycle = cyc;
                poke_queue_modcount(queue);
            }

            // rename worked, we can now re-try the peek_tailer
            continue;
        }

        if (r == TS_EXTEND_FAIL) {
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
        if (r != TS_AWAITING_ENTRY) {
            printf("shmipc: Cannot write in state %d, sleeping\n", r);
            sleep(1);
            continue;
        }

        if ((appender->qf_tip - appender->qf_mmapoff) + write_sz > appender->qf_mmapsz) {
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

            // if given a clock, test if we should write EOF and advance cycle
            if (ms > 0) {
                uint64_t cyc = chronicle_cycle_from_ms(queue, ms);
                if (cyc > appender->qf_index >> queue->cycle_shift) {
                    printf("shmipc: appender setting cycle from timestamp: current %" PRIu64 " proposed %" PRIu64 "\n", appender->qf_index >> queue->cycle_shift, cyc);
                    appender->qf_index = cyc << queue->cycle_shift;

                    printf("shmipc: got write lock, writing EOF to start roll\n");
                    uint32_t header = HD_EOF;
                    memcpy(ptr, &header, sizeof(header));
                    continue; // retry write in next queuefile
                }
            }

            // Java does not patch EOF on prev files if it is down during the roll, it
            // just starts a new file. As a workaround their readers 'timeout' the wait for EOF
            // if a higher cycle is known for the queue. I'd like to be as correct as possible, so
            // we'll patch missing EOFs during our writes if we hold the lock. This will nudge on any
            // readers who haven't noticed the roll.
            if (appender->qf_index < queue->highest_cycle << queue->cycle_shift) {
                printf("shmipc: got write lock, but about to write to queuefile < maxcycle, writing EOF\n");
                uint32_t header = HD_EOF;
                memcpy(ptr, &header, sizeof(header));
                continue; // retry write in next queuefile
            }

            queue->append_write(ptr+4, msg, write_sz);

            asm volatile ("mfence" ::: "memory");
            uint32_t header = write_sz & HD_MASK_LENGTH;
            memcpy(ptr, &header, sizeof(header));

            if (debug) printf("shmipc: wrote %zu bytes as index %" PRIu64 "\n", write_sz, appender->qf_index);

            break;
        }

        printf("shmipc: write lock failed, peeking again\n");
        sleep(1);
    }

    return appender->qf_index;
}

tailer_t* chronicle_tailer(queue_t *queue, cdispatch_f dispatcher, void* dispatch_ctx, uint64_t index) {
    if (queue == NULL) return chronicle_perr("queue is not valid");

    // decompose index into cycle (file) and seqnum within file
    int cycle = index >> queue->cycle_shift;
    int seqnum = index & queue->seqnum_mask;

    printf("shmipc: tailer added index=%" PRIu64 " (cycle=%d seqnum=%d) cb=%p\n", index, cycle, seqnum, dispatcher);
    if (cycle < queue->lowest_cycle) {
        index = queue->lowest_cycle << queue->cycle_shift;
    }
    if (cycle > queue->highest_cycle) {
        index = queue->highest_cycle << queue->cycle_shift;
    }

    // allocate struct, we'll link if all checks pass
    tailer_t* tailer = malloc(sizeof(tailer_t));
    if (tailer == NULL) return chronicle_perr("tm fail");
    bzero(tailer, sizeof(tailer_t));

    tailer->dispatch_after = index - 1;
    tailer->qf_index = index & ~queue->seqnum_mask; // start replay from first entry in file
    tailer->dispatcher = dispatcher;
    tailer->dispatch_ctx = dispatch_ctx;
    tailer->state = 5;
    tailer->mmap_protection = PROT_READ;

    tailer->next = queue->tailers; // linked list
    tailer->prev = NULL;
    if (queue->tailers) queue->tailers->prev = tailer;
    queue->tailers = tailer;

    tailer->queue = queue; // parent pointer
    return tailer;
}

COBJ chronicle_collect(tailer_t *tailer, collected_t *collected) {
    if (tailer == NULL) return chronicle_perr("null tailer");
    if (collected == NULL) return chronicle_perr("null collected");
    tailer->collect = collected;

    uint64_t delaycount = 0;
    peek_queue_modcount(tailer->queue);
    while (1) {
        int r = chronicle_peek_tailer(tailer->queue, tailer);
        if (debug) printf("collect value returns %d into object %p\n", r, tailer->collect);
        if (r == TS_COLLECTED) {
            break;
        }
        if (delaycount++ >> 20) usleep(delaycount >> 20);
    }
    tailer->collect = NULL;
    return collected->msg;
}

tailstate_t chronicle_tailer_state(tailer_t* tailer) {
    return tailer->state;
}

uint64_t chronicle_tailer_index(tailer_t* tailer) {
    return tailer->qf_index;
}

void chronicle_tailer_close(tailer_t* tailer) {
    if (tailer->qf_fn) { // if next filename cached...
        free(tailer->qf_fn);
    }
    if (tailer->qf_buf) { // if mmap() open...
        munmap(tailer->qf_buf, tailer->qf_mmapsz);
    }
    if (tailer->qf_fd) { // if open() open...
        close(tailer->qf_fd);
    }
    // unlink ourselves from doubly-linked chain and update parent pointer if we were first
    if (tailer->next) {
        tailer->next->prev = tailer->prev;
    }
    if (tailer->prev) {
        tailer->prev->next = tailer->next;
    } else if (tailer->queue->tailers == tailer) {
        tailer->queue->tailers = tailer->next;
    }
    free(tailer);
}

int chronicle_cleanup(queue_t* queue_delete) {
    if (queue_delete == NULL) return chronicle_err("queue is NULL");

    // check if queue already open
    queue_t **parent = &queue_head; // pointer to a queue_t pointer
    queue_t *queue = queue_head;

    while (queue != NULL) {
        if (queue == queue_delete) {
            *parent = queue->next; // unlink

            // delete tailers
            tailer_t *tailer = queue->tailers; // shortcut to save both collections
            while (tailer != NULL) {
                tailer_t* next_tmp = tailer->next;
                chronicle_tailer_close(tailer);
                tailer = next_tmp;
            }
            queue->tailers = NULL;

            if (queue->appender) chronicle_tailer_close(queue->appender);

            // kill queue
            munmap(queue->dirlist, queue->dirlist_statbuf.st_size);
            if (queue->dirlist_fd > 0) {
                close(queue->dirlist_fd);
            }
            free(queue->dirlist_name);
            free(queue->dirname);
            free(queue->queuefile_pattern);
            // roll_format points to global static struct
            globfree(&queue->queuefile_glob);
            free(queue);

            return 0;
        }
        parent = &queue->next;
        queue = queue->next;
    }
    return chronicle_err("chronicle_close: queue already closed");
}

int queuefile_init(char* fn, queue_t* queue) {
    int fd;
    int mode = 0777;

    printf("Creating %s\n", fn);

    // open/create the output file
    if ((fd = open(fn, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0) {
        printf("can't create %s for writing", fn);
        return chronicle_err("shmipc: create tmp queuefile err");
    }

    // go to the location corresponding to the last byte
    if (lseek(fd, qf_disk_sz - 1, SEEK_SET) == -1) {
        return chronicle_err("shmipc: lseek error");
    }

    // write a dummy byte at the last location
    if (write(fd, "", 1) != 1) {
        return chronicle_err("shmipc: write error");
    }

    // TODO: write header
    // TODO: write index2index
    printf("Created %s\n", fn);

    close(fd);
    return 0;
}

int directory_listing_init(queue_t* queue, uint64_t cycle) {
    int fd;
    int mode = 0777;

    if ((fd = open(queue->dirlist_name, O_RDWR | O_CREAT | O_TRUNC, mode)) < 0) {
        printf("can't create %s for writing", queue->dirlist_name);
        return chronicle_err("shmipc: directory_listing_init open failed");
    }

    wirepad_t* pad = wirepad_init(1024);

    // deliberately not updating queue->roll_epoch here so that we verify
    // we can read them back from the directory_listing file
    int roll_epoch = (queue->roll_epoch == -1) ? 0 : queue->roll_epoch;
    int modcount = 1;

    // single metadata message
    wirepad_qc_start(pad, 1);
    wirepad_event_name(pad, "header");
    wirepad_type_prefix(pad, "STStore");
    wirepad_nest_enter(pad); //header
      wirepad_field_type_enum(pad, "wireType", "WireType", "BINARY_LIGHT");
      // field metadata, type prefix SCQMeta, nesting begin
      wirepad_field(pad, "metadata");
      wirepad_type_prefix(pad, "SCQMeta");
      wirepad_nest_enter(pad);
        wirepad_field(pad, "roll");
        wirepad_type_prefix(pad, "SCQSRoll");
        wirepad_nest_enter(pad);
          wirepad_field_varint(pad, "length", queue->roll_length);
          wirepad_field_text(pad, "format", queue->roll_format);
          wirepad_field_varint(pad, "epoch", roll_epoch);
        wirepad_nest_exit(pad);
        wirepad_field_varint(pad, "deltaCheckpointInterval", 64);
        wirepad_field_varint(pad, "sourceId", 0);
      wirepad_nest_exit(pad);
      wirepad_pad_to_x8(pad); // feels wrong - should be automatic?
      wirepad_nest_exit(pad);
    wirepad_qc_finish(pad);

    // 6 data messages
    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.highestCycle");
    wirepad_uint64_aligned(pad, cycle); // this cannot be varint as memory mapped! explicit size
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.lowestCycle");
    wirepad_uint64_aligned(pad, cycle);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.modCount");
    wirepad_uint64_aligned(pad, modcount);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.write.lock");
    wirepad_uint64_aligned(pad, 0x8000000000000000);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.lastIndexReplicated");
    wirepad_uint64_aligned(pad, -1);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.lastAcknowledgedIndexReplicated");
    wirepad_uint64_aligned(pad, -1);
    wirepad_qc_finish(pad);

    write(fd, wirepad_base(pad), wirepad_sizeof(pad));
    close(fd);

    wirepad_free(pad);
    return 0;
}

int directory_listing_reopen(queue_t* queue, int open_flags, int mmap_prot) {
    if ((queue->dirlist_fd = open(queue->dirlist_name, open_flags)) < 0) {
        return chronicle_err("directory_listing_reopen open failed");
    }

    // find size of dirlist and mmap
    if (fstat(queue->dirlist_fd, &queue->dirlist_statbuf) < 0)
        return chronicle_err("dirlist fstat");
    if ((queue->dirlist = mmap(0, queue->dirlist_statbuf.st_size, mmap_prot, MAP_SHARED, queue->dirlist_fd, 0)) == MAP_FAILED)
        return chronicle_err("dirlist mmap fail");

    if (debug) printf("shmipc: parsing dirlist\n");
    parse_dirlist(queue);

    // check the polled fields in header section were all resolved to pointers within the map
    if (queue->dirlist_fields.highest_cycle == NULL || queue->dirlist_fields.lowest_cycle == NULL ||
        queue->dirlist_fields.modcount == NULL) {
        return chronicle_err("dirlist parse hdr ptr fail");
    }
    return 0;
}

COBJ chronicle_decoder_default_parse(unsigned char* base, int lim) {
    char* msg = calloc(1, lim+1);
    memcpy(msg, base, lim);
    return msg;
}

size_t chronicle_encoder_default_sizeof(COBJ msg) {
    return strlen(msg);
}

void chronicle_encoder_default_write(unsigned char* base, COBJ msg, size_t sz) {
    memcpy(base, msg, sz);
}
