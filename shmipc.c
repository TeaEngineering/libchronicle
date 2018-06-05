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
#include "buffer.h"
#include "wire.h"

/**
 * Note
 * This code is not reentrant, but there is only one kx main thread, so this is not a problem.
 * Should not be used by the slave threads without external locking.
 * However multiple processes can write and read from a queue concurrently, and one process
 * may read or write from multiple queues.
 *
 * The current iteration requires a Java Chronicle-Queues node to be writing to the same queue
 * on a periodic timer to roll over the log files and maintain the index structures.
 *
 * It should be possible to append to a queue from a tailer on same queue, so the fds and mmaps
 * used for writing are separate to those used in recovery.
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

// a couple of funciton pointer typedefs
typedef void (*parsedata_f)(unsigned char*,int,uint64_t,void* userdata);

typedef struct {
    unsigned char *highest_cycle;
    unsigned char *lowest_cycle;
    unsigned char *modcount;
} dirlist_fields_t;

typedef struct tailer {
    uint64_t          index;
    int               state;

    char*             qf_fn;
    int               qf_fd;
    struct stat       qf_statbuf;
    uint64_t          qf_extent;
    unsigned char*    qf_buf;
    uint64_t          qf_tip; // byte position of the next header within qf_fd
    K                 callback;
    struct tailer*    next;
} tailer_t;

typedef struct queue {
    char*             dirname;
    char*             hsymbolp;
    int               blocksize;

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

    tailer_t*         tailers;

    // the appender is a shared tailer, polled by append[], with writing logic
    // and no callback to user code for events
    tailer_t*         appender;

    struct queue*     next;
} queue_t;

// globals
char* debug = NULL;
uint64_t pid_header = 0;
queue_t *queue_head = NULL;

// forward declarations
void parse_dirlist(queue_t *item);
void parse_queuefile_meta(unsigned char* base, queue_t *item);
void parse_queuefile_data(unsigned char* base, queue_t *item, uint64_t index);
int shmipc_peek_tailer(queue_t*, tailer_t*);

char* get_cycle_fn_yyyymmdd(queue_t *item, int cycle) {
    char* buf;
    // time_t aka long. seconds since midnight 1970
    time_t rawtime = cycle * 60*60*24;
    struct tm info;
    gmtime_r(&rawtime, &info);

    // tail of this buffer is overwritten by the strftime below
    int bufsz = asprintf(&buf, "%s/yyyymmdd.cq4", item->dirname);
    strftime(buf+bufsz-12, 13, "%Y%m%d.cq4", &info);
    return buf;
}

K shmipc_init(K dir, K parser) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");
    if (dir->t != -KS) return krr("parser is not symbol");

    debug = getenv("SHMIPC_DEBUG");
    wire_trace = getenv("SHMIPC_WIRETRACE");

    pid_header = (getpid() & HD_MASK_LENGTH) | HD_WORKING;

    printf("shmipc: opening dir %p %p %s\n", dir, dir->s, dir->s);

    // check if queue already open
    queue_t *item = queue_head;
    while (item != NULL) {
        if (item->hsymbolp == dir->s) return krr("shmipc dir dupe init");
        item = item->next;
    }

    // allocate struct, we'll link if all checks pass
    item = malloc(sizeof(queue_t));
    if (item == NULL) return krr("m fail");
    bzero(item, sizeof(queue_t));

    // dir is on the stack, but dir->s points to the heap interned table. safe to use ref to q
    item->dirname = &dir->s[1];
    item->blocksize = 1024;

    // Is this a directory
    struct stat statbuf;
    if (stat(item->dirname, &statbuf) != 0) return krr("stat fail");
    if (!S_ISDIR(statbuf.st_mode)) return krr("dir is not a directory");

    // Does it contain some .cq4 files?
    glob_t *g = &item->queuefile_glob;
    g->gl_offs = 0;

    asprintf(&item->queuefile_pattern, "%s/*.cq4", item->dirname);
    glob(item->queuefile_pattern, GLOB_ERR, NULL, g);
    printf("shmipc: glob %zu queue files found\n", g->gl_pathc);
    if (g->gl_pathc < 1) {
        return krr("no queue files - java run?");
    }
    for (int i = 0; i < g->gl_pathc;i++) {
        printf("   %s\n", g->gl_pathv[i]);
    }

    // Can we map the directory-listing.cq4t file
    asprintf(&item->dirlist_name, "%s/directory-listing.cq4t", item->dirname);
    if ((item->dirlist_fd = open(item->dirlist_name, O_RDONLY)) < 0) {
        return orr("dirlist open");
    }

    // find size of dirlist and mmap
    if (fstat(item->dirlist_fd, &item->dirlist_statbuf) < 0)
        return krr("dirlist fstat");
    if ((item->dirlist = mmap(0, item->dirlist_statbuf.st_size, PROT_READ, MAP_SHARED, item->dirlist_fd, 0)) == MAP_FAILED)
        return krr("dirlist mmap fail");

    printf("shmipc: parsing dirlist\n");
    parse_dirlist(item);

    // check the polled fields in header section were all resolved to pointers within the map
    if (item->dirlist_fields.highest_cycle == NULL || item->dirlist_fields.lowest_cycle == NULL ||
        item->dirlist_fields.modcount == NULL) {
        return krr("dirlist parse hdr ptr fail");
    }

    // read a 'queue' header from any one of the datafiles to get the
    // rollover configuration. We don't know how to generate a filename from a cycle code
    // yet, so this needs to use the directory listing.
    // TODO: don't map whole, map blocksize
    int               queuefile_fd;
    struct stat       queuefile_statbuf;
    uint64_t          queuefile_extent;
    unsigned char*    queuefile_buf;

    char* fn = item->queuefile_glob.gl_pathv[0];
    // find size of dirlist and mmap
    if ((queuefile_fd = open(fn, O_RDONLY)) < 0) {
        return orr("qfi open");
    }
    if (fstat(queuefile_fd, &queuefile_statbuf) < 0)
        return krr("qfi fstat");
    if ((queuefile_buf = mmap(0, queuefile_statbuf.st_size, PROT_READ, MAP_SHARED, queuefile_fd, 0)) == MAP_FAILED)
        return krr("qfi mmap fail");

    // we don't need a data-parser at this stage as we only need values from the header
    printf("shmipc: parsing queuefile %s\n", fn);
    parse_queuefile_meta(queuefile_buf, item);

    // close queuefile
    munmap(queuefile_buf, queuefile_statbuf.st_size);
    close(queuefile_fd);

    // check we loaded some rollover settings from queuefile
    if (item->roll_length == 0) krr("qfi roll_length fail");
    if (item->index_count == 0) krr("qfi index_count fail");
    if (item->index_spacing == 0) krr("qfi index_spacing fail");
    if (item->roll_format == 0) krr("qfi roll_format fail");

    // TODO check yyyymmdd
    item->cycle2file_fn = &get_cycle_fn_yyyymmdd;
    item->cycle_shift = 32;
    // TODO: Logic from RollCycles.java ensures rollover occurs before we run out of index2index pages?
    //  cycleShift = Math.max(32, Maths.intLog2(indexCount) * 2 + Maths.intLog2(indexSpacing));

    // verify user-specified parser for data segments
    if (strncmp(parser->s, "text", 5) == 0) {
        printf("shmipc: format set to text\n");
    } else {
        return krr("bad format");
    }

    // Good to use
    item->next = queue_head;
    item->hsymbolp = dir->s;
    queue_head = item;

    printf("shmipc: init complete\n");

    if (debug) {
        printf("shmipc: opening");
    }
    return (K)NULL;

    // wip: unwinder
//unwind_1:
    free(item->dirlist_name);
    free(item->queuefile_pattern);
    close(item->dirlist_fd);
    globfree(&item->queuefile_glob);
    // unlink LL if entered
    munmap(item->dirlist, item->dirlist_statbuf.st_size);
//unwind_2:
    free(item);

    return (K)NULL;

}

void parse_queue_block(unsigned char* base, uint64_t seqnum, wirecallbacks_t* hcbs, parsedata_f parse_data, void* userdata) {

    uint32_t header;
    int sz;
    while (1) {
        memcpy(&header, base, sizeof(header)); // relax, fn optimised away
        // no speculative fetches before the header is read
        asm volatile ("mfence" ::: "memory");

        if (header == HD_UNALLOCATED) {
            printf(" %llu @%p unallocated\n", seqnum, base);
            return;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            printf(" @%p locked for writing by pid %d\n", base, header & HD_MASK_LENGTH);
            return;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            printf(" @%p metadata size %x\n", base, sz);
            parse_wire(base+4, sz, 0, hcbs);
            // EventName  header
            //   switch to header parser
        } else if ((header & HD_MASK_META) == HD_EOF) {
            printf(" @%p EOF\n", base);
            return;
        } else {
            sz = (header & HD_MASK_LENGTH);
            printf(" %llu @%p data size %x\n", seqnum, base, sz);
            if (parse_data) {
                parse_data(base+4, sz, seqnum, userdata);
            } else {
                // bail at first data message
                return;
            }
            seqnum++;
        }
        int align = ((sz & 0x0000003F) >= 60) ? -60 + (sz & 0x3F) : 0;
        // printf("  %p + 4 + size %d + align %x\n", base, sz, align);
        base = base + 4 + sz + align;
    }
}

void handle_dirlist_ptr(char* buf, int sz, unsigned char *dptr, wirecallbacks_t* cbs) {
    // we are preserving *pointers* within the shared directory data page
    queue_t *item = (queue_t*)cbs->userdata;
    if (strncmp(buf, "listing.highestCycle", sz) == 0) {
        item->dirlist_fields.highest_cycle = dptr;
    } else if (strncmp(buf, "listing.lowestCycle", sz) == 0) {
        item->dirlist_fields.lowest_cycle = dptr;
    } else if (strncmp(buf, "listing.modCount", sz) == 0) {
        item->dirlist_fields.modcount = dptr;
    }
}

void handle_qf_uint32(char* buf, int sz, uint32_t data, wirecallbacks_t* cbs){
    queue_t *item = (queue_t*)cbs->userdata;
    if (strncmp(buf, "length", sz) == 0) {
        item->roll_length = data;
    }
}

void handle_qf_uint16(char* buf, int sz, uint16_t data, wirecallbacks_t* cbs) {
    queue_t *item = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexCount", sz) == 0) {
        item->index_count = data;
    }
}

void handle_qf_uint8(char* buf, int sz, uint8_t data, wirecallbacks_t* cbs) {
    queue_t *item = (queue_t*)cbs->userdata;
    if (strncmp(buf, "indexSpacing", sz) == 0) {
        item->index_spacing = data;
    }
}

void handle_qf_text(char* buf, int sz, char* data, int dsz, wirecallbacks_t* cbs) {
    queue_t *item = (queue_t*)cbs->userdata;
    if (strncmp(buf, "format", sz) == 0) {
        item->roll_format = strndup(data, dsz);
    }
}

void parse_dirlist(queue_t *item) {
    // our mmap is the size of the fstat, so bound the replay
    int lim = item->dirlist_statbuf.st_size;
    int n = 0;
    unsigned char* base = item->dirlist;

    wirecallbacks_t cbs;
    bzero(&cbs, sizeof(cbs));
    cbs.ptr_uint64 = &handle_dirlist_ptr;
    cbs.userdata = item;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    parse_queue_block(base, n, &hcbs, &parse_wire2, &cbs);
}

void parse_queuefile_meta(unsigned char* base, queue_t *item) {
    int n = 0;
    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    hcbs.field_uint32 = &handle_qf_uint32;
    hcbs.field_uint16 = &handle_qf_uint16;
    hcbs.field_uint8 =  &handle_qf_uint8;
    hcbs.field_char = &handle_qf_text;
    hcbs.userdata = item;
    parse_queue_block(base, n, &hcbs, NULL, NULL);
}

void parse_queuefile_data(unsigned char* base, queue_t *item, uint64_t cycle) {
    // our mmap is the size of the fstat, so bound the replay
    uint64_t index = cycle << item->cycle_shift;
    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    hcbs.userdata = item;
    parse_queue_block(base, index, &hcbs, &parse_data_text, NULL);
}


K shmipc_peek(K x) {
    queue_t *queue = queue_head;
    uint64_t modcount;

    while (queue != NULL) {
        printf("peeking at %s\n", queue->hsymbolp);

        // poll shared directory for modcount
        // TODO: check header unlocked?
        memcpy(&modcount, queue->dirlist_fields.modcount, sizeof(modcount));

        if (queue->modcount != modcount) {
            printf(" %s modcount changed from %llu to %llu - scanning\n", queue->hsymbolp, queue->modcount, modcount);
            // slowpath poll
            memcpy(&queue->modcount, queue->dirlist_fields.modcount, sizeof(modcount));
            memcpy(&queue->lowest_cycle, queue->dirlist_fields.lowest_cycle, sizeof(modcount));
            memcpy(&queue->highest_cycle, queue->dirlist_fields.highest_cycle, sizeof(modcount));

            // now scan filenames as directory contents changed
        }

        tailer_t *tailer = queue->tailers; // shortcut to save both collections
        while (tailer != NULL) {
            shmipc_peek_tailer(queue, tailer);

            tailer = tailer->next;
        }

        queue = queue->next;
    }
    return ki(0);
}


// return codes exposed via. tailer-> state
//     0    awaiting next entry
//     -1   missing queuefile indicated, awaiting advance or creation
//     -2   fstat failed
//     -3   mmap failed (probably fatal)
//     -4   queue file locked, awaiting unlock
//     -5   reached EOF entry
const char* tailer_state_messages[] = {"AWAITING", "E_STAT", "E_MMAP", "E_LOCK", "E_EOF"};

int shmipc_peek_tailer(queue_t *queue, tailer_t *tailer) {
    // for each cycle file { for each block { for each entry { emit }}}
    // we need to be able to run this like a generator... suspend in the innermost
    // iteration when we hit the end of the file and pick up at the next poll()

    // convert cycle to filename
    uint64_t cycle = tailer->index >> queue->cycle_shift;

    for (uint64_t i = cycle; i <= queue->highest_cycle; i++) {
        tailer->qf_fn = queue->cycle2file_fn(queue, i);
        printf("cycle %llu filename %s\n", i, tailer->qf_fn);

        // find size of dirlist and mmap
        if ((tailer->qf_fd = open(tailer->qf_fn, O_RDONLY)) < 0) {
            printf("shmipc: missing queuefile for %s %d errno=%d\n", tailer->qf_fn, tailer->qf_fd, errno);
            return -1;
        } else {
            if (fstat(tailer->qf_fd, &tailer->qf_statbuf) < 0)
                return -2;

            if ((tailer->qf_buf = mmap(0, tailer->qf_statbuf.st_size, PROT_READ, MAP_SHARED, tailer->qf_fd, 0)) == MAP_FAILED)
                return -3;

            // TODO for each blocksize in mmap + overhang
            parse_queuefile_data(tailer->qf_buf, queue, i);

            // close queuefile
            munmap(tailer->qf_buf, tailer->qf_statbuf.st_size);
            close(tailer->qf_fd);
        }
        free(tailer->qf_fn);
        tailer->qf_fn = NULL;
    }
    return 0;
}

K shmipc_debug(K x) {
    printf("shmipc: open handles\n");

    queue_t *current = queue_head;
    while (current != NULL) {
        printf(" handle              %s\n",   current->hsymbolp);
        printf("  blocksize          %x\n",   current->blocksize);
        printf("  dirlist_name       %s\n",   current->dirlist_name);
        printf("  dirlist_fd         %d\n",   current->dirlist_fd);
        printf("  dirlist_sz         %lld\n", current->dirlist_statbuf.st_size);
        printf("  dirlist            %p\n",   current->dirlist);
        printf("    cycle-low        %lld\n", current->lowest_cycle);
        printf("    cycle-high       %lld\n", current->highest_cycle);
        printf("    modcount         %llu\n", current->modcount);
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
            const char* state_text = "";
            if (tailer->state < 0) state_text = tailer_state_messages[-tailer->state];
            printf("    callback         %p\n",   tailer->callback);
            int cycle = tailer->index >> current->cycle_shift;
            int seqnum = tailer->index & current->seqnum_mask;
            printf("    index            %llu (cycle %d, seqnum %d)\n", tailer->index, cycle, seqnum);
            printf("    state            %d - %s\n", tailer->state, state_text);
            printf("    qf_fn            %s\n",   tailer->qf_fn);
            printf("    qf_fd            %d\n",   tailer->qf_fd);
            printf("    qf_statbuf_sz    %lld\n", tailer->qf_statbuf.st_size);
            printf("    qf_tip           %llu\n", tailer->qf_tip);
            printf("    qf_extent        %llu\n", tailer->qf_extent);
            printf("    qf_buf           %p\n",   tailer->qf_buf);
            tailer = tailer->next;
        }
        current = current->next;
    }
    return 0;
}

K shmipc_appender(K x) {

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

    return 0;
}

K shmipc_tailer(K dir, K cb, K kindex) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");
    if (cb->t != 100) return krr("cb is not function");
    if (kindex->t != -KJ) return krr("index must be J");

    // check if queue already open
    queue_t *item = queue_head;
    while (item != NULL) {
        if (item->hsymbolp == dir->s) break;
        item = item->next;
    }
    if (item == NULL) return krr("dir must be shmipc.init[] first");

    // decompose index into cycle (file) and seqnum within file
    J index = kindex->j;
    int cycle = index >> item->cycle_shift;
    int seqnum = index & item->seqnum_mask;

    printf("shmipc: tailer added index=%llu (cycle=%d seqnum=%d)\n", index, cycle, seqnum);
    if (cycle < item->lowest_cycle) {
        index = item->lowest_cycle << item->cycle_shift;
    }
    if (cycle > item->highest_cycle) {
        index = item->highest_cycle << item->cycle_shift;
    }

    // allocate struct, we'll link if all checks pass
    tailer_t* tailer = malloc(sizeof(tailer_t));
    if (tailer == NULL) return krr("tm fail");
    bzero(tailer, sizeof(tailer_t));

    tailer->index = index;
    tailer->callback = cb;

    tailer->next = item->tailers;
    item->tailers = tailer;

    return (K)NULL;
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
                if (tailer->qf_fn) { // if next filename cached...
                    free(tailer->qf_fn);
                }
                if (tailer->qf_buf) { // if mmap() open...
                    munmap(tailer->qf_buf, tailer->qf_extent);
                }
                if (tailer->qf_fd) { // if open() open...
                    close(tailer->qf_fd);
                }
                tailer = tailer->next;
            }

            // kill queue
            munmap(queue->dirlist, queue->dirlist_statbuf.st_size);
            close(queue->dirlist_fd);
            free(queue->dirlist_name);
            free(queue->queuefile_pattern);
            globfree(&queue->queuefile_glob);
            free(queue);

            return (K)NULL;
        }
        parent = &queue->next;
        queue = queue->next;
    }
    return krr("does not exist");
}
