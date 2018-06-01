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

typedef struct {
    unsigned char *highest_cycle;
    unsigned char *lowest_cycle;
    unsigned char *modcount;
} dirlist_fields_t;

typedef struct {
    uint64_t next;
    K callback;
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
    // struct dl_info    dirlist_info; // useful content from wire protocol content
    dirlist_fields_t  dirlist_fields;

    char*             queuefile_pattern;
    glob_t            queuefile_glob; // last glob of data files, refreshed by poll on modcount

    // current cycle observed at poll
    long long int     cycle;

    uint64_t          highest_cycle;
    uint64_t          lowest_cycle;
    uint64_t          modcount;

    // a window into one of the queue files
    uint64_t          open_cycle;
    int               queuefile_fd;
    struct stat       queuefile_statbuf;
    uint64_t          queuefile_base; // offset within file of mmap, size mapped is blocksize or until extent
    unsigned char*    queuefile_buf;

    // tailers LL
    // appenders LL

    struct queue *next;
} queue_t;

// globals
char* debug = NULL;
uint64_t pid_header = 0;
queue_t *queue_head = NULL;

// forward declarations
void parse_dirlist(queue_t *item);
void parse_queuefile(queue_t *item);

K shmipc_init(K dir) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");

    debug = getenv("SHMIPC_DEBUG");
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
    printf("shmipc: %d queue files found\n", g->gl_matchc);
    if (g->gl_matchc < 1) {
        return krr("no queue files - java run?");
    }
    for (int i = 0; i < g->gl_matchc;i++) {
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
    char* fn = item->queuefile_glob.gl_pathv[0];
    // find size of dirlist and mmap
    if ((item->queuefile_fd = open(fn, O_RDONLY)) < 0) {
        return orr("qf open");
    }
    if (fstat(item->queuefile_fd, &item->queuefile_statbuf) < 0)
        return krr("qf fstat");
    if ((item->queuefile_buf = mmap(0, item->queuefile_statbuf.st_size, PROT_READ, MAP_SHARED, item->queuefile_fd, 0)) == MAP_FAILED)
        return krr("qf mmap fail");

    printf("shmipc: parsing queuefile %s\n", fn);
    parse_queuefile(item);

    // Good to use
    item->next = queue_head;
    item->hsymbolp = dir->s;
    queue_head = item;

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


void handle_dirlist_fields(char* buf, int sz, unsigned char *dptr, wirecallbacks_t* cbs) {
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

typedef void (*parsedata_f)(unsigned char*,int,void* userdata);

void parse_queue_block(unsigned char* base, int n, wirecallbacks_t* hcbs, parsedata_f parse_data, void* userdata) {

    uint32_t header;
    int sz;
    while (1) {
        memcpy(&header, base, sizeof(header)); // relax, fn optimised away
        asm volatile ("mfence" ::: "memory");

        if (header == HD_UNALLOCATED) {
            printf(" %d @%p unallocated\n", n, base);
            return;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            printf(" %d @%p locked for writing by pid %d\n", n, base, header & HD_MASK_LENGTH);
            return;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            printf(" %d @%p metadata size %x\n", n, base, sz);
            parse_wire(base+4, sz, hcbs);
            // EventName  header
            //   switch to header parser
        } else if ((header & HD_MASK_META) == HD_EOF) {
            printf(" %d @%p EOF\n", n, base);
            return;
        } else {
            sz = (header & HD_MASK_LENGTH);
            printf(" %d @%p data size %x\n", n, base, sz);
            if (parse_data) {
                parse_data(base+4, sz, userdata);
            } else if (userdata) {
                parse_wire(base+4, sz, userdata);
            }
        }
        int align = ((sz & 0x0000003F) >= 60) ? -60 + (sz & 0x3F) : 0;
        // printf("  %p + 4 + size %d + align %x\n", base, sz, align);
        base = base + 4 + sz + align;
        n++;
    }
}

void parse_dirlist(queue_t *item) {
    // our mmap is the size of the fstat, so bound the replay
    int lim = item->dirlist_statbuf.st_size;
    int n = 0;
    unsigned char* base = item->dirlist;

    wirecallbacks_t cbs;
    bzero(&cbs, sizeof(cbs));
    cbs.event_uint64 = &handle_dirlist_fields;
    cbs.userdata = item;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    parse_queue_block(base, n, &hcbs, parse_wire, &cbs);
}

void parse_queuefile(queue_t *item) {
    // our mmap is the size of the fstat, so bound the replay
    int lim = item->blocksize;
    int n = 0;
    unsigned char* base = item->queuefile_buf;

    wirecallbacks_t cbs;
    bzero(&cbs, sizeof(cbs));
    // cbs.event_uint64 = &handle_dirlist_fields;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));

    parse_queue_block(base, n, &hcbs, NULL, NULL);
}

K shmipc_peek(K x){
    queue_t *current = queue_head;
    uint64_t modcount;

    while (current != NULL) {
        printf("peeking at %s\n", current->hsymbolp);

        // poll shared directory for modcount
        // TODO: check header unlocked?
        memcpy(&modcount, current->dirlist_fields.modcount, sizeof(modcount));

        if (current->modcount != modcount) {
            printf(" %s modcount changed from %llu to %llu - scanning\n", current->hsymbolp, current->modcount, modcount);
            // slowpath poll
            memcpy(&current->modcount, current->dirlist_fields.modcount, sizeof(modcount));
            memcpy(&current->lowest_cycle, current->dirlist_fields.lowest_cycle, sizeof(modcount));
            memcpy(&current->highest_cycle, current->dirlist_fields.highest_cycle, sizeof(modcount));

            // now scan filenames as directory contents changed
        }


        current = current->next;
    }
    return ki(0);
}

K shmipc_debug(K x) {
    printf("shmipc open handles\n");

    queue_t *current = queue_head;
    while (current != NULL) {
        printf(" handle              %s\n", current->hsymbolp);
        printf("  dirlist_name       %s\n", current->dirlist_name);
        printf("  dirlist_fd         %d\n", current->dirlist_fd);
        printf("  dirlist_sz         %lld\n", current->dirlist_statbuf.st_size);
        printf("  dirlist            %p\n", current->dirlist);
        printf("    cycle-low        %lld\n", current->lowest_cycle);
        printf("    cycle-high       %lld\n", current->highest_cycle);
        printf("    modcount         %llu\n", current->modcount);
        printf("  queuefile_pattern  %s\n", current->queuefile_pattern);

        current = current->next;
    }
    return 0;
}

K shmipc_appender(K x) {
    //free(buf_base);
    return 0;
}

K shmipc_tailer(K handle, K cb, K start) {

    return krr("NYI soon");
}
