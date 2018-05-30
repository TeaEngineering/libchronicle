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
    long long highest_cycle;
    long long lowest_cycle;
    long modcount;
} dirlist_fields_t;

typedef struct queue {
    char*             dirname;
    char*             hsymbolp;
    int               blocksize;

    // directory-listing.cq4t
    char*             dirlist_name;
    int               dirlist_fd;
    struct stat       dirlist_statbuf;
    char*             dirlist; // mmap base
    // struct dl_info    dirlist_info; // useful content from wire protocol content
    dirlist_fields_t  dirlist_fields;

    char*             queuefile_pattern;
    glob_t            queuefile_glob; // last glob of data files

    // current cycle observed at poll
    long long int     cycle;

    // tailers LL
    // appenders LL

    struct queue *next;
} queue_t;

// globals
char* debug = NULL;
queue_t *queue_head = NULL;

// forward declarations
void parse_dirlist(queue_t *item);
void parse_data_wire(char* buf, int n);

K shmipc_init(K dir) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle :");

    debug = getenv("SHMIPC_DEBUG");

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
    if ((item->dirlist = mmap(0, item->dirlist_statbuf.st_size, PROT_READ, MAP_SHARED, item->dirlist_fd, 0)) == (caddr_t) -1)
        return krr("dirlist mmap fail");

    printf("parsing dirlist\n");
    parse_dirlist(item);

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

void parse_dirlist(queue_t *item) {
    // our mmap is the size of the fstat, so bound the replay
    int lim = item->dirlist_statbuf.st_size;
    int n = 0;
    char* base = item->dirlist;

    uint32_t header;
    int sz;
    while (1) {
        memcpy(&header, base, sizeof(header)); // relax, optimised away
        if (header == HD_UNALLOCATED) {
            printf(" %8d @%p unallocated\n", n, base);
            return;
        } else if ((header & HD_MASK_META) == HD_WORKING) {
            printf(" %8d @%p working\n", n, base);
            return;
        } else if ((header & HD_MASK_META) == HD_METADATA) {
            sz = (header & HD_MASK_LENGTH);
            printf(" %8d @%p metadata size %x\n", n, base, sz);
        } else if ((header & HD_MASK_META) == HD_EOF) {
            printf(" %8d @%p EOF\n", n, base);
            return;
        } else {
            sz = (header & HD_MASK_LENGTH);
            printf(" %8d @%p data size %x\n", n, base, sz);
            parse_data_wire(base+4, sz);
        }
        int align = ((sz & 0x0000003F) >= 60) ? -60 + (sz & 0x3F) : 0;
        printf("  %p + 4 + size %d + align %x\n", base, sz, align);
        base = base + 4 + sz + align;
        n++;
    }
}

void parse_data_wire(char* base, int n) {
    char* p = base;
    uint8_t control;

    char* ev_name = NULL;
    uint8_t ev_name_sz = 0;
    uint64_t jlong = 0;
    uint32_t padding32 = 0;

    while (p-base < n) {
        control = p[0];
        switch(control) {
            case 0xB9: // EVENT_NAME
                ev_name = p+2;
                ev_name_sz = p[1];
                p += 2 + ev_name_sz;
                break;
            case 0x8F: // PADDING
                p++;
                break;
            case 0x8E: // PADDING_32
                memcpy(&padding32, p+1, sizeof(padding32));
                p += 1 + 4 + padding32;
                break;
            case 0xA7: // INT64
                memcpy(&jlong, p+1, sizeof(jlong));
                printf("    %.*s = %llu\n", ev_name_sz, ev_name, jlong);
                p += 1 + 8;
                break;
            default:
                printf("Aborted DATA unknown control %d %02x\n", control, control);
                p = base + n;
                break;
        }
    }
}

K shmipc_peek(K x){
    queue_t *current = queue_head;
    while (current != NULL) {
        printf("peeking at %s\n", current->hsymbolp);
        current = current->next;
    }
    return ki(0);
}

K shmipc_debug(K x) {
    printf("shmipc open handles\n");

    queue_t *current = queue_head;
    while (current != NULL) {
        printf("  handle             %s\n", current->hsymbolp);
        printf("  dirlist_name       %s\n", current->dirlist_name);
        printf("  dirlist_fd         %d\n", current->dirlist_fd);
        printf("  dirlist_sz         %lld\n", current->dirlist_statbuf.st_size);
        printf("  dirlist            %p\n", current->dirlist);
        printf("  queuefile_pattern  %s\n", current->queuefile_pattern);
        printf("  cycle              %lld", current->cycle);
        current = current->next;
    }
    return 0;
}

K shmipc_appender(K x) {
    //free(buf_base);
    return 0;
}

K shmipc_tailer(K x, K y, K z){
    return krr("NYI soon");
}
