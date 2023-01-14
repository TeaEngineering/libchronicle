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

#define KXVER 3
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include "k.h"
#include "mock_k.h"
#include "serdes_k.h"
#include "libchronicle.h"

// This is a stand-alone tool for replaying a queue for use with fuzzer
int print_data = 0;
int print_meta = 0;

int print_msg(void* ctx, uint64_t index, void* msg) {
    K x = (K)msg;
    if (print_data) {
        printf("[%" PRIu64 "] KC ", index);
        if (x->t == KC)
            fwrite(kC(x), sizeof(char), x->n, stdout);
        printf("\n");
    }
    return 5;
}

/* The state array must be initialized to not be all zero */
uint32_t xorshift128(uint32_t state[static 4]) {
    /* Algorithm "xor128" from p. 5 of Marsaglia, "Xorshift RNGs" */
    uint32_t s, t = state[3];
    t ^= t << 11;
    t ^= t >> 8;
    state[3] = state[2]; state[2] = state[1]; state[1] = s = state[0];
    t ^= s;
    t ^= s >> 19;
    state[0] = t;
    return t;
}

//////// APPLE VALGRIND COMPAT ////////
// valgrind has no support for mach system call under mkstemp, printing
//     WARNING: unhandled amd64-darwin syscall: unix:464      and
// returning -1 as fp whilst setting syserr to "Function not implemented".
// Due to the fail the random number generator leaks a tonne of malloc() mem.
// Reported but unresolved https://www.mail-archive.com/kde-bugs-dist@kde.org/msg209613.html
static inline uint64_t rdtsc(void) {
    uint32_t a, d;
    __asm__ __volatile__("rdtscp" : "=a" (a), "=d" (d));
    return (((uint64_t) d << 32) | a);
}
int c_mkstemp(char* pattern) {
    uint32_t xor_state[4];
    xor_state[0] = xor_state[1] = xor_state[2] = xor_state[3] = (uint32_t)rdtsc();
    char* rep = strstr(pattern, "XXXXXX");
    if (rep) {
        for (int i = 0; i < 6; i++) {
            uint8_t incre = xorshift128(xor_state) & 0x1F;
            rep[i] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"[incre];
        }
    }
    int fid = open(pattern, O_RDWR | O_CREAT | O_EXCL, 0660);
    if (fid < 0) {
        printf("tmpfile %s: %s\n", pattern, strerror(errno));
        abort();
    }
    return fid;
}
////////  END APPLE VALGRIND COMPAT ////////

int main(const int argc, char **argv) {
    int c;
    opterr = 0;
    int verboseflag = 0;
    FILE * fuzzfid = NULL;

    while ((c = getopt(argc, argv, "dmv:")) != -1)
    switch (c) {
        case 'd':
            print_data = 1;
            break;
        case 'm':
            print_meta = 1;
            break;
        case 'v':
            verboseflag = 1;
            break;
        case '?':
            fprintf(stderr, "Unknown option `-%c'.\n", optopt);
            exit(2);
        default:
            fprintf(stderr, "%c??", c);
            exit(3);
    }

    if (optind + 2 > argc) {
        printf("Missing mandatory argument.\n Expected: %s [-d] [-m] [-v] QUEUE FUZZFILE\n", argv[0]);
        printf("  -d print data\n");
        printf("  -m print metadata\n");
        printf("  -v verbose mode\n");
        printf("  QUEUE     queue directory\n");
        printf("  FUZZFILE  fuzzing script file (- stdin) providing clock and data sizes\n");
        printf("\n");
        printf(" FUZZFILE should contain new-line separated lines, each containing space separated values\n");
        printf(" the first value `time` advances the clock by time nanoseconds.\n");
        printf(" the second `bytes` appends that many random bytes as a new entry to the queue\n");
        printf(" As the script is played, the random number generator seed, recieved index and byte count\n");
        printf(" are written to a log. This is then re-opened to verify the data in the queue matches.\n");
        
        exit(1);
    }

    // mandatory arguments
    char* dir = argv[optind];
    char* fuzz = argv[optind+1];

    fuzzfid = stdin;
    if (strcmp(fuzz, "-") == 0) {
        fprintf(stderr, "fuzzing from stdin\n");
    } else {
        fuzzfid = fopen(fuzz, "r");
        if (fuzzfid == NULL) { 
            fprintf(stderr, "Unable to open %s\n", fuzz);
            exit(4);
        }
    }

    queue_t* queue = chronicle_init(dir);
    chronicle_set_decoder(queue, &parse_kx, &free_kx);
    chronicle_set_encoder(queue, &sizeof_kx, &append_kx);
    chronicle_set_version(queue, 5);
    chronicle_set_roll_scheme(queue, "FAST_HOURLY");
    chronicle_set_create(queue, 1);
    if (chronicle_open(queue) != 0) exit(-1);
    
    tailer_t* tailer = chronicle_tailer(queue, print_msg, NULL, 0);
    chronicle_peek_queue(queue);

    if (verboseflag) {
        chronicle_debug();
    }
 
    size_t linecap = 0;
    ssize_t linelen = 0;
    char *msgp = NULL;
    char *parsep = NULL;

    int line = 0;
    long long bytes;
    long long time;
    uint64_t clock = 0;
    uint64_t index = 0;

    uint32_t xor_state[4];

    char* tmpfile = strdup("/tmp/shmmain.XXXXXX");
    int tmpfid = c_mkstemp(tmpfile);
    printf("logging fuzz expectations to %s fid %d msg %s\n", tmpfile, tmpfid, strerror(errno));
    FILE* tmp = fdopen(tmpfid, "w+");
    printf(" tmp is %p %s\n", tmp, strerror(errno));
    while ((linelen = getline(&msgp, &linecap, fuzzfid)) > 0) {
        line++;
        parsep = msgp;
        time = strtoll(parsep, &parsep, 0);
        if (*parsep == ' ') parsep++;
        bytes = strtoll(parsep, &parsep, 0);

        printf(" FUZ: %lld millis, %lld bytes\n", time, bytes);
        clock += time;
        xor_state[0] = xor_state[1] = xor_state[2] = xor_state[3] = (uint32_t)line+1;
        K x = ktn(KC, bytes);
        for (long long b = 0; b < bytes; b++) {
            kG(x)[b] = (uint8_t)xorshift128(xor_state);
        }
        index = chronicle_append(queue, x);
        fprintf(tmp, "%" PRIu64 " %lld %lld\n", clock, bytes, index);
        r0(x);
    }
    // we're done parsing input, now replay checking using temp file
    //fclose(tmp);
    //close(tmpfid);

    fflush(tmp);
    if (fseek(tmp, 0L, SEEK_SET) != 0) { printf("abort not fseek"); abort(); };

    int rline = 0;
    int r;
    printf("replay\n");
    do {
        r = fscanf(tmp, "%lld %lld %lld\n", &clock, &bytes, &index);
        if (r == 3) {
            // clock bytes index
            printf(" did %lld %lld %lld\n", clock, bytes, index);

            // TODO: verify!!
        } else if (rline != line) {
            printf ("error, %s r=%d  at line %d!\n\n", tmpfile, r, rline);
        }
        rline++;
    } while (r != EOF);

    fclose(tmp);

    // unlink(tmpfile);
    free(tmpfile);
    if (msgp) free(msgp);
    printf("parse finished\n");

    exit(rline == line ? 0 : 5); // exit with fail

    if (verboseflag) {
        chronicle_debug();
    }

    chronicle_cleanup(queue);

    return 0;
}
