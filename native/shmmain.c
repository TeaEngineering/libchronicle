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

#include <libchronicle.h>
#include <wire.h>
#include <stdarg.h>
#include <ctype.h>

// This is a stand-alone tool for replaying a queue, and optionally writing to it.
// It is compatible with java InputMain / OutputMain, ie. the data payloads are
// wire-format encoded. we use wire.h to encode/decode this.
int print_msg(void* ctx, uint64_t index, COBJ y) {
    printf("[%" PRIu64 "] %s\n", index, (char*)y);
    free(y);
    return 0;
}

int main(const int argc, char **argv) {
    int c;
    opterr = 0;
    int verboseflag = 0;
    int followflag = 0;
    char* append = NULL;
    uint64_t index = 0;
    int queueflags = 0;

    while ((c = getopt(argc, argv, "i:va:cf")) != -1)
    switch (c) {
        case 'i':
            index = strtoull(optarg, NULL, 0);
            break;
        case 'v':
            verboseflag = 1;
            break;
        case 'a':
            append = optarg;
            break;
        case 'c':
            //queueflags = queueflags | SHMIPC_INIT_CREATE;
            queueflags = queueflags + 1;
            break;
        case 'f':
            followflag = 1;
            break;
        case '?':
            fprintf(stderr, "Option '-%c' missing argument\n", optopt);
            exit(2);
        default:
            fprintf(stderr, "Unknown option '%c'\n", c);
            exit(3);
    }

    if (optind + 1 > argc) {
        printf("Missing mandatory argument.\n Expected: %s [-d] [-m] [-i INDEX] [-v] [-a text] [-f] QUEUE\n", argv[0]);
        printf("  -i INDEX resume from index\n");
        printf("  -v verbose mode\n");
        printf("  -a TEXT write value text\n");
        printf("  -c create QUEUE directory and files if it does not exist\n");
        printf("  -f follow queue for more entries (rather than exit)\n");
        printf("  -4 -5 set expected version");
        printf("\n");
        printf("shmmain opens the chronicle-queue directory QUEUE and plays all messages from INDEX\n");
        printf("adding -c allows the queue to be created (and any parent directories) if it does not exist.\n");
        printf("setting -d -m -v vary the amount of printing that occurs during this\n");
        printf("once the end of the queue is reached, if append (-a TEXT) is set, we will write a new\n");
        printf("message containing the value TEXT.\n");
        printf("if follow (-f) is set, will we continue to poll for (and print) new messages, else exit.\n");
        exit(1);
    }

    char* dir = argv[optind];
    queue_t* queue = chronicle_init(dir);
    chronicle_set_encoder(queue, &wirepad_sizeof, &wirepad_write);
    chronicle_set_decoder(queue, &wire_parse_textonly, &free);

    if (chronicle_open(queue) != 0) {
        printf("failed to open %s", chronicle_strerror());
        exit(-1);
    }

    wirepad_t* pad = wirepad_init(1024);

    chronicle_tailer(queue, &print_msg, NULL, index);
    chronicle_peek();

    if (append) {
        printf("writing %s\n", append);
        wirepad_clear(pad);
        wirepad_text(pad, append);
        chronicle_append(queue, pad);
    }

    while (followflag) {
        usleep(500*1000);
        chronicle_peek();
    }

    if (verboseflag) chronicle_debug();

    chronicle_cleanup(queue);
    return 0;
}
