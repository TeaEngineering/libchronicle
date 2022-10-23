
## Shared-memory communication using OpenHFT's chronicle-queue protocol

[OpenHFT](https://github.com/OpenHFT) aka. [Chronicle Software Ltd.](https://chronicle.software/) provide an open-source Java '[Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue)' ipc library. [This project](https://github.com/TeaEngineering/libchronicle) is an unaffiliated, mostly-compatible, open source implementation in the C-programming language, with bindings to other non-JVM languages. To differentiate I refer to OpenHFTs implementation capitalised as 'Chronicle Queue', and the underlying procotol itself chronicle-queue, and this implementation as `libchronicle`.

## Getting started

See more examples in [python](bindings/python/) or [kdb](bindings/kdb/).

Our C example is in two parts. The writing side is [`native/shm_example_writer.c`](native/shm_example_writer.c) and can be built by `make`:

    $ git clone git@github.com:TeaEngineering/libchronicle.git
    $ cd libchronicle/native
    $ make
    $ mkdir /tmp/testq
    $ ./obj/shm_example_writer /tmp/testq

```C
#include <libchronicle.h>
#include <stdarg.h>

void append_msg(unsigned char* base, void* msg, size_t sz) {
    memcpy(base, msg, sz);
}

size_t sizeof_msg(void* msg) {
    return strlen(msg);
}

int main(const int argc, char **argv) {
    queue_t* queue = chronicle_init(argv[1]);
    chronicle_set_encoder(queue, &sizeof_msg, &append_msg);
    chronicle_set_version(queue, 5);
    chronicle_set_roll_scheme(queue, "FAST_HOURLY");
    chronicle_set_create(queue, 1);
    if (chronicle_open(queue) != 0) exit(-1);

    char line[1024];
    while (1) {
        char* g = fgets(line, 1024, stdin);
        if (g == NULL) break;
        line[strlen(line) - 1] = 0; // remove line break
        long int index = chronicle_append(queue, line);
        printf("[%" PRIu64 "] %s\n", index, (char*)g);
    }
    chronicle_cleanup(queue);
}
```

Running this code creates a new v5 queue in the directory specified using `FAST_HOURLY` rollover, then writes any text input from stdin as messages.

The reading side is similar, invoked as `$ ./obj/shm_example_reader /tmp/testq` with source [`shm_example_reader.c`](native/shm_example_reader.c):

```C
#include <libchronicle.h>
#include <stdarg.h>
#include <signal.h>

static volatile int keepRunning = 1;

void* parse_msg(unsigned char* base, int lim) {
    char* msg = calloc(1, lim+1);
    memcpy(msg, base, lim);
    return msg;
}

int print_msg(void* ctx, uint64_t index, void* msg) {
    printf("[%" PRIu64 "] %s\n", index, (char*)msg);
    return 0;
}

void sigint_handler(int dummy) {
    keepRunning = 0;
}

int main(const int argc, char **argv) {
    signal(SIGINT, sigint_handler);
    queue_t* queue = chronicle_init(argv[1]);
    chronicle_set_decoder(queue, &parse_msg, &free);
    if (chronicle_open(queue) != 0) exit(-1);
    chronicle_tailer(queue, &print_msg, NULL, 0);

    while (keepRunning) {
        usleep(500*1000);
        chronicle_peek();
    }
    printf("exiting\n");
    chronicle_cleanup(queue);
}
```

## Documenting the chronicle-queue format
The format of the chronicle-queue files containing your data, the shared memory protocol, and the safe ordering of queue file maintenance is currently implementation defined. One hope is that this project will change that!

A chronicle-queue has no single controlling broker process, the operating system provides persistence and hardware itself provides arbitration. Messages always flow from an 'appender' to a 'tailer' process, in one direction, with no flow control. The queue is fully contained with a standard file system directory, which should be otherwise empty. Queue files are machine independant and can be moved between machines.

* An _appender_ process writes messages to a queue directory, is given 64bit value 'index' of the write
* Whilst a _tailer_ subscribes to messages in a queue directory, starting from 0 or provided index

Multiple appenders, multiple tailers are supported, can be added and removed at will so long as all on
the same machine. All writes resolve into total order which is preserved on replay. Message length must
be positive and pack into 30 bits, so the maximum size of any individually sequenced payload is `(1<<31)-1` bytes, or 1023 Mb. Typical IPC latency 1us, depending on frequency of polling.

Each persisted message is identified by a sequential 64-bit index value. Settings control how the 64-bit index value is interpreted, and Chronicle Queue ships with several built-in schemes (`RollCycles`) that split the 64-bit value into two parts at a particular bit position. Upper bits are known as the 'cycle' and the remainder are the 'seqnum'. Cycle values map to a particular queuefile on disk, whilst seqnum indexes the data message within the file.

The popular _DAILY_ scheme splits the 64-bit index in half, into upper 32 bit cycle and bottom 32 bit are 'seqnum'. The filenames are derived as  `cycle+1970.01.01` formatted as `${yyyymmdd}.cq4`. It is critical for the writers and readers to agree on the scheme in use, therefore the scheme is written to the header in each queuefile.

Queue file writers maintain a double-index structure (also stored within the queue file) to allow resuming from a
particular seqnum value with a reasonable upper bound on the number of disk seeks. The first Metadata message is an array of byte positions within the file of subsequent index pages. Each of those is a Metadata message containing an array (of the same length) containing byte positions of future data messages. The _DAILY_ scheme indexes every 64th message and has 8000 entries per index page. Note that the RollCycle configures the indexing layout.

Appenders sample the clock during a write to determine if the current cycle should be 'rolled' into the
next. Changing cycle clears seqnum to zero, which causes the index value to "jump" correspondingly and switches the filename in use. A jump to the next cycle also occurs when the indexing structure is full, ie. all entries in the root index point to index pages that are themselves full.

For performance and correctness, the queue files must be memory mapped. Kernel guarantees
maps into process address space for the same file and offset and made with `MAP_SHARED` by
multiple processes are mapped to the same _physical pages_, which allows shared memory primitives to be used for inter-process communication.

Memory subsystem ensures correctness between CPUs and packages. To bound the `mmap()` to sensible
sizes, the file is mapped in chunks of 'blocksize' bytes. If a payload is to be written or
deserialised larger than blocksize, blocksize is doubled and the mmap mapping rebuilt. The kernel arranges
dirty pages to be written to disk. Blocking I/O using `read()` and `write()` may see stale data
however filesystem tools (e.g. `cp`) now typically use mmap, so a live copy may be taken.

Messages are written to the queue with a four-byte header, containing the message size and two control
bits. Writers arbitrate using compare-and-set operations (`lock; cmpxchgl`) on these four bytes to
determine who takes the write lock:

    bits[0-29] 30  31  meaning                    shmipc.c constant
      0        0   0   available / unallocated    HD_UNALLOCATED 0x00000000
      size     0   0   data payload
      size     1   0   metadata                   HD_METADATA    0x40000000
      pid      0   1   working                    HD_WORKING     0x80000000
      0        1   1   end-of-file                HD_EOF         0xC0000000

Performing `lock_cmpxchgl(&header, HD_UNALLOCATED, HD_WORKING)` takes the write lock. Data is then
written and header is re-written with the size and working bit clear. x86 (64) preserves ordering
of writes through to read visibility. Tailers need to use an 'mfence' between reading the header
and payload to ensure payload is not prematurely fetched and decoded before the working signal
is clear. mfence used in this way stops both compiler re-ordering and cpu prefetch.

The `directory-listing.cq4t` file (v4) or `metadata.cq4t` file (v5) is a simple counter of the min and max cycles, which is used
to avoid probing the file system execessively with `stat` system calls during a reply and around the dateroll. It is memory-mapped into all appenders and tailers to see cycle rolls in real time.

## Bindings

This repository contains command line C utilities, as well as language bindings for `libchronicle.so` for:
- [python](bindings/python/)
- [kdb](bindings/kdb/)

Planned:
- nodejs

## Development and Compatability Status

Queue creation and rollover is implemented for v4 and v5 queues, plus all of the standard roll schemes.
So far as possible our reader and writer examples are compatable with the `InputMain` and `OutputMain` exmaples provided by Chronicle Software Ltd.

The main missing feature is reading/writing the index structures maintained in the metadata pages. This allows joining an existing queue to be done very cheaply.

In order to read and write queue metadata, we have a fairly complete implementation of the 'chroicle-wire' serialisation format, implemented by `wire.c` and `wire.h` that might be useful if the data in your queue is written using that format.

If you can reproduce a segfault on an otherwise valid queuefile, examples would be happily recieved via. a Github Issue.

## Unit tests

To build the test suite first install [cmocka](https://cmocka.org/).

    $ sudo apt install libcmocka-dev
    $ cd native
    $ make test

Some of the test cases use queues written by Chronicle Queue in Java, using test data written by [these Java files](java/).

## Fuzzer & Coverage
There is basic code coverage reporting using `clang`'s coverage suite, tested on a Mac. This uses shmmain to read and write some entries and shows line by line coverage. Try:

    cd native
    make coverage

There is a very basic automated fuzzing tool (using [AFL - American Fuzzy Lop](http://lcamtuf.coredump.cx/afl/)), which follows a fuzzing script to read and write deterministic payload for a given number of bytes, then jumps the clock by an  amount, repeatedly. It writes the indices and seeds for the values written. The tool under test then replays the queuefiles and expects the same payload length, index and payload data to be output.

     cd native
     make fuzzer

The fuzzing runs quite slowly, due to disk I/O, and also most of AFLs attempts to modify the fuzzing instruction file are invalid. Looks like changing it to a binary rather than ASCII file would help a great deal.

