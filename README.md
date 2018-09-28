
## Shared-memory communication using Chronicle's chronocle-queue protocol

[OpenHFT](https://github.com/OpenHFT) aka. [Chronicle Software Ltd.](https://chronicle.software/) provide an open-source Java '[Chronocle Queue](https://github.com/OpenHFT/Chronicle-Queue)' ipc library. This project is an unaffiliated, mostly-compatible, open source implementation in the C-programming language, with bindings to other non-JVM languages. To differentiate I refer to OpenHFTs implementation capitalised as 'Chronicle Queue', and the underlying procotol itself chronicle-queue, and this implementation as `libchronicle`.

### Documenting the chronicale-queue format
There is no controlling broker process, the operating system provides persistence and hardware itself provides arbitration. Messages always flow from an 'appender' to a 'tailer' process, in one direction with no flow control. The queue is fully contained with a standard directory, which should be otherwise empty.
'appender' publishes payload to a queue directory, is given 64bit value 'index' of the write
'tailer' subscribes to messages in a queue directory, starting from 0 or provided index
Multiple appenders, multiple tailers supported, can be added and removed at will so long as all on
same machine. All writes resolve into total order which is preserved on replay. Message length must
pack into 30 bits. Typical IPC latency 1us, depending on frequency of shmipc.peek

Settings control how the 64-bit index value is interpreted, the typical scheme is upper 32 bits
are 'cycle', and bottom 32 bits are the 'seqnum'. Cycle values map to a particular queue file
on disk, e.g. cycle+1970.01.01 -> yyyymmdd.cq4 . seqnum is the message position within the file.
Queue file writers maintain an index structure within the queue file to allow resuming from a
particular seqnum value with a reasonable upper bound on the number of disk seeks. Appenders
sample the clock during a write to determine if the current cycle should be 'rolled' into the
next, which sets the seqnum to zero and increments cycle, switching the filename in use.
Queue files are machine independant and can be moved between machines.

For performance and correctness, the queue files must be memory mapped. Kernel guarantees
maps into process address space for the same file and offset and made with MAP_SHARED by
multiple processes are mapped to the same physical pages, which allows zero-copy communication.
Memory subsystem ensures correctness between CPUs and packages. To bound the mmap() to sensible
sizes, the file is mapped in chunks of 'blocksize' bytes. If a payload is to be written or
deserialised larger than blocksize, it is extended and the mapping rebuilt. The kernel arranges
dirty pages to be written to disk. Blocking I/O using read() and write() may see stale data
however filesystem tools (cp) now typically use mmap.

Messages are written to the queue with a four-byte header, containing the message size and two control
bit. Writers arbitrate using compare-and-set operations (lock; cmpxchgl) on these four bytes to
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

The `directory-listing.cq4t` file is a simple counter of the min and max cycles, which is used
to avoid probing disk execessively during a reply and around the dateroll. It is memory-mapped to
allow tailers to see cycle rolls in real time.

## Bindings

This repository contains command line C utilities, as well as language bindings (.so+.q) for kdb.
Planned:
- python
- nodejs
