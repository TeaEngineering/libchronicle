// Shared memory IPC based on OpenHFT's Chronocle Queue format
// 'appender' publishes payload to a queue directory, is given 64bit value 'index' of the write
// 'tailer' subscribes to messages in a queue directory, starting from 0 or higher than a provided index
// Multiple appenders, multiple tailers supported, can be added and removed at will so long as all on
// same machine. Alll writes resolve into total order which is preserved on replay. Message length must
// pack into 30 bits.
// Typical IPC latency typically 1us, depending on frequency of shmipc.peek
//
// Settings control how the 64-bit index value is interpreted, the typical scheme is upper 32 bits
// are 'cycle', and bottom 32 bits are the 'seqnum'. Cycle values map to a particular queue file
// on disk, e.g. cycle+1970.01.01 -> yyyymmdd.cq4 . seqnum is the message position within the file.
// Queue file writers maintain an index structure within the queue file to allow resuming from a
// particular seqnum value with a reasonable upper bound on the number of disk seeks. Appenders
// sample the clock during a write to determine if the current cycle should be 'rolled' into the
// next, which sets the seqnum to zero and increments cycle, switching the filename in use.
// Queue files are machine independant and can be moved between machines.
//
// For performance and correctness, the queue files must be memory mapped. Kernel guarantees
// maps into process address space for the same fid and offset and made with MAP_SHARED by
// multiple processes are mapped to the same physical pages, which allows zero-copy communication.
// Memory subsystem ensures correctness between CPUs and packages. To bound the mmap() to sensible
// sizes, the file is mapped in chunks of 'blocksize' bytes. If a payload is to be written or
// deserialised larger than blocksize, it is extended and the mapping rebuilt. The kernel arranges
// dirty pages to be written to disk. Blocking I/O using read() and write() may see stale data
// however filesystem tools (cp) typically use mmap now.
//
// Messages are written to the queue with a four-byte header, containing the message size and two control
// bit. Writers arbitrate using compare-and-set operations (lock; cmpxchgl) on these four bytes to
// determine who takes the write lock:
//
//  bits [0-29]  30  31   meaning                    shmipc.c constant
//       0        0   0   available / unallocated    HD_UNALLOCATED 0x00000000
//       size     0   0   data payload
//       size     1   0   metadata                   HD_METADATA    0x40000000
//       pid      0   1   working                    HD_WORKING     0x80000000
//       0        1   1   end-of-file                HD_EOF         0xC0000000
//
// Performing lock_cmpxchgl(&header, HD_UNALLOCATED, HD_WORKING) takes the write lock. Data is then
// written and header is re-written with the size and working bit clear. x86 (64) preserves ordering
// of writes through to read visibility. Tailers need to use an 'mfence' between reading the header
// and payload to ensure payload is not prematurely fetched and decoded before the working signal
// is clear. mfence used in this way stops both compiler re-ordering and cpu prefetch.
//
// The directory-listing.cq4t file is a simple counter of the min and max cycles, which is used
// to avoid probing disk execessively during a reply and around the dateroll. It is memory-mapped to
// allow tailers to see cycle rolls in real time.
//

.shmipc.init2:`:native/obj/shmipc 2:(`shmipc_init;2)
.shmipc.close:`:native/obj/shmipc 2:(`shmipc_close;1)
.shmipc.peek:`:native/obj/shmipc 2:(`shmipc_peek;1)
.shmipc.tailer:`:native/obj/shmipc 2:(`shmipc_tailer;3)
.shmipc.append_ts:`:native/obj/shmipc 2:(`shmipc_append_ts;3)
.shmipc.append_raw:`:native/obj/shmipc 2:(`shmipc_append;2)
.shmipc.debug:`:native/obj/shmipc 2:(`shmipc_debug;1)

.timer.hpet_open:`:native/obj/hpet 2:(`hpet_open;2)

.shmipc.dto:`long$2000.01.01D00:00:00.000-1970.01.01D00:00:00.000
.shmipc.ctmillis:{(.shmipc.dto+`long$.z.p) div 1000000}

.shmipc.init:{[dir;fmt]
  // check if the queue directory exists, create if missing
  system " " sv ("mkdir";"-p";1_string dir);

  cycle:`int$.z.d-`int$1970.01.01;
  qffn:(string .z.d)[0 1 2 3 5 6 8 9],".cq4";

  dlsz:65536;
  dl:` sv (dir;`$"directory-listing.cq4t");
  if[(not exists dl) | @[hcount;dl;-1] < dlsz;
    /-1 " " sv ("shmipc: creating dirlist";string dl);
    // dlist:read1[(`$":native/test/shm001/directory-listing.cq4t";0;512)]
    dlist:0x6c000040b906686561646572b607535453746f72658256000000c87769726554797065b6085769726554797065ec42494e4152595f4c49474854c87265636f76657279b61254696d656453746f72655265636f766572798214000000c974696d655374616d708fa7000000000000000024000000b9146c697374696e672e686967686573744379636c658e00000000a7104500000000000024000000b9136c697374696e672e6c6f776573744379636c658e0100000000a7094500000000000024000000b9156c697374696e672e6578636c75736976654c6f636b8f8f8f8fa700000000000000001c000000b9106c697374696e672e6d6f64436f756e748fa711;
    outf:dlist,(dlsz-count dlist)#0x00;
    dl 1: outf;
  ];

  qf:` sv (dir;`$"20180529.cq4");
  if[(not exists qf) | @[hcount;qf;-1] < 80000;
    /-1 " " sv ("shmipc: creating queue";string qf);
    // qfh:read1[(`$":native/test/shm002/20180605.cq4";0;496)]
    qfh:0xb6010040b906686561646572b60853435153746f7265829f010000c87769726554797065b6085769726554797065ec42494e4152595f4c494748548e00000000cd7772697465506f736974696f6e8f8d0200000000000000020000000000000000020200000000000000000000020200c4726f6c6cb60853435153526f6c6c8223000000c66c656e677468a6005c2605c6666f726d6174e8797979794d4d6464c565706f636800c8696e646578696e67b60c53435153496e646578696e67824d000000ca696e646578436f756e74a50020cc696e64657853706163696e6740cb696e64657832496e6465788f8f8f8fa7ba01000000000000c96c617374496e6465788e00000000a74000000000000000df6c61737441636b6e6f776c6564676564496e6465785265706c6963617465648e020000000000a7ffffffffffffffffc87265636f76657279b61254696d656453746f72655265636f766572798216000000c974696d655374616d708f8f8fa70000000000000000d764656c7461436865636b706f696e74496e74657276616c00d36c617374496e6465785265706c6963617465648f8fa7ffffffffffffffffc8736f7572636549640022000140b90b696e64657832696e6465788f8f8f8f8d00200000000000000100000000000000e00101;
    //qfi:read1[(`$":native/test/shm002/20180605.cq4";66016;48)]
    qfi:0x1c000140b905696e6465788f8f8f8f8d0020000000000000010000000000000000020200000000000000000000000000;
    // data all zeros starts from 0x00020200
    qfsz:83754496; / 0x5000000-0x00020200, 80mb
    data:0x0500000068656c6c6f; / "hello"
    outf:qfh,((66016-count qfh)#0x00),qfi,((65568-count qfi)#0x00),data,((qfsz-count data)#0x00);
    qf 1: outf;
  ];

  .shmipc.init2[dir;fmt]
 }

.shmipc.append:{[dir;msg]
  .shmipc.append_ts[dir;msg;.shmipc.ctmillis[]]
 }
