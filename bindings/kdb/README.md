
Bindings for KDB/Q
===

Build/install
---

First build and install the libary .so and headers (libchronicle.so and libchronicle.h) by running

    $ cd native
    $ make
    $ sudo make install

The `Makefile` in this directory assumes this has been done to be able to build. Next run 

    $ make

Which builds `obj/hpet.so` and `obj/shmipc.so`. These need to be copied into the KDB library directory (e.g. `l64`) or the full path specied the shm.q to work.

If you dont have permission to write to global libs directories, use `PREFIX=~/usr make install` then  customise the Makefile to use your local libs and include directories.

Examples
---

Example `shm-sender.q` writes the start time of the process to a queue every second. `shm-recv.q` will tail this queue and output each value. To show guaranteed delivery, `shm-recv-once.q` uses a second queue to record where in the first queue it reached:

* shm-recv.q
* shm-recv-once.q
* shm-sender.q

The second example shows a pair of queues used to push IPC commands to a server, input is in one queue and the output to another:

* shm-ipc-client.q
* shm-ipc.server.q

Old example
---

    / add a tailer by using .shmipc.tailer[`:queue;cb;cycle;decoder] where cycle may be 0 to replay
    / from the beginning of time, and cb is the callback for each event in the queue. A replay
    / occurs 'inline' and any new records are dispatched by calls to .peek[]
    cb:{0N!(x;y)}
    .shmipc.tailer[`:java/queue;cb;0];
    .shmipc.debug[];

    / Note the standard Java wire implementations are largely ignored and returned as byte arrays
    / for debug tracing $ export SHMIPC_DEBUG=1 && ./q.sh native/shmipc.q

    / add a souce by .shmipc.appender[`:queue;data]
    / multiple appenders may write to the same queue, but there can be no more than one appender per
    / process. this is because file locks are used to synchronise some operations and these are issued
    / at the 'pid' level.
    .shmipc.appender[`:java/queue;"message"];

    // tailers only advance on calls to poll[], wheres appenders advance on every call to appenders
