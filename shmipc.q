
.shmipc.init:`:native/obj/shmipc 2:(`shmipc_init;2)
.shmipc.close:`:native/obj/shmipc 2:(`shmipc_close;1)
.shmipc.peek:`:native/obj/shmipc 2:(`shmipc_peek;1)
.shmipc.tailer:`:native/obj/shmipc 2:(`shmipc_tailer;3)
.shmipc.appender:`:native/obj/shmipc 2:(`shmipc_appender;2)
.shmipc.debug:`:native/obj/shmipc 2:(`shmipc_debug;1)

.timer.hpet_open:`:native/obj/hpet 2:(`hpet_open;2)

/ opening an appender or tailer first need watchers on the queue directory and the most
/ recent data file to be opened. The initial layout needs to be created by a Java
/ process.
/ decoder type can be `text`bytes`kdb
n:(0 1000)[system["hostname"][0] like "*Chris*"]
do[n;.shmipc.init[`:java/queue;`text];.shmipc.debug[0];.shmipc.peek[0];.shmipc.debug[0];.shmipc.close[`:java/queue];];
.shmipc.debug[];

// fd:.timer.hpet_open[{.shmipc.peek[0]}; 0D00:00:00.500000000];
.shmipc.init[`:java/queue;`text];
.shmipc.peek[];
.shmipc.debug[];

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

// To debug wire.h protocol parsers
// $ export SHMIPC_WIRETRACE=1
// $ unset SHMIPC_WIRETRACE
// $ ./q.sh native/shmipc.q
