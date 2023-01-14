
.shmipc.init2:`:native/obj/shmipc 2:(`shmipc_init;4)
.shmipc.close:`:native/obj/shmipc 2:(`shmipc_close;1)
.shmipc.peek:`:native/obj/shmipc 2:(`shmipc_peek;1)
.shmipc.tailer:`:native/obj/shmipc 2:(`shmipc_tailer;3)
.shmipc.append_ts:`:native/obj/shmipc 2:(`shmipc_append_ts;3)
.shmipc.append_raw:`:native/obj/shmipc 2:(`shmipc_append;2)
.shmipc.debug:`:native/obj/shmipc 2:(`shmipc_debug;1)

.shmipc.dto:`long$2000.01.01D00:00:00.000-1970.01.01D00:00:00.000
.shmipc.ctmillis:{(.shmipc.dto+`long$.z.p) div 1000000}

.shmipc.init:{[dir;fmt;ver;create]
  // check if the queue directory exists, create if missing
  system " " sv ("mkdir";"-p";1_string dir);
  .shmipc.init2[dir;fmt;ver;create]
 }

.shmipc.append:{[dir;msg]
  .shmipc.append_ts[dir;msg;.shmipc.ctmillis[]]
 }
