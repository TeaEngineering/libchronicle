\l native/demo/shm.q

// Recovery logic
.pos:0

.shmipc.init[`:java/queueout;`kx];
rcb:{.pos:y}
.shmipc.tailer[`:java/queueout;rcb;0];
.shmipc.peek[] // replay the outbound log in a "blocking" style

show " " sv ("recovered position is";string .pos)
.shmipc.close[`:java/queueout] // kill recovery tailer

// Regular application here
.shmipc.init[`:java/queue;`text];   // input
.shmipc.init[`:java/queueout;`kx];  // output

// handler for new input pushes corresponding index to output
cb:{ 0N!(x;y);.shmipc.append[`:java/queueout;x]; }

fd:.timer.hpet_open[{.shmipc.peek[]}; 0D00:00:00.010000000];
.shmipc.tailer[`:java/queue;cb;.pos]
