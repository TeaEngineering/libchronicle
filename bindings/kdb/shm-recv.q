\l shm.q

.shmipc.init[`:java/queue;`text;5;0b];

fd:.timer.hpet_open[{.shmipc.peek[]}; 0D00:00:00.010000000];

cb:{0N!(x;y)}
.shmipc.tailer[`:java/queue;cb;0];
