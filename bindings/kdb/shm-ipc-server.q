\l shm.q
\l hpet.q

\p 12345

.shmipc.init[`:java/in;`kx;5;1b];
.shmipc.init[`:java/out;`kx;5;1b];
fd:.timer.hpet_open[{do[100;.shmipc.peek[]];}; 0D00:00:00.000001000];
cb:{.shmipc.append[`:java/out;(x;value y)];}
.shmipc.tailer[`:java/in;cb;0];
