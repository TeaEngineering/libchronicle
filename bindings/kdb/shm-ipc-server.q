\l shm.q
\l timer.q

\p 12345

.shmipc.init[`:java/in;`kx];
.shmipc.init[`:java/out;`kx];
fd:.timer.hpet_open[{do[100;.shmipc.peek[]];}; 0D00:00:00.000001000];
cb:{.shmipc.append[`:java/out;(x;value y)];}
.shmipc.tailer[`:java/in;cb;0];
