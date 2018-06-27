\l shm.q

.shmipc.init[`:java/in;`kx];
.shmipc.init[`:java/out;`kx];

/ fd:.timer.hpet_open[{.shmipc.peek[]}; 0D00:00:00.010000000];
hi:.shmipc.tailer[`:java/out;{[x;y]};0];

shmquery:{[x]
  wr:.shmipc.append[`:java/in;x];
  tmp:(-1;(0;0)); / (outseq;(inseq;result));
  while[tmp[1][0]<>wr;tmp:.shmipc.collect[hi]];
  :tmp[1][1]
 };

shmquery["0"]; / round trip to create queuefiles etc

/shmquery["4+12"];
n:1000;
\t do[n;shmquery["12+34"]];

/h: hopen `::12345;
/ \t do[n;h"12+34"];

