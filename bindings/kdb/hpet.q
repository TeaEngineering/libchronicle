/ High performance event timer
/ uses timerfd on linux or kevent/kqueue on BSD/Mac

.timer.hpet_open:`:native/obj/hpet 2:(`hpet_open;2)
.timer.hpet_update:`:native/obj/hpet 2:(`hpet_update;2)
.timer.hpet_close:`:native/obj/hpet 2:(`hpet_close;1)

cb:{show .z.p;}

// print every 0.5s
fd:.timer.hpet_open[cb; 0D00:00:00.500000000];
.timer.hpet_update[fd; 0D00:00:00.250000000];

// stop printing timer after 5 seconds
cb2:{.timer.hpet_close[fd];.timer.hpet_close[fd2];}
fd2:.timer.hpet_open[cb2;0D00:00:05.000000000];

// exit after 10 seconds
cb3:{exit 42;}
fd3:.timer.hpet_open[cb3; 0D00:00:10.000000000];


