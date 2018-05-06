/**
 * high performance event timer, uses timerfd on linux and kqueue/kevent on BSD/mac
 * achieves timer callback via. the main thread at an interval greater than the built in
 * 1ms KDB Timer.
 */

#include"k.h"
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __APPLE__
#include <sys/event.h>
#elif __linux__
#include <stdint.h>
#include <sys/timerfd.h>
#else

#endif

#define MAX_KX_FD 1024
K klookup_cb[MAX_KX_FD]; // map fd index to callback

K hpet_update(K x, K timespan);

K read_cb(int fd) {

	K rcb = klookup_cb[fd];
	if (rcb == NULL) return (K)NULL;
	// printf("hpet: event fd=%d\n", fd);

#ifdef __linux__
	uint64_t num_expirations = 0;
	ssize_t num_read = 0;
	if((num_read = read(fd, &num_expirations, sizeof(uint64_t))) == -1) {
		sd0(fd);
		return orr("read");
	}
#else
    struct kevent e;
	int res = kevent(fd, 0, 0, &e, 1, 0); // get the event
	if (res <= 0) {
		printf("no event %d?\n", res);
		return krr("no event");
	}
#endif

    // prep args and fire callback
    K msg = ki(0); // don't free this, handed over to q interp
    K arg = knk(1, msg);
    K r = dot(rcb, arg);
	return ki(0);
}

K hpet_open(K cb, K timespan) {
    if (cb->t != 100) return krr("cb is not function");
    if (timespan->t != -KN) return krr("y timespan expected");

    int fd;
#ifdef __APPLE__
    fd = kqueue();
#else
	fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
#endif
	if (fd < 0 || fd >= MAX_KX_FD) {
		close(fd);
		return krr("hpet fd error %d > %d\n"); // TODO memleak
	}
	r1(cb);
	klookup_cb[fd] = cb;
	hpet_update(ki(fd), timespan);
	return sd1(fd, &read_cb); // does this return fd?
}

K hpet_close(K x) {
    if (x->t != -KI) return krr("x fd is not int");
	int fd = x->i;

	if (fd < 0 || fd >= MAX_KX_FD) {
		close(fd);
		return krr("hpet fd error %d > %d\n"); // TODO memleak
	}
	printf("hpet: closing timer fd %d\n", fd);

	K rcb = klookup_cb[fd];
	if(rcb == NULL)	return krr("not started");

	sd0(fd); // unlink from selector and close(fd)
	r0(rcb); // unref callback
	klookup_cb[fd] = NULL;
	return ki(fd);
}

K hpet_update(K x, K timespan) {
	if (x->t != -KI) return krr("x fd is not int");
	int fd = x->i;
	K rcb = klookup_cb[fd];
	if(rcb == NULL)	return krr((S) "x fd not started");

    if (timespan->t != -KN) return krr("y timespan expected");

    printf("hpet: setting fd %d timespan to %lluns\n", fd, timespan->j);

#ifdef __APPLE__
	struct kevent e;
    EV_SET(&e, 1, EVFILT_TIMER, EV_ADD | EV_ENABLE, NOTE_NSECONDS, timespan->j, 0);
    if (kevent(fd, &e, 1, 0, 0, 0) != 0) {
		orr("hpet update kevent error");
	}
#else
	struct itimerspec newtimer;
	newtimer.it_interval.tv_sec = (timespan->j) / (J)1e9;
	newtimer.it_interval.tv_nsec = timespan->j%(J)1e9;
	newtimer.it_value.tv_sec = (timespan->j) / (J)1e9;
	newtimer.it_value.tv_nsec = timespan->j%(J)1e9;
	if (timerfd_settime(fd, 0, &newtimer, NULL) != 0) {
		orr("hpet update error");
	}
#endif
	return (K) 0;
}
