
# k.h from https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h
# apt-get install libcurl4-openssl-dev

detected_OS := $(shell uname -s)

IDIR=.
CC=gcc
CFLAGS=-fPIC -I$(IDIR) -Wall
CDFLAGS=-shared
ifeq ($(detected_OS),Darwin)  # Mac OS X
    CDFLAGS += -undefined dynamic_lookup
endif
ifeq ($(detected_OS),Linux)
    CFLAGS += -std=gnu99
endif

LIBS=-lm
DEPS=wire.h libchronicle.h k.h

all: obj/hpet.so obj/shmipc.so

k.h:
	wget https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h

obj/hpet.so: hpet.c k.h 
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

obj/shmipc.so: shmipc.c k.h
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

syms:
	nm -D ../../kdb/l64/q | grep " T "

.PHONY: clean syms

clean:
	rm -f *~ core $(INCDIR)/*~ default.prof*
	rm -Rf $(ODIR)/*
