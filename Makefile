
# k.h from https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h
# apt-get install libcurl4-openssl-dev

detected_OS := $(shell uname -s)

IDIR=.
CC=gcc
CFLAGS=-DKXVER=3 -fPIC -I$(IDIR) -shared
ifeq ($(detected_OS),Darwin)  # Mac OS X
    CFLAGS += -undefined dynamic_lookup
endif
ifeq ($(detected_OS),Linux)
    CFLAGS += -std=gnu99
endif

ODIR=obj
LIBS=-lm
DEPS = k.h wire.h

all: obj/cpu.so obj/hpet.so obj/shmipc.so obj/shmmain

$(ODIR)/%.so: %.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS)

$(ODIR)/shmmain: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< -DKXVER=3 -g -fPIC -I$(IDIR)

.PHONY: clean

clean:
	rm -f $(ODIR)/*.so *~ core $(INCDIR)/*~
