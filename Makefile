
# k.h from https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h
# apt-get install libcurl4-openssl-dev

detected_OS := $(shell uname -s)

IDIR=.
CC=gcc
CFLAGS=-DKXVER=3 -fPIC -I$(IDIR)
CDFLAGS=-shared
ifeq ($(detected_OS),Darwin)  # Mac OS X
    CDFLAGS += -undefined dynamic_lookup
endif
ifeq ($(detected_OS),Linux)
    CFLAGS += -std=gnu99
endif

ODIR=obj
LIBS=-lm
DEPS = k.h wire.h

all: obj/cpu.so obj/hpet.so obj/shmipc.so obj/shmmain

$(ODIR)/%.so: %.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

$(ODIR)/shmmain: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

coverage: obj/shmcov
	obj/shmcov -v -a WORLD :demo/stress
	/Library/Developer/CommandLineTools/usr/bin/llvm-profdata merge default.profraw -output=default.profout
	/Library/Developer/CommandLineTools/usr/bin/llvm-cov show -instr-profile default.profout obj/shmcov

$(ODIR)/shmcov: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -fprofile-instr-generate -fcoverage-mapping -O0 -g

grind: $(ODIR)/shmmain
	valgrind --track-origins=yes --leak-check=full $(ODIR)/shmmain :../java/out

.PHONY: clean grind coverage

clean:
	rm -f *~ core $(INCDIR)/*~ default.prof*
	rm -Rf $(ODIR)/*
