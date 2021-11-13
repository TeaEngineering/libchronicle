
# k.h from https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h
# apt-get install libcurl4-openssl-dev

detected_OS := $(shell uname -s)

IDIR=.
CC=gcc
CFLAGS=libchronicle.c -fPIC -I$(IDIR) -Wall
CDFLAGS=-shared
ifeq ($(detected_OS),Darwin)  # Mac OS X
    CDFLAGS += -undefined dynamic_lookup
endif
ifeq ($(detected_OS),Linux)
    CFLAGS += -std=gnu99
endif

ODIR=obj
LIBS=-lm
DEPS=k.h wire.h libchronicle.h libchronicle.c

tests := $(patsubst %.c,%.to,$(wildcard test/test*.c))
testsok := $(patsubst %.c,%.tok,$(wildcard test/test*.c))
all: obj/hpet.so obj/shmmain obj/shmexample obj/shmipc.so $(tests)

k.h:
	wget https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h

$(ODIR)/%.so: %.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

%.to: %.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -lcmocka

%.tok: %.to
	$< && touch $@

$(ODIR)/fuzzmain: fuzzmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

$(ODIR)/shmmain: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

$(ODIR)/shmexample: shmexample.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

coverage: obj/shmcov
	obj/shmcov -v -a WORLD :demo/stress
	/Library/Developer/CommandLineTools/usr/bin/llvm-profdata merge default.profraw -output=default.profout
	/Library/Developer/CommandLineTools/usr/bin/llvm-cov show -instr-profile default.profout obj/shmcov

$(ODIR)/shmcov: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -fprofile-instr-generate -fcoverage-mapping -O0 -g

grind: $(ODIR)/shmmain
	valgrind --track-origins=yes --leak-check=full $(ODIR)/shmmain :../java/out

syms:
	nm -D ../../kdb/l64/q | grep " T "

fuzz: $(ODIR)/shmmain.fuzz
	# brew install afl-fuzz
	mkdir -p test/fuzz_output
	afl-fuzz -i test/fuzz_input -o test/fuzz_output $(ODIR)/shmmain.fuzz -F - :../java/out

$(ODIR)/shmmain.fuzz: shmmain.c shmipc.c mock_k.h $(DEPS)
	/usr/local/Cellar/afl-fuzz/2.52b/bin/afl-clang -o $@ $< $(CFLAGS) -g -O0

.PHONY: clean grind coverage syms fuzz test

test: $(testsok)

clean:
	rm -f *~ core $(INCDIR)/*~ default.prof*
	rm -Rf $(ODIR)/*
	rm -Rf test/*.to test/*.tok
