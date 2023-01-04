
# k.h from https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h
# apt-get install libcurl4-openssl-dev

detected_OS := $(shell uname -s)

IDIR=.
CC=gcc
CFLAGS=libchronicle.c wire.c buffer.c -fPIC -I$(IDIR) -Wall
CDFLAGS=-shared
ifeq ($(detected_OS),Darwin)  # Mac OS X
    CDFLAGS += -undefined dynamic_lookup
endif
ifeq ($(detected_OS),Linux)
    CFLAGS += -std=gnu99
endif

ODIR=obj
LIBS=-lm
DEPS=wire.h wire.c libchronicle.h libchronicle.c buffer.h buffer.c

tests := $(patsubst %.c,%.to,$(wildcard test/test*.c))
tests_ok := $(patsubst %.c,%.tok,$(wildcard test/test*.c))
tests_vg := $(patsubst %.c,%.tvg,$(wildcard test/test*.c))

all: obj/hpet.so obj/shmmain obj/shm_example_reader obj/shm_example_writer obj/libchronicle.so

k.h:
	wget https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h

$(ODIR)/%.so: $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

%.to: %.c test/testdata.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0 -lcmocka -larchive

%.tok: %.to
	$< && touch $@

%.tvg: %.to
	valgrind --track-origins=yes --leak-check=full $< && touch $@

$(ODIR)/fuzzmain: fuzzmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

$(ODIR)/shmmain: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

$(ODIR)/shm_example%: shm_example%.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

coverage: obj/shmcov
	obj/shmcov -v -a WORLD :demo/stress
	/Library/Developer/CommandLineTools/usr/bin/llvm-profdata merge default.profraw -output=default.profout
	/Library/Developer/CommandLineTools/usr/bin/llvm-cov show -instr-profile default.profout obj/shmcov

$(ODIR)/shmcov: shmmain.c shmipc.c mock_k.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -fprofile-instr-generate -fcoverage-mapping -O0 -g

syms:
	nm -D ../../kdb/l64/q | grep " T "

fuzz: $(ODIR)/shmmain.fuzz
	# brew install afl-fuzz
	mkdir -p test/fuzz_output
	AFL_SKIP_CPUFREQ=1 afl-fuzz -i test/fuzz_input -o test/fuzz_output $(ODIR)/shmmain.fuzz -F - :../java/out

$(ODIR)/shmmain.fuzz: shmmain.c shmipc.c mock_k.h $(DEPS)
	afl-clang -o $@ $< $(CFLAGS) -g -O0

.PHONY: clean grind coverage syms fuzz test

test: $(tests_ok)

grind: $(tests_vg)

clean:
	rm -f *~ core $(INCDIR)/*~ default.prof*
	rm -Rf $(ODIR)/*
	rm -Rf test/*.to test/*.tok test/*.tvg
