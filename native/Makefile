
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

ifeq ($(PREFIX),)
	PREFIX := /usr
endif

tests := $(patsubst %.c,%.to,$(wildcard test/test*.c))
tests_ok := $(patsubst %.c,%.tok,$(wildcard test/test*.c))
tests_vg := $(patsubst %.c,%.tvg,$(wildcard test/test*.c))

all: obj/shmmain obj/shm_example_reader obj/shm_example_writer obj/libchronicle.so

$(ODIR)/%.so: $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) $(CDFLAGS)

%.to: %.c test/testdata.h $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0 -lcmocka -larchive

%.tok: %.to
	$< && touch $@

%.tvg: %.to
	valgrind --track-origins=yes --leak-check=full $< && touch $@


$(ODIR)/shmmain: shmmain.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

$(ODIR)/shm_example%: shm_example%.c $(DEPS)
	$(CC) -o $@ $< $(CFLAGS) -g -O0

coverage: obj/shmcov
	rm -Rf test/coverage_queue
	mkdir -p test/coverage_queue
	obj/shmcov -v -c -a WORLD test/coverage_queue
	llvm-profdata merge default.profraw -output=default.profout
	llvm-cov show -instr-profile default.profout obj/shmcov

$(ODIR)/shmcov: shmmain.c $(DEPS)
	clang -o $@ $< $(CFLAGS) -fprofile-instr-generate -fcoverage-mapping -O0 -g

k.h:
	wget https://raw.githubusercontent.com/KxSystems/kdb/master/c/c/k.h

$(ODIR)/fuzzmain: fuzzmain.c k.h mock_k.h $(DEPS)
	afl-clang -o $@ $< $(CFLAGS) -g -O0

fuzz: $(ODIR)/fuzzmain
	# brew install afl-fuzz
	# sudo apt install afl++
	rm -Rf test/fuzz_output/*
	rm -Rf test/fuzz_queue/
	mkdir -p test/fuzz_output test/fuzz_queue
	AFL_SKIP_CPUFREQ=1 afl-fuzz -i test/fuzz_input -o test/fuzz_output $(ODIR)/fuzzmain test/fuzz_queue -


.PHONY: clean grind coverage fuzz test install

test: $(tests_ok)

grind: $(tests_vg)

clean:
	rm -f *~ core $(INCDIR)/*~ default.prof*
	rm -Rf $(ODIR)/*
	rm -Rf test/*.to test/*.tok test/*.tvg

install: obj/libchronicle.so
	install -d $(DESTDIR)$(PREFIX)/lib/
	install -m 644 obj/libchronicle.so $(DESTDIR)$(PREFIX)/lib/
	install -d $(DESTDIR)$(PREFIX)/include/
	install -m 644 libchronicle.h $(DESTDIR)$(PREFIX)/include/
