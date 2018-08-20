#include <shmipc.c>
#include <stdarg.h>
#include <ctype.h>
#include "mock_k.h"

// This is a stand-alone tool for replaying a queue for use with valgrind etc that are tricky to
// operate within KDB, e.g. with
// valgrind --leak-check=full -v ./native/obj/shmmain :java/queue
int print_data = 0;

K printxy(K x, K y) {
	if (print_data) {
		printf("%llu: ", x->j);
		if (y->t == KC)
			fwrite(kC(y), sizeof(char), y->n, stdout);
		printf("\n");
	}
	return ki(5);
}

K kfrom_c_str(const char* s) { // k symbol from string?
	int n = strlen(s);
	K r = ktn(KC, n);
	memcpy((char*)r->G0, s, n);
	return r;
}

/* The state array must be initialized to not be all zero */
uint32_t xorshift128(uint32_t state[static 4]) {
	/* Algorithm "xor128" from p. 5 of Marsaglia, "Xorshift RNGs" */
	uint32_t s, t = state[3];
	t ^= t << 11;
	t ^= t >> 8;
	state[3] = state[2]; state[2] = state[1]; state[1] = s = state[0];
	t ^= s;
	t ^= s >> 19;
	state[0] = t;
	return t;
}

//////// APPLE VALGRIND COMPAT ////////
// valgrind has no support for mach system call under mkstemp, printing
//     WARNING: unhandled amd64-darwin syscall: unix:464      and
// returning -1 as fp whilst setting syserr to "Function not implemented".
// Due to the fail the random number generator leaks a tonne of malloc() mem.
// Reported but unresolved https://www.mail-archive.com/kde-bugs-dist@kde.org/msg209613.html
static inline uint64_t rdtsc(void) {
  uint32_t a, d;
  __asm__ __volatile__("rdtscp" : "=a" (a), "=d" (d));
  return (((uint64_t) d << 32) | a);
}
int c_mkstemp(char* pattern) {
	uint32_t xor_state[4];
	xor_state[0] = xor_state[1] = xor_state[2] = xor_state[3] = (uint32_t)rdtsc();
	char* rep = strstr(pattern, "XXXXXX");
	if (rep) {
		for (int i = 0; i < 6; i++) {
			uint8_t incre = xorshift128(xor_state) & 0x1F;
			rep[i] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"[incre];
		}
	}
	int fid = open(pattern, O_RDWR | O_CREAT | O_EXCL, 0660);
	if (fid < 0) {
		printf("tmpfile %s: %s\n", pattern, strerror(errno));
		abort();
	}
	return fid;
}
////////  END APPLE VALGRIND COMPAT ////////

int main(const int argc, char **argv) {
	int c;
	opterr = 0;
	int kxflag = 0;
	int verboseflag = 0;
	int pollflag = 0;
	FILE * fuzzfid = NULL;
	int fuzzit = 0;
	char* append = NULL;
	uint64_t index = 0;

	while ((c = getopt(argc, argv, "kdmi:va:pF:")) != -1)
	switch (c) {
		case 'd':
			print_data = 1;
			break;
		case 'k':
			kxflag = 1;
			break;
		case 'v':
			verboseflag = 1;
			break;
		case 'i':
			break;
		case 'a':
			append = optarg;
			break;
		case 'p':
			pollflag = 1;
			break;
		case 'F':
			fuzzfid = stdin;
			if (strcmp(optarg, "-")) {
				fuzzfid = fopen(optarg, "r");
				if (fuzzfid == NULL) { fprintf(stderr, "Unable to open %s,\n", optarg); exit(4); }
			}
			fuzzit = 1;
			break;
		case '?':
			fprintf(stderr, "Unknown option `-%c'.\n", optopt);
			exit(2);
		default:
			fprintf(stderr, "%c??", c);
			exit(3);
	}

	if (optind + 1 > argc) {
		printf("Missing mandatory argument.\n Expected: %s [-k] [-d] [-m] [-i INDEX] [-v] [-a text] [-p] [-F FILE] :queue\n", argv[0]);
		printf("  -k expect kx format queue\n");
		printf("  -d print data\n");
		printf("  -m print metadata\n");
		printf("  -i INDEX resume from index\n");
		printf("  -v verbose mode\n");
		printf("  -F FILE Fuzz using fuzzing script from file (- stdin) providing clock and data sizes\n");
		printf("  -a TEXT write value text\n");
		printf("  -p poll queue (rather than exit)\n");
		exit(1);
	}

	// what follows is translated q calls from shmipc.q
	K dir = kss(argv[optind]);
	K parser = kss(kxflag ? "kx" : "text");
	per(shmipc_init(dir, parser));

	K cb = dl(&printxy, 2);
	K kindex = kj(index);
	per(shmipc_tailer(dir,cb,kindex));
	per(shmipc_peek(dir));

	if (verboseflag) {
		per(shmipc_debug((K)NULL));
	}

	if (append) {
		printf("writing %s\n", append);
		K msg = kfrom_c_str(append);
		per(shmipc_append(dir,msg));
		r0(msg);
	}

	if (fuzzit) {
		size_t linecap = 0;
		ssize_t linelen = 0;
		char *msgp = NULL;
		char *parsep = NULL;

		int line = 0;
		long long bytes;
		long long time;
		uint64_t index;
		uint64_t clock = 0;

		uint32_t xor_state[4];

		char* tmpfile = strdup("/tmp/shmmain.XXXXXX");
		int tmpfid = c_mkstemp(tmpfile);
		printf("logging fuxx expectations to %s fid %d msg %s\n", tmpfile, tmpfid, strerror(errno));
		FILE* tmp = fdopen(tmpfid, "w+");
		printf(" tmp is %p %s\n", tmp, strerror(errno));
		while ((linelen = getline(&msgp, &linecap, fuzzfid)) > 0) {
			line++;
			parsep = msgp;
			time = strtoll(parsep, &parsep, 0);
			if (*parsep == ' ') parsep++;
			bytes = strtoll(parsep, &parsep, 0);

			printf(" FUZ: %lld millis, %lld bytes\n", time, bytes);
			clock += time;
			xor_state[0] = xor_state[1] = xor_state[2] = xor_state[3] = (uint32_t)line+1;
			K x = ktn(KC, bytes);
			for (long long b = 0; b < bytes; b++) {
				kG(x)[b] = (uint8_t)xorshift128(xor_state);
			}
			K r = shmipc_append(dir, x);
			fprintf(tmp, "%lld %lld %lld\n", clock, bytes, r->j);
			r0(r);
			r0(x);
		}
		// we're done parsing input, now replay checking using temp file
		//fclose(tmp);
		//close(tmpfid);

		fflush(tmp);
		if (fseek(tmp, 0L, SEEK_SET) != 0) { printf("abort not fseek"); abort(); };

		int rline = 0;
		int r;
		printf("replay\n");
		do {
			r = fscanf(tmp, "%lld %lld %lld\n", &clock, &bytes, &index);
			if (r == 3) {
				// clock bytes index
				printf(" did %lld %lld %lld\n", clock, bytes, index);
			} else if (rline != line) {
				printf ("error, %s r=%d  at line %d!\n\n", tmpfile, r, rline);
			}
			rline++;
		} while (r != EOF);

		fclose(tmp);

		// unlink(tmpfile);
		free(tmpfile);
		if (msgp) free(msgp);
		printf("parse finished\n");

		exit(rline == line ? 0 : 5); // exit with fail

	}

	while (pollflag) {
		usleep(500*1000);
		per(shmipc_peek(dir));
	}

	if (verboseflag) {
		per(shmipc_debug((K)NULL));
	}

	per(shmipc_close(dir));

	r0(dir);
	r0(parser);
	r0(cb);
	r0(kindex);

	return 0;
}
