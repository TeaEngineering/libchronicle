#include <shmipc.c>
#include <stdarg.h>
#include <ctype.h>
#include "mock_k.h"

// This is a stand-alone tool for replaying a queue for use with valgrind etc that are tricky to
// operate within KDB, e.g. with
// valgrind --leak-check=full -v ./native/obj/shmmain :java/queue
int pflag = 0;

K printxy(K x, K y) {
	if (pflag) {
		printf("x=%llu y=", x->j);
		fwrite(kC(y), sizeof(char), y->n, stdout);
		printf("\n");
	}
	return ki(5);
}

int main(const int argc, char **argv) {
	int c;
	opterr = 0;

	while ((c = getopt(argc, argv, "p")) != -1)
	switch (c) {
		case 'p':
			pflag = 1;
			break;
		case '?':
			if (isprint (optopt)) {
				fprintf (stderr, "Unknown option `-%c'.\n", optopt);
			} else {
				fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
			}
			return 1;
		default:
			abort();
	}

	if (optind + 1 > argc) {
		printf("Missing mandatory argument.\n Expected: %s [-p] [-i INDEX] :queue\n", argv[0]);
		exit(-1);
	}

	// what follows is translated q calls from shmipc.q
	K dir = kss(argv[optind]);
	K parser = kss("text");
	K r;
	per(shmipc_init(dir, parser));
	per(shmipc_debug((K)NULL));
	per(shmipc_peek(dir));

	K cb = dl(&printxy, 2);
	K index = kj(0);
	per(shmipc_tailer(dir,cb,index));
	per(shmipc_debug((K)NULL));
	per(shmipc_peek(dir));
	per(shmipc_debug((K)NULL));

	printf("closing down\n");
	per(shmipc_close(dir));
	per(shmipc_debug((K)NULL));

	r0(dir);
	r0(parser);
	r0(cb);
	r0(index);

	return 0;
}
