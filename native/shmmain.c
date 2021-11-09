// Copyright 2018 Tea Engineering Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

int main(const int argc, char **argv) {
	int c;
	opterr = 0;
	int kxflag = 0;
	int verboseflag = 0;
	int pollflag = 0;
	char* append = NULL;
	uint64_t index = 0;

	while ((c = getopt(argc, argv, "kdmi:va:p")) != -1)
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
		case '?':
			fprintf(stderr, "Unknown option `-%c'.\n", optopt);
			exit(2);
		default:
			fprintf(stderr, "%c??", c);
			exit(3);
	}

	if (optind + 1 > argc) {
		printf("Missing mandatory argument.\n Expected: %s [-k] [-d] [-m] [-i INDEX] [-v] [-a text] [-p] :queue\n", argv[0]);
		printf("  -k expect kx format queue\n");
		printf("  -d print data\n");
		printf("  -m print metadata\n");
		printf("  -i INDEX resume from index\n");
		printf("  -v verbose mode\n");
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
