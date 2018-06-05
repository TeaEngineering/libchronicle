#include <shmipc.c>
#include <stdarg.h>

// This is a stand-alone tool for replaying a queue for use with valgrind etc that are tricky to
// operate within KDB, e.g. with
// valgrind --leak-check=full -v ./native/obj/shmmain :java/queue

int kxx_errno = 0;
char* kxx_msg = NULL;

// stub out kx layer so we can run in main()
K krr(const S msg) {
	printf("'%s\n", msg);
	kxx_errno = -1;
	kxx_msg = msg;
	return (K)NULL;
}
K orr(const S msg) {
	printf("%s\n", msg);
	kxx_errno = -1;
	kxx_msg = msg;
	return (K)NULL;
}
K ki(int i) {
	return (K)NULL;
}
K kj(long long i) {
	K r = malloc(sizeof(struct k0));
	r->t = -KJ;
	r->j = i;
	r->r = 0;
	return r;
}
K kss(const char* ss) {
	K r = malloc(sizeof(struct k0));
	r->t = -KS;
	r->s = (char*)ss;
	r->r = 0;
	return r;
}
K kfunctionptr() {
	K r = malloc(sizeof(struct k0));
	r->t = 100;
	r->s = NULL;
	return r;
}

K dot(K x, K y) { // call function pointer in x with args in mixed list y
	printf("pretend calling function\n");
	r0(y);
	return (K)NULL;
}

K knk(int n, ...) { // create a mixed list from K's in varg
 	va_list ap;
    K r = malloc(sizeof(struct k0) + n*sizeof(K));
    r->r = 0;
    r->t = 0; // mixed
    r->n = n;
    va_start(ap, n); //Requires the last fixed parameter (to get the address)
    for(int j=0; j<n; j++) {
        kK(r)[j] = va_arg(ap, K); //Requires the type to cast to. Increments ap to the next argument.
        kK(r)[j];
    }
    va_end(ap);
    return r;
}
//                mx KB UU ?? KG KH KI KJ KE KF KC KS KP KM KD KZ KN KU KV KT
//                0  1  2  -  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19
int sizefor[] = { 8, 1, 16,1, 2, 2, 4, 8, 4, 8, 1, 8 ,8, 4, 4, 8, 8, 4, 4, 4 };
K ktn(int type, long long n) { // K type number
 	K r = malloc(sizeof(struct k0) + n*sizefor[abs(type)]);
    r->r = 0;
    r->t = type; // mixed
    r->n = n;
    return r;
}

K pe(K x) {
	if (kxx_errno != 0) exit(-1);
	return x;
}

void r0(K x) { // Decrement the object‘s reference count
	if (x->r < 0) printf("Bug double-free of %p\n", x);
	if (x->r == 0) {
		if (x->t == 0) for (int i = 0; i < x->n; i++) r0(kK(x)[i]);
		free(x);
	} else {
		x->r--;
	}
}
K r1(K x) { // Increment the object‘s reference count
	x->r++;
	return x;
}

int main(int argc, char const *argv[]) {
	if (argc < 2) {
		printf("Missing mandatory argument.\n Expected: %s [:queue]\n", argv[0]);
		exit(-1);
	}
   	// what follows is translated q calls from shmipc.q
	K dir = kss(argv[1]);
	K parser = kss("text");
	K r;
	r = pe(shmipc_init(dir, parser));
	r = pe(shmipc_debug((K)NULL));
	r = pe(shmipc_peek(dir));
	r = pe(shmipc_debug((K)NULL));
	r = pe(shmipc_debug((K)NULL));

	K cb = kfunctionptr();
	K index = kj(0);
	r = pe(shmipc_tailer(dir,cb,index));
	r = pe(shmipc_debug((K)NULL));
	r = pe(shmipc_peek(dir));
	r = pe(shmipc_close(dir));

	free(dir);
	free(parser);
	free(cb);
	free(index);

	printf("finished\n");

	return 0;
}
