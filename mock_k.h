//
// stub out kx layer (the extern function in k.h) so we can run in main, profile,
// valgrind native code etc.
//
// It's impossible to find the source of memory leaks with the Kx slab allocator
// as valgrind can't hook the individual allocations to record the stack.
//

// globals
int kxx_errno = 0;
char* kxx_msg = NULL;

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
    K r = ktn(-KI, 0);
    r->i = i;
    return r;
}
K kj(long long i) {
    K r = ktn(-KJ, 0);
    r->j = i;
    return r;
}
K kss(const char* ss) {
    K r = ktn(-KS, 0);
    r->s = (char*)ss;
    return r;
}
K dl(void* fnptr, int n) {
    K r = ktn(100,0);
    r->s = fnptr;
    r->a = n;
    return r;
}

typedef K (*kfunc_1arg)(K);
typedef K (*kfunc_2arg)(K,K);
typedef K (*kfunc_3arg)(K,K,K);

K dot(K x, K y) { // call function pointer in x with args in mixed list y
    if (x->t != 100) return krr("x must be fptr");
    if (y->t != 0) return krr("y must be list");
    if (x->a == 2) {
        kfunc_2arg fptr = (kfunc_2arg)x->s;
        return fptr(kK(y)[0], kK(y)[1]);
    }
    return ki(1);
}

K knk(int n, ...) { // create a mixed list from K's in varg
    va_list ap;
    K r = ktn(0, n);
    va_start(ap, n); //Requires the last fixed parameter (to get the address)
    for(int j=0; j<n; j++) {
        kK(r)[j] = va_arg(ap, K); //Requires the type to cast to. Increments ap to the next argument.
    }
    va_end(ap);
    return r;
}
//                mx KB UU ?? KG KH KI KJ KE KF KC KS KP KM KD KZ KN KU KV KT
//                0  1  2  -  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19
int sizefor[] = { 8, 1, 16,0, 1, 2, 4, 8, 4, 8, 1, 8 ,8, 4, 4, 8, 8, 4, 4, 4 };
K ktn(int type, long long n) { // K type number
    int sz = (type > 19) ? 8 : sizefor[abs(type)];
    K r = malloc(sizeof(struct k0) + n*sz);
    r->r = 0;
    r->t = type;
    if (n > 0) r->n = n; // keep: trap accessing n for atom in valgrind
    return r;
}

// dummy serialiser returns a single byte array [SOH]
K b9(I mode, K obj) {
    K r = ktn(KB,1);
    r->G0[0] = 1;
    return r;
}

K d9(K obj) {
    return ki(1);
}

int okx(K obj) { return 1; }
// repl equivelent wrapper (protected eval, with and without gc)
K pe(K x) {
    if (kxx_errno != 0) exit(-1);
    return x;
}
void per(K x) {
    pe(x);
    if (x != NULL) r0(x);
}

void r0(K x) { // Decrement the object‘s reference count
    if (x == 0) { printf("Bug r0 of null pointer %p\n", x); return; }
    if (x->r < 0) printf("Bug double-free of %p\n", x);
    if (x->r == 0) {
        if (x->t == 0) for (int i = 0; i < x->n; i++) r0(kK(x)[i]);
        // flip?
        // dict?
        free(x);
    } else {
        x->r--;
    }
}
K r1(K x) { // Increment the object‘s reference count
    x->r++;
    return x;
}

