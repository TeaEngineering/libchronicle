#define _GNU_SOURCE

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>

#include "k.h"
#define COBJ K
#include <libchronicle.c>
#include <mock_k.h>

long print_data(void* ctx, uint64_t index, K y) {
    printf("[%" PRIu64 "] ", index);
    if (y->t == KC)
        fwrite(kC(y), sizeof(char), y->n, stdout);
    printf("\n");
    return 0;
}

K parse_data(unsigned char* base, int lim) {
    if (debug) printf(" text: '%.*s'\n", lim, base);
    K msg = ktn(KC, lim); // don't free this, handed over to q interp
    memcpy((char*)msg->G0, base, lim);
    return msg;
}

int append_data(unsigned char* base, int sz, K msg) {
    memcpy(base, (char*)msg->G0, sz);
    return msg->n;
}

long sizeof_data(K msg) {
    return msg->n;
}

static void queue_not_exist(void **state) {
    queue_t* queue = chronicle_init("q2", &parse_data, &sizeof_data, &append_data);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "dir stat fail");
}

static void queue_is_file(void **state) {
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    mkstemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir, &parse_data, &sizeof_data, &append_data);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "dir is not a directory");
}

static void queue_empty_dir_no_ver(void **state) {
    // this will be an empty directory, so we can't tell the version
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    temp_dir = mkdtemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir, &parse_data, &sizeof_data, &append_data);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "qfi version detect fail");
}


int main(void) {

    const struct CMUnitTest tests[] = {
        cmocka_unit_test(queue_not_exist),
        cmocka_unit_test(queue_is_file),
        cmocka_unit_test(queue_empty_dir_no_ver),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
