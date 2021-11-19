#define _GNU_SOURCE

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>

#include <libchronicle.h>
#include <wire.h>
#include "testdata.h"

char* argv0;

int print_msg(void* ctx, uint64_t index, COBJ y) {
    printf("[%" PRIu64 "] %s\n", index, (char*)y);
    return 0;
}

static void queue_not_exist(void **state) {
    queue_t* queue = chronicle_init("q2", &wire_parse_textonly, &wirepad_sizeof, &wirepad_write);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "dir stat fail");
}

static void queue_is_file(void **state) {
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    mkstemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir, &wire_parse_textonly, &wirepad_sizeof, &wirepad_write);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "dir is not a directory");

    chronicle_close(queue);
    delete_test_data(temp_dir);
    free(temp_dir);
}

static void queue_empty_dir_no_ver(void **state) {
    // this will be an empty directory, so we can't tell the version
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    temp_dir = mkdtemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir, &wire_parse_textonly, &wirepad_sizeof, &wirepad_write);
    assert_null(queue);
    assert_string_equal(chronicle_strerror(), "qfi version detect fail");

    chronicle_close(queue);

    delete_test_data(temp_dir);
    free(temp_dir);
}

static void queue_cqv5_sample_input(void **state) {
    char* test_queuedir = unpack_test_data("cqv5-sample-input.tar.bz2", argv0);
    assert_non_null(test_queuedir);

    char* queuedir;
    asprintf(&queuedir, "%s/qv5", test_queuedir);
    queue_t* queue = chronicle_init(queuedir, &wire_parse_textonly, &wirepad_sizeof, &wirepad_write);
    assert_non_null(queue);

    tailer_t* tailer = chronicle_tailer(queue, NULL, NULL, 0);
    assert_non_null(tailer);

    char* p = (char*)chronicle_collect(tailer);
    assert_string_equal("one", p);
    free(p);

    p = (char*)chronicle_collect(tailer);
    assert_string_equal("two", p);
    free(p);

    p = (char*)chronicle_collect(tailer);
    assert_string_equal("three", p);
    free(p);

    p = (char*)chronicle_collect(tailer);
    assert_string_equal("a much longer item that will need encoding as variable length text", p);
    free(p);

    chronicle_close(queue);

    delete_test_data(test_queuedir);
    free(queuedir);
    free(test_queuedir);
}

int main(int argc, char* argv[]) {
    argv0 = argv[0];
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(queue_not_exist),
        cmocka_unit_test(queue_is_file),
        cmocka_unit_test(queue_empty_dir_no_ver),
        cmocka_unit_test(queue_cqv5_sample_input),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
