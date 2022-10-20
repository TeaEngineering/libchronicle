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

static void queue_init_cleanup(void **state) {
    queue_t* queue = chronicle_init("q2");
    assert_non_null(queue);
    chronicle_cleanup(queue);
}

static void queue_not_exist(void **state) {
    queue_t* queue = chronicle_init("q2");
    assert_non_null(queue);
    assert_int_not_equal(chronicle_open(queue), 0);
    assert_string_equal(chronicle_strerror(), "dir stat fail");
}

static void queue_is_file(void **state) {
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    mkstemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir);
    assert_non_null(queue);
    assert_int_not_equal(chronicle_open(queue), 0);
    assert_string_equal(chronicle_strerror(), "dir is not a directory");

    chronicle_cleanup(queue);
    delete_test_data(temp_dir);
    free(temp_dir);
}

static void queue_empty_dir_no_ver(void **state) {
    // this will be an empty directory, so we can't tell the version
    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    temp_dir = mkdtemp(temp_dir);
    queue_t* queue = chronicle_init(temp_dir);
    assert_non_null(queue);
    assert_int_not_equal(chronicle_open(queue), 0);
    assert_string_equal(chronicle_strerror(), "qfi version detect fail");

    chronicle_cleanup(queue);

    delete_test_data(temp_dir);
    free(temp_dir);
}

static void queue_cqv5_sample_input(void **state) {
    uint64_t idx = 0;
    collected_t result;

    char* test_queuedir = unpack_test_data("cqv5-sample-input.tar.bz2", argv0);
    assert_non_null(test_queuedir);

    char* queuedir;
    asprintf(&queuedir, "%s/qv5", test_queuedir);
    queue_t* queue = chronicle_init(queuedir);
    assert_non_null(queue);

    chronicle_set_decoder(queue, &wire_parse_textonly);
    chronicle_set_encoder(queue, &wirepad_sizeof, &wirepad_write);
    assert_int_equal(chronicle_open(queue), 0);

    tailer_t* tailer = chronicle_tailer(queue, NULL, NULL, 0);
    assert_non_null(tailer);
    assert_int_equal(chronicle_tailer_state(tailer), TS_PEEK);

    char* p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("one", p);
    assert_int_equal(result.index, 0x4A0500000000);
    assert_int_equal(chronicle_tailer_state(tailer), TS_COLLECTED);
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_int_equal(result.index, 0x4A0500000001);
    assert_string_equal("two", result.msg);
    assert_int_equal(4, result.sz); // TODO: bug, should parser be able to re-write this?
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_int_equal(result.index, 0x4A0500000002);
    assert_string_equal("three", result.msg);
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_int_equal(result.index, 0x4A0500000003);
    assert_string_equal("a much longer item that will need encoding as variable length text", p);
    free(p);

    wirepad_t* pad = wirepad_init(1024);
    wirepad_text(pad, "four five");
    idx = chronicle_append(queue, pad);
    assert_int_equal(idx, 0x4A0500000004);  // easier to see cycle/index split in hex

    // write with timestamp that will match recording of 20211118F
    wirepad_clear(pad);
    wirepad_text(pad, "six");
    idx = chronicle_append_ts(queue, pad, 1637267400000L); // 20211118T203000
    assert_int_equal(idx, 0x4A0500000005);  // easier to see cycle/seqnum split in hex

    // write next day's timestamp
    wirepad_clear(pad);
    wirepad_text(pad, "seven");
    idx = chronicle_append_ts(queue, pad, 1637308800000L); // 20211119T080000
    assert_int_equal(idx, 0x4a0600000000);  // cycle rolled, seqnum reset

    p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("four five", p);
    free(p);
    p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("six", p);
    free(p);

    chronicle_collect(tailer, &result);
    assert_string_equal("seven", result.msg);
    assert_int_equal(result.index, 0x4A0600000000);
    free(result.msg);
    chronicle_tailer_close(tailer);

    // add a new tailer starting from midway through
    tailer_t* tailer2 = chronicle_tailer(queue, NULL, NULL, 0x4A0500000003);
    chronicle_collect(tailer2, &result);
    assert_int_equal(result.index, 0x4A0500000003);
    assert_string_equal("a much longer item that will need encoding as variable length text", result.msg);
    free(result.msg);

    chronicle_collect(tailer2, &result);
    assert_string_equal("four five", result.msg);
    free(result.msg);
    chronicle_collect(tailer2, &result);
    assert_string_equal("six", result.msg);
    free(result.msg);
    chronicle_collect(tailer2, &result);
    assert_string_equal("seven", result.msg);
    free(result.msg);

    chronicle_cleanup(queue);

    delete_test_data(test_queuedir);
    free(queuedir);
    free(test_queuedir);
    wirepad_free(pad);
}

void* parse_cqv4_textonly(unsigned char* base, int lim) {
    char* text_result = strndup((const char*) base, lim);
    return text_result;
}

static void queue_cqv4_sample_input(void **state) {
    char* test_queuedir = unpack_test_data("cqv4-sample-input.tar.bz2", argv0);
    assert_non_null(test_queuedir);

    char* queuedir;
    asprintf(&queuedir, "%s/cqv4", test_queuedir);
    queue_t* queue = chronicle_init(queuedir);
    assert_non_null(queue);
    chronicle_set_decoder(queue, &parse_cqv4_textonly);
    chronicle_set_encoder(queue, &wirepad_sizeof, &wirepad_write);
    assert_int_equal(chronicle_open(queue), 0);

    collected_t result;

    tailer_t* tailer = chronicle_tailer(queue, NULL, NULL, 0);
    assert_non_null(tailer);

    char* p = (char*)chronicle_collect(tailer, &result);
    assert_non_null(p);
    assert_string_equal("one", p);
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("two", p);
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("three", p);
    free(p);

    p = (char*)chronicle_collect(tailer, &result);
    assert_string_equal("a much longer item that will need encoding as variable length text", p);
    free(p);

    chronicle_cleanup(queue);

    delete_test_data(test_queuedir);
    free(queuedir);
    free(test_queuedir);
}

static void queue_init_rollscheme(void **state) {
    queue_t* queue = chronicle_init("/tmp");
    assert_non_null(queue);

    assert_int_equal(chronicle_get_version(queue), 0);
    chronicle_set_version(queue, 6);
    assert_int_equal(chronicle_get_version(queue), 0);
    chronicle_set_version(queue, 4);
    assert_int_equal(chronicle_get_version(queue), 4);
    chronicle_set_version(queue, 5);
    assert_int_equal(chronicle_get_version(queue), 5);

    assert_null(chronicle_get_roll_scheme(queue));
    chronicle_set_roll_scheme(queue, "MADE_UP_SCHEME");
    assert_null(chronicle_get_roll_scheme(queue));
    chronicle_set_roll_scheme(queue, "FAST_HOURLY");
    assert_string_equal(chronicle_get_roll_scheme(queue), "FAST_HOURLY");
    assert_string_equal(chronicle_get_roll_format(queue), "yyyyMMdd-HH'F'");
    char* p;
    p = chronicle_get_cycle_fn(queue, 0);
    assert_string_equal(p, "/tmp/19700101-00F.cq4");
    free(p);
    p = chronicle_get_cycle_fn(queue, 1);
    assert_string_equal(p, "/tmp/19700101-01F.cq4");
    free(p);
    p = chronicle_get_cycle_fn(queue, 24);
    assert_string_equal(p, "/tmp/19700102-00F.cq4");
    free(p);

    chronicle_set_roll_scheme(queue, "FIVE_MINUTELY");
    p = chronicle_get_cycle_fn(queue, 0);
    assert_string_equal(p, "/tmp/19700101-0000V.cq4");
    free(p);
    p = chronicle_get_cycle_fn(queue, 1);
    assert_string_equal(p, "/tmp/19700101-0005V.cq4");
    free(p);

    chronicle_set_roll_scheme(queue, "DAILY");
    assert_string_equal(chronicle_get_roll_scheme(queue), "DAILY");
    assert_string_equal(chronicle_get_roll_format(queue), "yyyyMMdd");
    p = chronicle_get_cycle_fn(queue, 0);
    assert_string_equal(p, "/tmp/19700101.cq4");
    free(p);

    chronicle_cleanup(queue);
}

int main(int argc, char* argv[]) {
    argv0 = argv[0];
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(queue_init_cleanup),
        cmocka_unit_test(queue_not_exist),
        cmocka_unit_test(queue_is_file),
        cmocka_unit_test(queue_empty_dir_no_ver),
        cmocka_unit_test(queue_init_rollscheme),
        cmocka_unit_test(queue_cqv4_sample_input),
        cmocka_unit_test(queue_cqv5_sample_input),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
