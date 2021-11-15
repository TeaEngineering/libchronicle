#define _GNU_SOURCE

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <wire.h>

void handle_text(char* buf, int sz, char* data, int dsz, wirecallbacks_t* cbs) {
    int* res = (int*)cbs->userdata;
    *res = (strncmp(buf, "hello", sz) == 0) ? 5 : 1;
    if (wire_trace) printf("  got text cb %.*s\n", dsz, data);
}

static void test_wirepad_text(void **state) {
    wire_trace = 0;

    wirepad_t* pad = wirepad_init(1024);
    assert_non_null(pad);
    assert_int_equal(wirepad_sizeof(pad), 0);

    // the null isn't written. 1 byte descriptor, 5 bytes text, 2 padding
    wirepad_text(pad, "hello");
    assert_int_equal(wirepad_sizeof(pad), 1+5+2);

    //wirepad_clear(pad);
    //assert_int_equal(wirepad_sizeof(pad), 0);
    char* dump = wirepad_hexformat(pad);
    assert_string_equal(dump,
        "00000000 e5 68 65 6c 6c 6f 00 00                          .hello..         \n"
    );
    free(dump);

    // run the parser over the pad and check we get one callback
    int result = 0;

    wirecallbacks_t hcbs;
    bzero(&hcbs, sizeof(hcbs));
    hcbs.field_char = &handle_text;
    hcbs.userdata = &result;
    wirepad_parse(pad, &hcbs);

    assert_int_equal(result, 5);

    wirepad_free(pad);
}

static void test_wirepad_fields(void **state) {
    // example from https://github.com/OpenHFT/Chronicle-Wire#simple-use-case

    wirepad_t* pad = wirepad_init(1024);
    wirepad_field_text(pad,    "message", "Hello World");
    wirepad_field_int64(pad,   "number",  1234567890L);
    wirepad_field_enum(pad,    "code",    "SECONDS");
    wirepad_field_float64(pad, "price",   10.50);

    char* dump = wirepad_hexformat(pad);
    assert_string_equal(dump,
        "00000000 c7 6d 65 73 73 61 67 65  eb 48 65 6c 6c 6f 20 57 .message .Hello W\n"
        "00000010 6f 72 6c 64 c6 6e 75 6d  62 65 72 a6 d2 02 96 49 orld.num ber....I\n"
        "00000020 c4 63 6f 64 65 e7 53 45  43 4f 4e 44 53 c5 70 72 .code.SE CONDS.pr\n"
        "00000030 69 63 65 90 00 00 28 41                          ice...(A         \n"
    );
    free(dump);
    wirepad_free(pad);
};


int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_wirepad_text),
        cmocka_unit_test(test_wirepad_fields),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
