#define _GNU_SOURCE

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <buffer.h>

static void test_buffer_hello(void **state) {

    char* f = formatbuf("hello world", 12);
    assert_string_equal(f,
        "00000000 68 65 6c 6c 6f 20 77 6f  72 6c 64 00             hello wo rld.    \n");
    free(f);

    f = formatbuf("hello", 0);
    assert_string_equal(f,
        "00000000                                                                   \n");
    free(f);

    char* p = "The chances of a neutrino actually hitting something... are roughly "
            "comparable to that of dropping a ball bearing at random from a cruising "
            "747 and hitting, say, an egg sandwich.";
    f = formatbuf(p, strlen(p));
    assert_string_equal(f,
        "00000000 54 68 65 20 63 68 61 6e  63 65 73 20 6f 66 20 61 The chan ces of a\n"
        "00000010 20 6e 65 75 74 72 69 6e  6f 20 61 63 74 75 61 6c  neutrin o actual\n"
        "00000020 6c 79 20 68 69 74 74 69  6e 67 20 73 6f 6d 65 74 ly hitti ng somet\n"
        "00000030 68 69 6e 67 2e 2e 2e 20  61 72 65 20 72 6f 75 67 hing...  are roug\n"
        "00000040 68 6c 79 20 63 6f 6d 70  61 72 61 62 6c 65 20 74 hly comp arable t\n"
        "00000050 6f 20 74 68 61 74 20 6f  66 20 64 72 6f 70 70 69 o that o f droppi\n"
        "00000060 6e 67 20 61 20 62 61 6c  6c 20 62 65 61 72 69 6e ng a bal l bearin\n"
        "00000070 67 20 61 74 20 72 61 6e  64 6f 6d 20 66 72 6f 6d g at ran dom from\n"
        "00000080 20 61 20 63 72 75 69 73  69 6e 67 20 37 34 37 20  a cruis ing 747 \n"
        "00000090 61 6e 64 20 68 69 74 74  69 6e 67 2c 20 73 61 79 and hitt ing, say\n"
        "000000a0 2c 20 61 6e 20 65 67 67  20 73 61 6e 64 77 69 63 , an egg  sandwic\n"
        "000000b0 68 2e                                            h.               \n"
    );
    free(f);
}


int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_buffer_hello),
        // cmocka_unit_test(),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
