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

    // the null isn't written. 1 byte descriptor, 5 bytes text
    wirepad_text(pad, "hello");
    // this might be padding from the QC layer, perhaps shouldn't be in this test
    wirepad_pad_to_x8_00(pad);
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
    wirepad_field_varint(pad,  "number",  1234567890L);
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


static void test_wirepad_metadata(void **state) {
    wire_trace = 0;
    // extract of the data written to metadata.cq4t (v5) up to the last non-zero byte
    // There is additional framing in this example, an outer structure of
    // chronicle-queue messages surounds the wire data.
    // You can use wiredpad_qc_start() and wirepad_qc_finish() to write these parts,
    // which also manipulates the "working bit" semantics (although not with a CAS op)
    // so non-contested writes only at the moment.
    //
    // 00000000 ac 00 00 40 b9 06 68 65  61 64 65 72 b6 07 53 54 ...@..he ader..ST
    // 00000010 53 74 6f 72 65 82 96 00  00 00 c8 77 69 72 65 54 Store... ...wireT
    // 00000020 79 70 65 b6 08 57 69 72  65 54 79 70 65 ec 42 49 ype..Wir eType.BI
    // 00000030 4e 41 52 59 5f 4c 49 47  48 54 c8 6d 65 74 61 64 NARY_LIG HT.metad
    // 00000040 61 74 61 b6 07 53 43 51  4d 65 74 61 82 5d 00 00 ata..SCQ Meta.]..
    // 00000050 00 c4 72 6f 6c 6c b6 08  53 43 51 53 52 6f 6c 6c ..roll.. SCQSRoll
    // 00000060 82 26 00 00 00 c6 6c 65  6e 67 74 68 a6 00 5c 26 .&....le ngth..\&
    // 00000070 05 c6 66 6f 72 6d 61 74  eb 79 79 79 79 4d 4d 64 ..format .yyyyMMd
    // 00000080 64 27 46 27 c5 65 70 6f  63 68 00 d7 64 65 6c 74 d'F'.epo ch..delt
    // 00000090 61 43 68 65 63 6b 70 6f  69 6e 74 49 6e 74 65 72 aCheckpo intInter
    // 000000a0 76 61 6c 40 c8 73 6f 75  72 63 65 49 64 00 8f 8f val@.sou rceId...
    // 000000b0 24 00 00 00 b9 14 6c 69  73 74 69 6e 67 2e 68 69 $.....li sting.hi
    // 000000c0 67 68 65 73 74 43 79 63  6c 65 8e 00 00 00 00 a7 ghestCyc le......
    // 000000d0 fd 49 00 00 00 00 00 00  24 00 00 00 b9 13 6c 69 .I...... $.....li
    // 000000e0 73 74 69 6e 67 2e 6c 6f  77 65 73 74 43 79 63 6c sting.lo westCycl
    // 000000f0 65 8e 01 00 00 00 00 a7  fd 49 00 00 00 00 00 00 e....... .I......
    // 00000100 1c 00 00 00 b9 10 6c 69  73 74 69 6e 67 2e 6d 6f ......li sting.mo
    // 00000110 64 43 6f 75 6e 74 8f a7  01 00 00 00 00 00 00 00 dCount.. ........
    // 00000120 24 00 00 00 b9 14 63 68  72 6f 6e 69 63 6c 65 2e $.....ch ronicle.
    // 00000130 77 72 69 74 65 2e 6c 6f  63 6b 8e 00 00 00 00 a7 write.lo ck......
    // 00000140 00 00 00 00 00 00 00 80  2c 00 00 00 b9 1d 63 68 ........ ,.....ch
    // 00000150 72 6f 6e 69 63 6c 65 2e  6c 61 73 74 49 6e 64 65 ronicle. lastInde
    // 00000160 78 52 65 70 6c 69 63 61  74 65 64 8f 8f 8f 8f a7 xReplica ted.....
    // 00000170 ff ff ff ff ff ff ff ff  34 00 00 00 b9 29 63 68 ........ 4....)ch
    // 00000180 72 6f 6e 69 63 6c 65 2e  6c 61 73 74 41 63 6b 6e ronicle. lastAckn
    // 00000190 6f 77 6c 65 64 67 65 64  49 6e 64 65 78 52 65 70 owledged IndexRep
    // 000001a0 6c 69 63 61 74 65 64 a7  ff ff ff ff ff ff ff ff licated. ........

    char* buf="\254\000\000@\271\006header\266\007STStore\202\226\000\000\000\310wireType\266\010"
        "WireType\354BINARY_LIGHT\310metadata\266\007SCQMeta\202]\000\000\000\304roll\266\010"
        "SCQSRoll\202&\000\000\000\306length\246\000\\&\005\306format\353yyyyMMdd'F'\305epoch"
        "\000\327deltaCheckpointInterval@\310sourceId\000\217\217$\000\000\000\271\024"
        "listing.highestCycle\216\000\000\000\000\247\375I\000\000\000\000\000\000$\000\000\000\271\023"
        "listing.lowestCycle\216\001\000\000\000\000\247\375I\000\000\000\000\000\000\034\000\000\000\271\020"
        "listing.modCount\217\247\001\000\000\000\000\000\000\000$\000\000\000\271\024"
        "chronicle.write.lock\216\000\000\000\000\247\000\000\000\000\000\000\000\200,\000\000\000\271\035"
        "chronicle.lastIndexReplicated\217\217\217\217\247\377\377\377\377\377\377\377\3774\000\000\000\271)"
        "chronicle.lastAcknowledgedIndexReplicated\247\377\377\377\377\377\377\377\377";

    wirepad_t* pad = wirepad_init(1024);

    // single metadata message
    wirepad_qc_start(pad, 1);
    wirepad_event_name(pad, "header");
    wirepad_type_prefix(pad, "STStore");
    wirepad_nest_enter(pad); //header
      wirepad_field_type_enum(pad, "wireType", "WireType", "BINARY_LIGHT");
      // field metadata, type prefix SCQMeta, nesting begin
      wirepad_field(pad, "metadata");
      wirepad_type_prefix(pad, "SCQMeta");
      wirepad_nest_enter(pad);
        wirepad_field(pad, "roll");
        wirepad_type_prefix(pad, "SCQSRoll");
        wirepad_nest_enter(pad);
          wirepad_field_varint(pad, "length", 86400000);
          wirepad_field_text(pad, "format", "yyyyMMdd'F'");
          wirepad_field_varint(pad, "epoch", 0);
        wirepad_nest_exit(pad);
        wirepad_field_varint(pad, "deltaCheckpointInterval", 64);
        wirepad_field_varint(pad, "sourceId", 0);
      wirepad_nest_exit(pad);
      wirepad_pad_to_x8(pad); // feels wrong - should be automatic?
      wirepad_nest_exit(pad);
    wirepad_qc_finish(pad);

    // 6 data messages
    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.highestCycle");
    wirepad_uint64_aligned(pad, 18941); // this cannot be varint as memory mapped! explicit size
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.lowestCycle");
    wirepad_uint64_aligned(pad, 18941);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "listing.modCount");
    wirepad_uint64_aligned(pad, 1);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.write.lock");
    wirepad_uint64_aligned(pad, 0x8000000000000000);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.lastIndexReplicated");
    wirepad_uint64_aligned(pad, -1);
    wirepad_qc_finish(pad);

    wirepad_qc_start(pad, 0);
    wirepad_event_name(pad, "chronicle.lastAcknowledgedIndexReplicated");
    wirepad_uint64_aligned(pad, -1);
    wirepad_qc_finish(pad);

    assert_memory_equal(buf, wirepad_base(pad), wirepad_sizeof(pad));

    wirepad_free(pad);

}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_wirepad_text),
        cmocka_unit_test(test_wirepad_fields),
        cmocka_unit_test(test_wirepad_metadata),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
