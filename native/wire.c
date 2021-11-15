// Copyright 2021 Tea Engineering Ltd.
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <wire.h>
#include <buffer.h>

// enables full printf tracing during parsing of wire data (slow)
int wire_trace = 0;


int read_stop_uint(unsigned char* p, int *stopsz) {
    *stopsz = 0;
    int n = 0;
    do {
        n++;
        *stopsz = (*stopsz << 7) + (p[n-1] & 0x7F);
    } while ((p[n-1] & 0x80) != 0x00);
    // printf("stopsz %d bytes %d\n", *stopsz, n);
    return n;
}

// used for reading index and header structures as well as directory-index.cq4t
void parse_wire(unsigned char* base, int lim, wirecallbacks_t* cbs) {
    // constants from
    // https://github.com/OpenHFT/Chronicle-Wire/blob/master/src/main/java/net/openhft/chronicle/wire/BinaryWireCode.java
    // decoder details
    // https://github.com/OpenHFT/Chronicle-Wire/blob/ea/src/main/java/net/openhft/chronicle/wire/BinaryWire.java
    unsigned char* p = base;
    uint8_t control;

    char* field_name = NULL;
    int   field_name_sz = 0;
    char* ev_name = NULL;
    int   ev_name_sz = 0;
    char* type_name = NULL;
    int   type_name_sz = 0;
    char* field_text = NULL;
    int   field_text_sz = 0;

    uint16_t padding16 = 0;
    uint32_t padding32 = 0;
    uint64_t padding64 = 0;
    uint64_t jlong2 = 0;
    float float32 = 0;

    // track nesting stack
    int nest = 0;
    unsigned char* pop_pos[10];
    pop_pos[nest] = base+lim;
    if (cbs->reset_nesting) cbs->reset_nesting();

    while (p < base + lim) {
        control = p[0]; p++;
        if (wire_trace) printf(" control 0x%02x\n", control);
        switch(control) {
            case 0x00 ... 0x7F: // NUM
                if (wire_trace) printf(" Field %.*s = %d (uint8)\n", (int)field_name_sz, field_name, control);
                if (cbs->field_uint8) cbs->field_uint8(field_name, field_name_sz, control, cbs);
                break;
            case 0xB9: // EVENT_NAME
                p+= read_stop_uint(p, &ev_name_sz);
                ev_name = (char*)p;
                field_name = (char*)p;
                field_name_sz = ev_name_sz;
                p += ev_name_sz;
                if (cbs->event_name) cbs->event_name(ev_name, ev_name_sz, cbs);
                if (wire_trace) printf("Event name '%.*s'\n", ev_name_sz, ev_name);
                break;
            case 0x8F: // PADDING
                break;
            case 0x8E: // PADDING_32
                memcpy(&padding32, p, sizeof(padding32));
                p += 4 + padding32;
                break;
            case 0x82: // BYTES_LENGTH32, introduces nested structure with length
                memcpy(&padding32, p, sizeof(padding32));
                p += 4;
                if (wire_trace) printf(" Enter nesting '%.*s' for %d bytes\n", field_name_sz, field_name, padding32);
                pop_pos[++nest] = p+padding32;
                break;
            case 0x8D: // I64_ARRAY
                // length/used a, then length*64-bit ints. used for index structured, so expose ptr
                memcpy(&padding64, p, sizeof(padding64));
                memcpy(&jlong2, p+8, sizeof(jlong2));
                if (wire_trace) printf(" Field %.*s = [...] (I64_ARRAY) used %" PRIu64 "/%" PRIu64 "\n", field_name_sz, field_name, jlong2, padding64);
                p += 16;
                if (cbs->ptr_uint64arr) cbs->ptr_uint64arr(field_name, field_name_sz, jlong2, padding64, p, cbs);
                p += 8*padding64;
                break;
            case 0x90: // FLOAT32
                memcpy(&float32, p, sizeof(float32));
                if (wire_trace) printf(" Field %.*s = %f (FLOAT32)\n", field_name_sz, field_name, float32);
                // if (cbs->field_float) cbs->field_float(field_name, field_name_sz, float32, cbs);
                p += 4;
                break;
            //case 0x91: // FLOAT64
            //    break;
            case 0xA5: // INT16
                memcpy(&padding16, p, sizeof(padding16));
                if (wire_trace) printf(" Field %.*s = %hu (uint16)\n", (int)field_name_sz, field_name, padding16);
                if (cbs->field_uint16) cbs->field_uint16(field_name, field_name_sz, padding16, cbs);
                p += 2;
                break;
            case 0xA6: // INT32
                memcpy(&padding32, p, sizeof(padding32));
                if (wire_trace) printf(" Field %.*s = %u (uint32)\n", (int)field_name_sz, field_name, padding32);
                if (cbs->field_uint32) cbs->field_uint32(field_name, field_name_sz, padding32, cbs);
                p += 4;
                break;
            case 0xA7: // INT64
                memcpy(&padding64, p, sizeof(padding64));
                if (wire_trace) printf(" Field %.*s = %" PRIu64 " (uint64)\n", field_name_sz, field_name, padding64);
                if (cbs->ptr_uint64) cbs->ptr_uint64(ev_name, ev_name_sz, p, cbs);
                if (cbs->field_uint64) cbs->field_uint64(field_name, field_name_sz, padding64, cbs);
                p += 8;
                break;
            case 0xB6: // TYPE_PREFIX
                p+= read_stop_uint(p, &type_name_sz);
                type_name = (char*)p;
                p += type_name_sz;
                if (wire_trace) printf("Type prefix !%.*s\n", type_name_sz, type_name);
                if (cbs->type_prefix) cbs->type_prefix(type_name, type_name_sz, cbs);
                break;
            case 0xC0 ... 0xDF: // Small Field start, UTF length encoded in control
                field_name = (char*)p;
                field_name_sz = control - 0xC0;
                // TODO: for UTF characters in utflen, we need to skip additional bytes here
                // see BytesInternal.parse8bit_SB1
                // printf(" Field %.*s \n", field_name_sz, field_name);
                p += field_name_sz;
                break;
            case 0xE0 ... 0xFF: // Text field value
                field_text = (char*)p;
                field_text_sz = control - 0xE0;
                if (wire_trace) printf(" Field %.*s = %.*s (text)\n", (int)field_name_sz, field_name, field_text_sz, field_text);
                if (cbs->field_char) cbs->field_char(field_name, field_name_sz, field_text, field_text_sz, cbs);
                p += field_text_sz;
                break;
            case 0xB8: // Text any length
                field_text = (char*)p;
                field_text_sz = control - 0xE0;
                if (wire_trace) printf(" Field %.*s = %.*s (text)\n", (int)field_name_sz, field_name, field_text_sz, field_text);
                if (cbs->field_char) cbs->field_char(field_name, field_name_sz, field_text, field_text_sz, cbs);
                p += field_text_sz;
                break;
            default:
                printf("Aborted at %p (+%04lx) unknown control word %d 0x%02x\n", p-1, p-base, control, control);
                p = base + lim;
                break;
        }

        while (p >= pop_pos[nest] && nest > 0) {
            nest--;
            if (wire_trace) printf(" nesting pop\n");
        }
    }
}

typedef struct wirepad {
    int      sz;
    unsigned char*    pos;
    unsigned char*    base;

    int               nest;
    unsigned char*    nest_enter_pos[10];
} wirepad_t;

wirepad_t*  wirepad_init(int initial_sz) {
    wirepad_t* pad = malloc(sizeof(wirepad_t));
    pad->sz = initial_sz;
    pad->base = NULL;
    pad->base = realloc(pad->base, pad->sz);
    if (wire_trace) printf("wire pad created at %p sz %d\n", pad->base, pad->sz);
    pad->pos = pad->base;
    pad->nest = 0;
    return pad;
}

void wirepad_free(wirepad_t* pad) {
    free(pad->base);
    free(pad);
}

void wirepad_clear(wirepad_t* pad) {
    pad->pos = pad->base;
    pad->nest = 0;
}

void wirepad_extent(wirepad_t* pad, int sz) {
    int used = pad->pos - pad->base;
    int remain = pad->sz - used - sz;
    if (wire_trace) printf(" wirepad_extent base=%p used=%d need=+%d remain=%d\n", pad->base, used, sz, remain);
    while (remain < 0) {
        pad->sz = pad->sz * 2;
        pad->base = realloc(pad->base, pad->sz);
        // pad->pos = ...
        remain = pad->sz - used - sz;
    }
}

void wirepad_text(wirepad_t* pad, char* text) {
    int d = strlen(text);
    int overhead = d < 0x1F ? 1 : 4;
    int padding = -(overhead + d) & 0x03;
    if (wire_trace) printf("wirepad_text pos=%p writing d=%d overhead=%d padding=%d\n", pad->pos, d, overhead, padding);
    wirepad_extent(pad, d+overhead+padding);
    if (d < 0x1F) {
        pad->pos[0] = 0xE0 + d;
    } else {
        pad->pos[0] = 0xB8;
        // put stop bit encoded length
        // ...
    }
    memcpy(pad->pos+overhead, text, d);
    if (padding == 1) {
        pad->pos[overhead+d+0] = 0x00;
    } else if (padding == 2) {
        pad->pos[overhead+d+0] = 0x00;
        pad->pos[overhead+d+1] = 0x00;
        //pad->pos[overhead+d];
    } else if (padding == 3) {
        pad->pos[overhead+d+0] = 0x00;
        pad->pos[overhead+d+1] = 0x00;
        pad->pos[overhead+d+2] = 0x00;
        // blah
    }
    pad->pos = pad->pos + d + overhead + padding;
}

void wirepad_field(wirepad_t* pad, char* text) {
    int d = strlen(text);
    int overhead = d < 0x1F ? 1 : 4;
    if (wire_trace) printf("wirepad_field pos=%p writing d=%d overhead=%d\n", pad->pos, d, overhead);
    wirepad_extent(pad, d+overhead);
    if (d < 0x1F) {
        pad->pos[0] = 0xC0 + d;
    } else {
        printf("long field encoding?");
        abort();
    }
    memcpy(pad->pos+overhead, text, d);
    pad->pos = pad->pos + d + overhead;
}

void wirepad_varint(wirepad_t* pad, uint64_t v) {
    if (wire_trace) printf("wirepad_varint pos=%p writing d=%" PRIu64 "\n", pad->pos, v);
    wirepad_extent(pad, 9);
    if (v <= 0xFFFF) {
        pad->pos[0] = 0xa5; // INT16
        pad->pos[1] = (v >>  0) & 0xff;
        pad->pos[2] = (v >>  8) & 0xff;
        pad->pos = pad->pos + 3;
    } else if (v <= 0xFFFFFFFF) {
        // 1234567890L => 49 96 02 D2
        // a6 d2 02 96 49
        pad->pos[0] = 0xa6; // INT32
        pad->pos[1] = (v >>  0) & 0xff;
        pad->pos[2] = (v >>  8) & 0xff;
        pad->pos[3] = (v >> 16) & 0xff;
        pad->pos[4] = (v >> 24) & 0xff;
        pad->pos = pad->pos + 5;
    } else {
        pad->pos[0] = 0xA7; // INT64
        pad->pos[1] = (v >>  0) & 0xff;
        pad->pos[2] = (v >>  8) & 0xff;
        pad->pos[3] = (v >> 16) & 0xff;
        pad->pos[4] = (v >> 24) & 0xff;
        pad->pos[5] = (v >> 32) & 0xff;
        pad->pos[6] = (v >> 40) & 0xff;
        pad->pos[7] = (v >> 48) & 0xff;
        pad->pos[8] = (v >> 56) & 0xff;
        pad->pos = pad->pos + 9;
    }

}

void wirepad_field_text(wirepad_t* pad, char* field, char* text) {
    wirepad_field(pad, field);
    wirepad_text(pad, text);
}

void wirepad_field_enum(wirepad_t* pad, char* field, char* text) {
    wirepad_field(pad, field);
    wirepad_text(pad, text);
}

void wirepad_field_int64(wirepad_t* pad, char* field, uint64_t v) {
    wirepad_field(pad, field);
    wirepad_varint(pad, v);
}

void wirepad_field_float64(wirepad_t* pad, char* field, double v) {
    wirepad_field(pad, field);
    float f = v;
    if (f == v) {
        pad->pos[0] = 0x90; // FLOAT32
        memcpy(pad->pos+1, &f, sizeof(f));
        pad->pos = pad->pos + 5;
    } else {
        printf("float64 large encoding?");
        abort();
    }

}

void wirepad_field_uint8(wirepad_t* pad, char* field, int v) {
    wirepad_field(pad, field);
}

void wirepad_field_uint32(wirepad_t* pad, char* field, int v) {
    wirepad_field(pad, field);
}

void wirepad_event_name(wirepad_t* pad, char* event_name) {

}

void wirepad_type_prefix(wirepad_t* pad, char* type_prefix) {

}


void wirepad_nest_enter(wirepad_t* pad, char* nest_name) {

}

void wirepad_nest_exit(wirepad_t* pad) {

}

void wirepad_parse(wirepad_t* pad, wirecallbacks_t* cbs) {
    parse_wire(pad->base, pad->pos-pad->base, cbs);
}

void wirepad_dump(wirepad_t* pad) {
    int n = pad->pos-pad->base;
    printf("wirepad_dump base=%p pos=%p (+%d) sz=%d\n", pad->base, pad->pos, n, pad->sz);
    printbuf((char *)pad->base, n);
    char* p = formatbuf((char*)pad->base, n);
    printf("%s\n", p);
    free(p);
}

char* wirepad_hexformat(wirepad_t* pad) {
    int n = pad->pos-pad->base;
    return formatbuf((char*)pad->base, n);
}


// sizeof and wrote take a wirepad_t argument, cast down to void* here for ease of use with libchronicle
long wirepad_sizeof(void* msg) {
    wirepad_t* pad = (wirepad_t*)msg;
    return pad->pos - pad->base;
}

long wirepad_write(unsigned char* base, int sz, void* msg) {
    wirepad_t* pad = (wirepad_t*)msg;
    return pad->pos - pad->base;
}

