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

#ifndef FILE_WIRE_SEEN
#define FILE_WIRE_SEEN

#include <sys/types.h>
#include <stdint.h>

// this library implements the "BinaryWire" self-describing wire protocol
// used for chronicle-queue metadata messages.


// global options - debug trace
extern int wire_trace;

// wire reader - set callbacks then invoke parse_wire
typedef struct wirecallbacks {
    void (*event_name)(char*,int,struct wirecallbacks*);
    void (*type_prefix)(char*,int,struct wirecallbacks*);

    void (*field_uint8)(char*,int,uint8_t,struct wirecallbacks*);
    void (*field_uint16)(char*,int,uint16_t,struct wirecallbacks*);
    void (*field_uint32)(char*,int,uint32_t,struct wirecallbacks*);
    void (*field_uint64)(char*,int,uint64_t,struct wirecallbacks*);
    void (*field_char)(char*,int,char*,int,struct wirecallbacks*);

    // direct pointer access for headers, index arrays
    void (*ptr_uint64)(char*,int,unsigned char*,struct wirecallbacks*);
    void (*ptr_uint64arr)(char*,int,uint64_t,uint64_t,unsigned char*,struct wirecallbacks*);

    void (*reset_nesting)();

    void* userdata;
} wirecallbacks_t;

void wire_parse(unsigned char* base, int lim, wirecallbacks_t* cbs);

// wire writer
// create a wirepad using wirepad_init, then write headers and fields
// print bytes wirepad_dump, write out with wirepad_write or run parse_wire
// over content with wirepad_parse
typedef struct wirepad wirepad_t;

wirepad_t*  wirepad_init(int initial_sz);
void        wirepad_clear(wirepad_t* pad);
void        wirepad_dump(wirepad_t* pad);
char*       wirepad_hexformat(wirepad_t* pad);
void        wirepad_free(wirepad_t* pad);

void        wirepad_pad_to_x8(wirepad_t* pad);
void        wirepad_pad_to_x8_00(wirepad_t* pad);
void        wirepad_event_name(wirepad_t* pad, char* event_name);
void        wirepad_type_prefix(wirepad_t* pad, char* type_prefix);

void        wirepad_text(wirepad_t* pad, char* text);
void        wirepad_uint64_aligned(wirepad_t* pad, uint64_t v);

void        wirepad_qc_start(wirepad_t* pad, int metadata);
void        wirepad_qc_finish(wirepad_t* pad);

void        wirepad_nest_enter(wirepad_t* pad);
void        wirepad_nest_exit(wirepad_t* pad);

void        wirepad_field(wirepad_t* pad, char* text);
void        wirepad_field_varint(wirepad_t* pad, char* field, int v);
void        wirepad_field_text(wirepad_t* pad, char* field, char* text);
void        wirepad_field_uint64(wirepad_t* pad, char* field, uint64_t v);
void        wirepad_field_float64(wirepad_t* pad, char* field, double v);
void        wirepad_field_enum(wirepad_t* pad, char* field, char* v);
void        wirepad_field_type_enum(wirepad_t* pad, char* field, char* type, char* v);

unsigned char* wirepad_base(wirepad_t* pad);
void        wirepad_parse(wirepad_t* pad, wirecallbacks_t* cbs);

// Integration with queues using BinaryWire for their data pages
//
// cparse_f for the common case where the wire format is just text,
// and we want to capture the text into a new object. Object must
// be freed by the callback cdispatch_f
void*       wire_parse_textonly(unsigned char* base, int lim);

// sizeof_f and append_f where the object to be written is a wirepad_t
// which is cast down to void* here for ease of use with libchronicle
size_t      wirepad_sizeof(void* pad);
size_t      wirepad_write(unsigned char* base, size_t sz, void* pad);

#endif