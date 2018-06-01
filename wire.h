
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
    void (*ptr_uint64arr)(char*,int,uint64_t,uint64_t,uint64_t*,struct wirecallbacks*);

    void (*reset_nesting)();

    void* userdata;
} wirecallbacks_t;

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
void parse_wire(unsigned char* base, int n, wirecallbacks_t* cbs) {
    // constants from
    // https://github.com/OpenHFT/Chronicle-Wire/blob/master/src/main/java/net/openhft/chronicle/wire/BinaryWireCode.java
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

    int stopsz = 0;

    // track nesting stack
    int nest = 0;
    unsigned char* pop_pos[10];
    pop_pos[nest] = base+n;
    if (cbs->reset_nesting) cbs->reset_nesting();

    while (p < base + n) {
        control = p[0]; p++;
        // printf(" control 0x%02x\n", control);
        switch(control) {
            case 0x00 ... 0x7F: // NUM
                printf(" Field %.*s = %d (uint8)\n", (int)field_name_sz, field_name, control);
                if (cbs->field_uint8) cbs->field_uint8(field_name, field_name_sz, control, cbs);
                break;
            case 0xB9: // EVENT_NAME
                p+= read_stop_uint(p, &ev_name_sz);
                ev_name = (char*)p;
                field_name = (char*)p;
                field_name_sz = ev_name_sz;
                p += ev_name_sz;
                if (cbs->event_name) cbs->event_name(ev_name, ev_name_sz, cbs);
                printf("Event name %.*s\n", ev_name_sz, ev_name);
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
                printf(" Enter nesting '%.*s' for %d bytes\n", field_name_sz, field_name, padding32);
                pop_pos[++nest] = p+padding32;
                break;
            case 0x8D: // I64_ARRAY
                // length/used a, then length*64-bit ints. used for index structured, so expose ptr
                memcpy(&padding64, p, sizeof(padding64));
                memcpy(&jlong2, p+8, sizeof(jlong2));
                printf(" Field %.*s = [...] (I64_ARRAY) used %llu/%llu\n", field_name_sz, field_name, jlong2, padding64);
                p += 16;
                if (cbs->ptr_uint64arr) cbs->ptr_uint64arr(field_name, field_name_sz, jlong2, padding64, p, cbs);
                p += 8*padding64;
                break;
            case 0xA5: // INT16
                memcpy(&padding16, p, sizeof(padding16));
                printf(" Field %.*s = %hu (uint16)\n", (int)field_name_sz, field_name, padding16);
                if (cbs->field_uint16) cbs->field_uint16(field_name, field_name_sz, padding16, cbs);
                p += 2;
                break;
            case 0xA6: // INT32
                memcpy(&padding32, p, sizeof(padding32));
                printf(" Field %.*s = %u (uint32)\n", (int)field_name_sz, field_name, padding32);
                if (cbs->field_uint32) cbs->field_uint32(field_name, field_name_sz, padding32, cbs);
                p += 4;
                break;
            case 0xA7: // INT64
                memcpy(&padding64, p, sizeof(padding64));
                printf(" Field %.*s = %llu\n", field_name_sz, field_name, padding64);
                if (cbs->ptr_uint64) cbs->ptr_uint64(ev_name, ev_name_sz, p, cbs);
                if (cbs->field_uint64) cbs->field_uint64(field_name, field_name_sz, padding64, cbs);
                p += 8;
                break;
            case 0xB6: // TYPE_PREFIX
                p+= read_stop_uint(p, &type_name_sz);
                type_name = (char*)p;
                p += type_name_sz;
                printf("Type prefix !%.*s\n", type_name_sz, type_name);
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
                printf(" Field %.*s = %.*s (text)\n", (int)field_name_sz, field_name, field_text_sz, field_text);
                if (cbs->field_char) cbs->field_char(field_name, field_name_sz, field_text, field_text_sz, cbs);
                p += field_text_sz;
                break;
            default:
                printf("Aborted at %p (+%04lx) unknown control word %d 0x%02x\n", p-1, p-base, control, control);
                p = base + n;
                break;
        }

        while (p >= pop_pos[nest] && nest > 0) {
            nest--;
            printf(" nesting pop\n");
        }
    }
}

void parse_data_text(unsigned char* base, int n, void* userdata) {

    unsigned char* p = base;
    uint32_t sz2 = 0;
    while (p < base+n) {
        memcpy(&sz2, p, sizeof(sz2));
        printf(" text: %.*s\n", sz2, p+4);
        p += 4 + sz2;
    }
}
