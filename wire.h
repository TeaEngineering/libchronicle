
typedef struct {
    void (*event_name)(char*,int,void*);
    void (*event_uint64)(char*,int,uint64_t,void*);
    void (*type_prefix)(char*,int,void*);
} wirecallbacks_t;

int read_stop_uint(unsigned char* p, uint64_t *stopsz) {
    *stopsz = 0;
    int n = 0;
    do {
        n++;
        *stopsz = (*stopsz << 7) + (p[n-1] & 0x7F);
    } while ((p[n-1] & 0x80) != 0x00);
    printf("stopsz %llu bytes %d\n", *stopsz, n);
    return n;
}

// used for reading index and header structures as well as directory-index.cq4t
void parse_wire(unsigned char* base, int n, wirecallbacks_t cbs, void* userdata) {
    // constants from
    // https://github.com/OpenHFT/Chronicle-Wire/blob/master/src/main/java/net/openhft/chronicle/wire/BinaryWireCode.java
    unsigned char* p = base;
    uint8_t control;

    char* ev_name = NULL;
    uint64_t ev_name_sz = 0;

    char* type_name = NULL;
    uint64_t type_name_sz = 0;

    uint64_t jlong = 0;
    uint32_t padding32 = 0;

    uint64_t stopsz = 0;

    while (p-base < n) {
        control = p[0]; p++;
        // printf(" control 0x%02x\n", control);
        switch(control) {
            case 0xB9: // EVENT_NAME
                p+= read_stop_uint(p, &ev_name_sz);
                ev_name = (char*)p;
                p += ev_name_sz;
                if (cbs.event_name) cbs.event_name(ev_name, ev_name_sz, userdata);
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
                printf(" BYTES_LENGTH32 len %d\n", padding32);
                // BracketType is Seq, Map or None
                // we can either skip the object with length or decend into it
                break;
            case 0xA7: // INT64
                memcpy(&jlong, p, sizeof(jlong));
                printf("    %.*s = %llu\n", ev_name_sz, ev_name, jlong);
                p += 8;
                if (cbs.event_uint64) cbs.event_uint64(ev_name, ev_name_sz, jlong, userdata);
                break;
            case 0xB6: // TYPE_PREFIX
                p+= read_stop_uint(p, &type_name_sz);
                type_name = (char*)p;
                p += type_name_sz;
                printf("Type prefix !%.*s\n", type_name_sz, type_name);
                if (cbs.type_prefix) cbs.type_prefix(type_name, type_name_sz, userdata);
                break;
            case 0xC0 ... 0xDF: // Small Field, UTF length encoded in control
                ev_name = (char*)p;
                ev_name_sz = control - 0xC0;
                printf("Small field len %d: %.*s\n", ev_name_sz, ev_name_sz, ev_name);
                // TODO: for UTF characters in utflen, we need to skip additional bytes here
                // see BytesInternal.parse8bit_SB1
                p += ev_name_sz;
                break;
            case 0xE0 ... 0xFF: // Text field value
                ev_name = (char*)p;
                ev_name_sz = control - 0xE0;
                printf("Small text len %d: %.*s\n", ev_name_sz, ev_name_sz, ev_name);
                p += ev_name_sz;
                break;
            default:
                printf("Aborted at %p (+%04lx) unknown control word %d 0x%02x\n", p-1, p-base, control, control);
                p = base + n;
                break;
        }
    }
}
