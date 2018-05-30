
typedef struct {
    void (*event_uint64)(char*,int,uint64_t,void*);
} wirecallbacks_t;

// used for reading index and header structures as well as directory-index.cq4t
void parse_wire(char* base, int n, wirecallbacks_t cbs, void* userdata) {
    // constants from
    // https://github.com/OpenHFT/Chronicle-Wire/blob/master/src/main/java/net/openhft/chronicle/wire/BinaryWireCode.java
    char* p = base;
    uint8_t control;

    char* ev_name = NULL;
    uint8_t ev_name_sz = 0;

    uint64_t jlong = 0;
    uint32_t padding32 = 0;

    while (p-base < n) {
        control = p[0];
        switch(control) {
            case 0xB9: // EVENT_NAME
                ev_name = p+2;
                ev_name_sz = p[1];
                p += 2 + ev_name_sz;
                break;
            case 0x8F: // PADDING
                p++;
                break;
            case 0x8E: // PADDING_32
                memcpy(&padding32, p+1, sizeof(padding32));
                p += 1 + 4 + padding32;
                break;
            case 0xA7: // INT64
                memcpy(&jlong, p+1, sizeof(jlong));
                printf("    %.*s = %llu\n", ev_name_sz, ev_name, jlong);
                p += 1 + 8;
                cbs.event_uint64(ev_name, ev_name_sz, jlong, userdata);
                break;
            default:
                printf("Aborted DATA unknown control %d %02x\n", control, control);
                p = base + n;
                break;
        }
    }
}
