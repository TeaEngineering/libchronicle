#include <libchronicle.h>
#include <stdarg.h>

// This is a stand-alone tool for replaying a queue
// queue data is null-terminated strings, embedded nulls will truncate printing
void* parse_msg(unsigned char* base, int lim) {
    char* msg = calloc(1, lim+1);
    memcpy(msg, base, lim);
    return msg;
}

int append_msg(unsigned char* base, int sz, void* msg) {
    memcpy(base, msg, sz);
    return sz;
}

long sizeof_msg(void* msg) {
    return strlen(msg);
}

int print_msg(void* ctx, uint64_t index, void* msg) {
    printf("[%" PRIu64 "] %s\n", index, (char*)msg);
    return 0;
}

int main(const int argc, char **argv) {
    queue_t* queue = chronicle_init(argv[1], &parse_msg, &sizeof_msg, &append_msg);
    chronicle_tailer(queue, &print_msg, NULL, 0);
    chronicle_append(queue, "Hello World");
    while (1) {
        usleep(500*1000);
        chronicle_peek();
    }
    chronicle_close(queue);
}
