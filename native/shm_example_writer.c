#include <libchronicle.h>
#include <stdarg.h>

// This is a stand-alone tool for writing a queue
// queue data is null-terminated strings, embedded nulls will truncate printing
void append_msg(unsigned char* base, void* msg, size_t sz) {
    memcpy(base, msg, sz);
}

size_t sizeof_msg(void* msg) {
    return strlen(msg);
}

int main(const int argc, char **argv) {
    queue_t* queue = chronicle_init(argv[1]);
    chronicle_set_encoder(queue, &sizeof_msg, &append_msg);
    chronicle_set_version(queue, 5);
    chronicle_set_roll_scheme(queue, "FAST_HOURLY");
    chronicle_set_create(queue, 1);
    if (chronicle_open(queue) != 0) exit(-1);

    char line[1024];
    while (1) {
        char* g = fgets(line, 1024, stdin);
        if (g == NULL) break;
        line[strlen(line) - 1] = 0; // remove line break
        long int index = chronicle_append(queue, line);
        printf("[%" PRIu64 "] %s\n", index, (char*)g);
    }
    chronicle_cleanup(queue);
}
