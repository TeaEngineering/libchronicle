#include <libchronicle.h>
#include <stdarg.h>
#include <signal.h>

static volatile int keepRunning = 1;

// This is a stand-alone tool for reading a queue
// queue data is null-terminated strings, embedded nulls will truncate printing
void* parse_msg(unsigned char* base, int lim) {
    char* msg = calloc(1, lim+1);
    memcpy(msg, base, lim);
    return msg;
}

void free_msg(void* msg) {
    free(msg);
}

int print_msg(void* ctx, uint64_t index, void* msg) {
    printf("[%" PRIu64 "] %s\n", index, (char*)msg);
    return 0;
}

void sigint_handler(int dummy) {
    keepRunning = 0;
}

int main(const int argc, char **argv) {
    signal(SIGINT, sigint_handler);
    queue_t* queue = chronicle_init(argv[1]);
    chronicle_set_decoder(queue, &parse_msg, &free_msg);
    if (chronicle_open(queue) != 0) exit(-1);
    chronicle_tailer(queue, &print_msg, NULL, 0);

    while (keepRunning) {
        usleep(500*1000);
        chronicle_peek();
    }
    printf("exiting\n");
    chronicle_cleanup(queue);
}

