#ifndef SHIMPC
#define SHIMPC

#include <stdint.h>

#include "k.h"

//typedef struct tailer tailer_t;
typedef struct queue queue_t;

typedef int (*parsedata_f)(unsigned char*,int,uint64_t,void* userdata);
typedef int (*appenddata_f)(unsigned char*,int,int*,K);
typedef K (*encodecheck_f)(struct queue*,K);

int parse_data_text(unsigned char* base, int lim, uint64_t index, void* userdata);
int append_data_text(unsigned char* base, int lim, int* sz, K msg);
K append_check_text(queue_t* queue, K msg);

int parse_data_kx(unsigned char* base, int lim, uint64_t index, void* userdata);
int append_data_kx(unsigned char* base, int lim, int* sz, K msg);
K append_check_kx(queue_t* queue, K msg);


K shmipc_init(K dir, parsedata_f* parser, appenddata_f* appender, encodecheck_f* encoder);

K shmipc_peek(K x);

K shmipc_tailer(K dir, K cb, K kindex);

K shmipc_debug(K x);

K shmipc_close(K dir);

#endif