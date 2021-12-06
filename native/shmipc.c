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

//
// This file builds a shared-object "shmipc" for easy integration of
// libchronicle with the KDB/q intepreter from Kx.
// It does not incorporate mock_k.h as the real
// implementation of those symbols is provided by the KDB runtime.
//
// The KDB calling convention requires all functions to return a K object
// and all arguments are provided as K objects, so these wrappers take care
// of that.
//
// We provide a simple way to select the protocol of the queue, either
//  "text", "raw", "kx" or "wire".
//
// *text* writes bytes directly to the queue data messages, requiring and
// returning messages bytes as characters in type KG
//
// *raw* writes and returns byte arrays of type KB - this is compatable with
// anything Java might produce, however will require some unpickling.
//
// *kx* uses d9/b9 to use KDB's built in serialisers. This is your best bet
// if serialising between two KDB processes, or if you want to use Kx objects
// tables etc. Remember the max data size is 1GB.
//
// *wire* uses wire.h to write some KDB values using the Chronicle-Wire
// "self-describing" format. This limits what you can do but is easiest for
// cross-platform compatability.
//
//  see bindings/kdb for .q harnesses and tests

#define _GNU_SOURCE
#define KERR -128

#define KXVER 3
#include "k.h"

#include <stdarg.h>
#include <ctype.h>
#include <libchronicle.h>

// Usual KDB indirection
// return handle integer to index structure
tailer_t** tailer_handles;
int tailer_handles_count = 0;
queue_t** queue_handles;
int queue_handles_count = 0;

COBJ parse_kx(unsigned char* base, int lim) {
    // prep args and fire callback
    K msg = ktn(KG, lim);
    memcpy((char*)msg->G0, base, lim);
    int ok = okx(msg);
    if (ok) {
        K out = d9(msg);
        r0(msg);
        return out;
    } else {
        // if (debug) printf("shmipc: caution index okx returned bytes !ok, skipping\n");
        return NULL;
    }
    return 0;
}

// the encoding via. b9 happens in shmipc_append, so here we just
// write the bytes
size_t append_kx(unsigned char* base, size_t lim, COBJ msg) {
    K m = (K)msg;
    memcpy(base, (char*)m->G0, m->n);
    return m->n;
}

size_t sizeof_kx(COBJ msg) {
    // if (debug) printf("shmipc: kx persist needs %lld bytes\n", msg->n);
    K m = (K)msg;
    return m->n;
}

int tailer_callback_kx(void* dispatch_ctx, uint64_t index, COBJ m) {
    K msg = (K)m;
    K arg = knk(2, kj(index), msg);

    K r = dot((K)dispatch_ctx, arg);
    r0(arg);
    if (r == NULL) {
        printf(" shmipc: caution, error signalled by callback (returned NULL)\n");
        return 0;
    } else if (r && r->t == KERR) {
        printf(" shmipc: callback error string: %s\n", r->s);
    }
    r0(r);
    return 0;
}

K shmipc_init(K dir, K parser) {
    if (dir->t != -KS) return krr("dir is not symbol");
    if (dir->s[0] != ':') return krr("dir is not symbol handle (starts with :)");

    // if (strncmp(parser->s, "text", parser->n) == 0) {
    //     queue->parser = &parse_text;
    //     queue->encoder = &append_text;
    //     queue->encodecheck = &sizeof_text;
    // } else if (strncmp(parser->s, "kx", parser->n) == 0) {
    //     queue->parser = &parse_kx;
    //     queue->encoder = &append_kx;
    //     queue->encodecheck = &sizeof_kx;
    // } else {
    //     return krr("bad format: supports `kx and `text");
    // }

    queue_t* queue;
    char* dirs = &dir->s[1];
    if (strncmp(parser->s, "text", parser->n) == 0) {
        queue = chronicle_init(dirs, &parse_kx, &sizeof_kx, &append_kx);
    } else if (strncmp(parser->s, "kx", parser->n) == 0) {
        queue = chronicle_init(dirs, &parse_kx, &sizeof_kx, &append_kx);
    } else {
        return krr("bad format: supports `kx and `text");
    }

    int handle = -1;
    if (queue) {
        // maintain array for index lookup
        handle = queue_handles_count;
        queue_handles = realloc(queue_handles, ++queue_handles_count * sizeof(queue_t*));
        queue_handles[handle] = queue;
    }
    return ki(handle);
}

K shmipc_append_ts(K queuei, K msg, K ms) {
    if (queuei->t != -KI) return krr("queue is not int");
    if (queuei->i < 0 || queuei->i >= queue_handles_count) return krr("queue out of range");
    queue_t *queue = queue_handles[queuei->i];
    if (queue == NULL) return krr("queue closed");

    if (ms != NULL && ms->t != -KJ) return krr("ms NULL or J milliseconds");

    long millis = -1;
    if (ms) millis = ms->j;

    // convert msg based on the format selected
    // TODO: support text etc etc?
    K enc = b9(3, msg);
    if (enc == NULL) {
        return krr("shmipc: failed to serialise msg using b9");
    }

    int j = chronicle_append_ts(queue, enc, millis);
    // TODO check return error
    r0(enc);
    return kj(j);
}

K shmipc_append(K queuei, K msg) {
    return shmipc_append_ts(queuei, msg, NULL);
}

K shmipc_tailer(K queuei, K callback, K index) {
    if (queuei->t != -KI) return krr("queue is not int");
    if (queuei->i < 0 || queuei->i >= queue_handles_count) return krr("queue out of range");
    queue_t *queue = queue_handles[queuei->i];
    if (queue == NULL) return krr("queue closed");

    if (index->t != -KJ) return krr("index must be J");
    // if (callback-> != function) return krr("dispatcher is not a callback");

    tailer_t* tailer = chronicle_tailer(queue, &tailer_callback_kx, callback, index->j);

    // maintain array for index lookup
    int handle = tailer_handles_count;
    tailer_handles = realloc(tailer_handles, ++tailer_handles_count * sizeof(tailer_t*));
    tailer_handles[handle] = tailer;
    K r = ki(handle);

    r0(index);
    return r;
}

K shmipc_collect(K taileri) {
    if (taileri->t != -KI) return krr("idx is not int");
    if (taileri->i < 0 || taileri->i >= tailer_handles_count) return krr("idx out of range");
    tailer_t* tailer = tailer_handles[taileri->i];
    collected_t result;

    K r = chronicle_collect(tailer, &result);
    return r;
}

K shmipc_close(K queuei) {
    if (queuei->t != -KI) return krr("queue is not int");
    if (queuei->i < 0 || queuei->i >= queue_handles_count) return krr("queue out of range");
    queue_t *queue = queue_handles[queuei->i];
    if (queue == NULL) return krr("queue already closed");

    queue_handles[queuei->i] = NULL;
    int r = chronicle_close(queue);

    return kj(r);
}

K shmipc_peek(K x) {
    chronicle_peek();
    return (K)0;
}
