// Copyright 2022 Tea Engineering Ltd.
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

// allows reading and writing K objects to a libchronicle queue
// link with either KDBs exported functions or mock_k.h
//
// sample use:
//
// queue_t* queue = chronicle_init(dirs);
// chronicle_set_decoder(queue, &parse_kx, &free_kx);
// chronicle_set_encoder(queue, &sizeof_kx, &append_kx);

#include "libchronicle.h"

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
void append_kx(unsigned char* base, COBJ msg, size_t lim) {
    K m = (K)msg;
    memcpy(base, (char*)m->G0, m->n);
}

size_t sizeof_kx(COBJ msg) {
    // if (debug) printf("shmipc: kx persist needs %lld bytes\n", msg->n);
    K m = (K)msg;
    return m->n;
}

void free_kx(COBJ msg) {
    K m = (K)msg;
    r0(m);
}

