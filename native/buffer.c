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

#ifndef FILE_LIBBUFFER_SEEN
#define FILE_LIBBUFFER_SEEN


#include <stdlib.h>
#include <stdio.h>

// Outputs non-printable characters as octal, which allows the resulting
// string to be a valid C-string constant.
void printbuf(char* c, int n) {
    printf("unsigned char* buf=\"");
    for (int i = 0; i < n; i++) {
    switch (c[i]) {

        case '\n':
            printf("\\n");
            break;
        case '\r':
            printf("\\r");
            break;
        case '\t':
            printf("\\t");
            break;
        case '\\':
            printf("\\\\");
            break;
        default:
            if ((c[i] < 0x20) || (c[i] > 0x7f)) {
                printf("\\%03o", (unsigned char)c[i]);
            } else {
                printf("%c", c[i]);
            }
        break;
      }
    }
    printf("\"\n");
}


char* formatbuf(char* buf, int sz) {
    // nicely format a buffer to hex/ascii with a length offset. each 16 bytes
    // or part thereof of the input take 76 bytes include newline character. caller
    // must free() the buffer.
    //
    // 00000000 c7 6d 65 73 73 61 67 65  eb 48 65 6c 6c 6f 20 57 ·message ·Hello W
    // 00000010 6f 72 6c 64 c6 6e 75 6d  62 65 72 a6 d2 02 96 49 orld·num ber····I
    // 00000020 c4 63 6f 64 65 e7 53 45  43 4f 4e 44 53 c5 70 72 ·code·SE CONDS·pr
    // 00000030 69 63 65 90 00 00 28 41                          ice···(A
    //
    // ----8--- -------------------------48--------------------- --------17-------\n
    const char* hexd = "0123456789abcdef";
    int lines = (sz + (16-1)) / 16; // sz/16 rounded up!
    if (lines == 0) lines++;
    int osz   = lines * 76 + 1; // +trailing null
    char* r = malloc(osz);
    for (int i = 0; i < lines; i++) {
        // this provides our trailing null!
        sprintf(r+i*76, "%08x %48s %17s\n", i*16, "", "");
        for (int j = i*16; j < sz && j < (i+1)*16; j++) {
            int c = j % 16;
            int e = c > 7 ? 1 : 0;
            r[i*76+9 +c*3+e] = hexd[(buf[j] >> 4) & 0x0F]; // upper nibble
            r[i*76+10+c*3+e] = hexd[buf[j]        & 0x0F]; // lower nibble
            // printable ascii?
            if (buf[j] >= 32 && buf[j] < 127) {
                r[i*76+9+49+c+e] = buf[j];
            } else {
                r[i*76+9+49+c+e] = '.';
            }
        }
    }
    // r[osz-1] = 0;
    return r;
}

#endif
