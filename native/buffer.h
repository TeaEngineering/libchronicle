// Copyright 2018 Tea Engineering Ltd.
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

static void printbuf(char* c, int n) {
    printf("'");
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
        default:
            if ((c[i] < 0x20) || (c[i] > 0x7f)) {
                printf("\\%03o", (unsigned char)c[i]);
            } else {
                printf("%c", c[i]);
            }
        break;
      }
    }
    printf("'\n");
}
