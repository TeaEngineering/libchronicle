
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
