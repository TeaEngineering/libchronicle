#include "shmipc.h"
#include "mock_k.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "ocamlshmipc.c"

/*
A simple debugging tool for ocamlshmipc
*/


void cb_test(uint64_t index, const char* data, uint64_t len)
{
    printf("\t\tcb_test index:%llu len:%llu data:'%.*s'\n", index, len, len, data);
}

int main(const int argc, char** argv)
{
    char* dir = ":/home/jonathan/dev/git/c-chronicle-q/queue";
    OCAMLshmipc_open_and_poll(dir, &cb_test);
    return 0;
}