
#include <iostream>
#include "bridge.h"

void terark_doit(const char* tag, const char* str, int len) {
    printf("%s: %.*s\n", tag, len, str);
}
