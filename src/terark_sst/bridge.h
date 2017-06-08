
#pragma once

#ifdef __cplusplus
 #define EXTERNC extern "C"
#else
 #define EXTERNC
#endif

EXTERNC void terark_doit(const char* tag, const char* str, int len);

#undef EXTERNC



