#ifndef __TEST_H
#define __TEST_H 1

#include <stdlib.h>

#define DEFAULT_PORT    3344
#define DEBUG(n) if(debug >= (n)) warn

extern int debug;
void test_init();

#endif // __TEST_H
