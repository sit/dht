#ifndef INCL_UTIL
#define INCL_UTIL

#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif
#ifndef MAX_INT
#define MAX_INT  0x7fffffff
#endif

int isGreater(int a, int b, int numBits);
int isGreaterOrEqual(int a, int b, int numBits);
int between(int x, int a, int b, int numBits);
int initRand(unsigned seed);
double funifRand(double a, double b);
int unifRand(int a, int b);
double fExp(double mean);
void panic(char *str);

#endif /* INCL_UTIL */
