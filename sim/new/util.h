#ifndef INCL_UTIL
#define INCL_UTIL

/* functions in util.c */
int   initRand(unsigned seed);
void  initVecRand();
double funifRand(double a, double b);
double fExp(double mean);
int    unifRand(int a, int b);
int    intExp(int mean);
double fPareto(double alpha);
double fParetoMean(double mean);
double funifVecRand(double a, double b);
double fVecExp(double mean);
double fVecParetoMean(double mean);
void panic(char *str);

#endif /* INCL_UTIL */
