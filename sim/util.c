#include <stdio.h>
#include <malloc.h>
#include <math.h>

#include "incl.h"

#define MAX_RAND_VEC 50000
int RandVec[MAX_RAND_VEC];

/* Initialize and array and pass it in to initstate. */
static long state1[32] = {
  3,
  0x9a319039, 0x32d9c024, 0x9b663182, 0x5da1f342,
  0x7449e56b, 0xbeb1dbb0, 0xab5c5918, 0x946554fd,
  0x8c2e680f, 0xeb3d799f, 0xb11ee0b7, 0x2d436b86,
  0xda672e2a, 0x1588ca88, 0xe369735d, 0x904f35f7,
  0xd7158fd6, 0x6fa6f051, 0x616e6b96, 0xac94efdc,
  0xde3b81e0, 0xdf0a6fb5, 0xf103bc02, 0x48f340fb,
  0x36413f93, 0xc622c298, 0xf5a42ab8, 0x8a88d77b,
  0xf5ad9d0e, 0x8999220b, 0x27fb47b9
};
 
int initRand(unsigned seed)
{
  int n;
 
  n = 128;
  initstate(seed, (char *) state1, n);
  setstate((char *)state1);
}
 

void initRandVec()
{
  int i;

  for (i = 0; i < MAX_RAND_VEC; i++) 
    RandVec[i] = random();
}


int getRandVec()
{
  static int i = 0;
  int res = RandVec[i];

  if (++i == MAX_RAND_VEC)
    i = 0;
  
  return res;
}


double funifRand(double a, double b)
{
  int c;
  double f =  (b - a)*((double)(c = random()))/((double)MAX_INT);
 
  return a + f;
}   

/* return a random integer number in the interval [a, b) */
int unifRand(int a, int b)
{
  double f;
#ifdef UNIX
  int c = random();
#else
  int c = rand();
#endif /* UNIX */

  if (c == MAX_INT) c--;
  f = (b - a)*((double)c)/((double)MAX_INT);

  return (int)(a + f);
}

double funifVecRand(double a, double b)
{
  int c;
  double f =  (b - a)*((double)(c = getRandVec()))/((double)MAX_INT);
 
  return a + f;
}   

double fPareto(double alpha)
{
#ifdef PARETO_TO_EXP
  return fExp(alpha/(alpha - 1));
#else
  return 1.0/pow(funifRand(0., 1.), 1.0/alpha);
#endif
}   


double fParetoMean(double mean)
{
  double a;

  if (mean <= 1.0)
    panic("fPareto: mean should be larger than 1.0.\n");

  a = mean/(mean - 1);

  return 1.0/pow(funifRand(0., 1.), 1.0/a);
}   


double fVecParetoMean(double mean)
{
  double a;

  if (mean <= 1.0)
    panic("fPareto: mean should be larger than 1.0.\n");

  a = mean/(mean - 1);

  return 1.0/pow(funifVecRand(0., 1.), 1.0/a);
}   


double fExp(double mean)
{
  double u;

  while ((u = funifRand(0., 1.)) == 0.0);

  return -mean * log(u);
}   

int intExp(int mean)
{
  int res = (int)fExp((int)mean);

  if (res < 0)
    return MAX_INT;
  else
    return res;
}   


double fVecExp(double mean)
{
  double u;

  while ((u = funifVecRand(0., 1.)) == 0.0);

  return -mean * log(u);
}   

void panic(char *str)
{ 
  printf(str);
  exit(-1);
}
 
