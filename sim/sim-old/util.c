#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "util.h"


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

/* 
 * check whether a is greater than b on circle; both a and b are 
 * represented on numBits bits
 */
int isGreater(int a, int b, int numBits)
{
  if (a - b > 0 && (a - b) < (1 << (numBits - 1)))
    return TRUE;

  if (a - b < 0 && (b - a) > (1 << (numBits - 1)))
    return TRUE;

  return FALSE;
}

/* 
 * check whether a is greater or equal than b on circle; both a and b are 
 * represented on numBits bits
 */
int isGreaterOrEqual(int a, int b, int numBits)
{
  if (a - b >= 0 && (a - b) < (1 << (numBits - 1)))
    return TRUE;

  if (a - b <= 0 && (b - a) > (1 << (numBits - 1)))
    return TRUE;

  return FALSE;
}

/* check whether x is between a and b on the circle */
int between(int x, int a, int b, int numBits)
{
  int flag = FALSE;

  /* check whether distance between a and b is greater than 2^{numBits - 1} */
  if (isGreaterOrEqual(b, a, numBits)) 
    /* distance between a and b greater than 2^{numBits - 1} */
    flag = TRUE;

  if ((!flag && 
       (isGreaterOrEqual(b, x, numBits) || isGreater(x, a, numBits))) ||
      (flag && isGreaterOrEqual(b, x, numBits) && 
       isGreater(x, a, numBits)))
    return TRUE;

  return FALSE;
}

int initRand(unsigned seed)
{
  int n;

  n = 128;
  initstate(seed, (char *) state1, n);
  setstate((char *)state1);
}

double funifRand(double a, double b)
{
  int c;

#ifdef UNIX
  double f = (b - a)*((double)(c = random()))/((double)MAX_INT);
#else
  double f = (b - a)*((double)(c = rand()))/((double)MAX_INT);
#endif

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

double fExp(double mean)
{
  double u;

  while ((u = funifRand(0., 1.)) == 0.0);

  return -mean * log(u);
}


void panic(char *str)
{
  printf(str);
  exit(-1);
}

