#include "incl.h"

/* compute (id + i) mod 2^NUM_BITS */
int successorId(int id, int i)
{
  id = id + (1 << i);

  if (id >= (1 << NUM_BITS))
      id -= (1 << NUM_BITS);

  return id;
}

/* compute (id - i) mod 2^NUM_BITS */
int predecessorId(int id, int i)
{
  id = id - (1 << i);

  if (id < 0)
      id += (1 << NUM_BITS);

  return id;
}

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
