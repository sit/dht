#include "testmaster.h"

testslave slaves[] = {
  { "127.0.0.1", 8002 }, // id: 0
  { "amsterdam.lcs.mit.edu", 8002 } // id: 1
};


void 
done(int *n, int ok)
{
  if(!ok)
    fatal << "error\n";

  if(!--*n)
    exit(0);
}

int
main(int argc, char *argv[])
{
  testmaster tm(slaves);

  int n = 2;
  tm.block(0, 1, wrap(&done, &n));
  tm.unblock(0, 1, wrap(&done, &n));

  amain();
}
