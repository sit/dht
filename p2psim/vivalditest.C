#include "vivalditest.h"
#include "packet.h"
#include "p2psim.h"
#include <stdio.h>
using namespace std;

VivaldiTest::VivaldiTest(Node *n) : Protocol(n)
{
  _vivaldi = new Vivaldi(n);

  delaycb(1000, VivaldiTest::tick, NULL);
}

VivaldiTest::~VivaldiTest()
{
}

char *
VivaldiTest::ts()
{
  static char buf[50];
  sprintf(buf, "%lu Vivaldi(%u)", now(), node()->ip());
  return buf;
}

void
VivaldiTest::tick(void *)
{
  printf("%s tick\n", ts());
  delaycb(1000, VivaldiTest::tick, NULL);
}
