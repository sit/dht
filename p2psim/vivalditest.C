#include "vivalditest.h"
#include "packet.h"
#include "p2psim.h"
#include <stdio.h>
using namespace std;

VivaldiTest::VivaldiTest(Node *n) : Protocol(n)
{
  _vivaldi = new Vivaldi(n);

  delaycb(1000, &VivaldiTest::tick, (void *) 0);
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
  IPAddress dst = (random() % 100) + 1;
  Vivaldi::Coord c;
  Time before = now();
  doRPC(dst, &VivaldiTest::handler, (void*) 0, &c);
  _vivaldi->sample(dst, c, now() - before);

  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::handler(void *args, Vivaldi::Coord *ret)
{
  *ret = _vivaldi->my_location();
}
