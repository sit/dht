#include "vivalditest.h"
#include "packet.h"
#include <iostream>
#include "p2psim.h"
using namespace std;

VivaldiTest::VivaldiTest(Node *n) : Protocol(n)
{
}

VivaldiTest::~VivaldiTest()
{
}

void
VivaldiTest::join(Args *a)
{
  cout << "VivaldiTest join" << endl;
}

void
VivaldiTest::leave(Args*)
{
  cout << "VivaldiTest leave" << endl;
}

void
VivaldiTest::crash(Args*)
{
  cout << "VivaldiTest crash" << endl;
}

void
VivaldiTest::insert(Args*)
{
  cout << "VivaldiTest insert" << endl;
}

void
VivaldiTest::lookup(Args *args)
{
  cout << "VivaldiTest lookup" << endl;
}
