#include "p2psim.h"
#include "pastry.h"
#include "packet.h"
#include <iostream>
using namespace std;

Pastry::Pastry(Node *n) : Protocol(n),
  // suggested values in paper
  idlength(128),
  _b(4),
  _L(2 << _b),
  _M(2 << _b)
{
  // create a random 128-bit ID
  _id = BN_new();
  BN_init(_id);
  BN_pseudo_rand(_id, idlength, -1, 0);

  // routing table as an array of RTRow's, indexed by length of common prefix.
  _rtable = new vector<RTRow>[idlength];
}


Pastry::~Pastry()
{
  BN_free(_id);
  delete[] _rtable;
}

void
Pastry::join(Args *a)
{
  cout << "Pastry join" << endl;
}

void
Pastry::leave(Args*)
{
  cout << "Pastry leave" << endl;
}

void
Pastry::crash(Args*)
{
  cout << "Pastry crash" << endl;
}

void
Pastry::insert(Args*)
{
  cout << "Pastry insert" << endl;
}

void
Pastry::lookup(Args *args)
{
  // if in leaf set
  cout << "Pastry lookup" << endl;
}
