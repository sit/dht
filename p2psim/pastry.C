#include "p2psim.h"
#include "pastry.h"
#include "packet.h"
#include <iostream>
using namespace std;

Pastry::Pastry(Node *n) : Protocol(n), idlength(128)
{
  _id = BN_new();
  BN_init(_id);
  BN_pseudo_rand(_id, idlength, -1, 0);
  cout << BN_bn2hex(_id) << endl;
}

Pastry::~Pastry()
{
  BN_free(_id);
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
Pastry::insert_doc(Args*)
{
  cout << "Pastry insert_doc" << endl;
}

void
Pastry::lookup_doc(Args*)
{
  cout << "Pastry lookup_doc" << endl;
}
