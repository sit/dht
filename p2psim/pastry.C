#include "p2psim.h"
#include "pastry.h"
#include "packet.h"
#include <iostream>
using namespace std;

Pastry::Pastry(Node *n) : Protocol(n)
{
}

Pastry::~Pastry()
{
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
