#include "eventqueue.h"
#include "simevent.h"
#include "p2psim.h"
#include <lib9.h>
#include <thread.h>
#include <iostream>
using namespace std;

SimEvent::SimEvent()
{
}

SimEvent::SimEvent(vector<string> *v) : Event(v)
{
  this->_op = (*v)[0];
}

SimEvent::~SimEvent()
{
}

void
SimEvent::execute()
{
  if(_op == "exit") {
    cout << "simulation exits" << endl;
    ::graceful_exit();
  } else
    cerr << "SimEvent::execute(): unknown op " << _op << "\n";
}
