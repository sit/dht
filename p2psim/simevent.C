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
  this->_op = v->at(0);
}

SimEvent::~SimEvent()
{
}

void
SimEvent::execute()
{
  if(_op == "exit") {
    cout << "exiting at " << now() << ". bye." << endl;
    threadexitsall(0);
  }
}
