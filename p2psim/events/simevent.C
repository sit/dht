#include "p2psim/threadmanager.h"
#include "simevent.h"
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
    DEBUG(1) << "simulation exits at the end of cycle " << now() << "." << endl;
    ThreadManager::Instance()->create(&::graceful_exit, (void *)0);
    //i like ugly exit
    //exit(1);
  } else
    cerr << "SimEvent::execute(): unknown op " << _op << "\n";
}
