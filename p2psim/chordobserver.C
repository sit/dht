#include "chordobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>
using namespace std;

ChordObserver* ChordObserver::_instance = 0;

ChordObserver*
ChordObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = new ChordObserver(a);
  return _instance;
}


ChordObserver::ChordObserver(Args *a)
{
  cout << "Instantiated with jinyang = " << (*a)["jinyang"] << endl;
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::execute()
{
  cout << "ChordObserver executing" << endl;
  list<Protocol*> l = Network::Instance()->getallprotocols("Chord");
}
