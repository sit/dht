#include "churneventgenerator.h"
#include "p2psim/eventqueue.h"
#include "events/p2pevent.h"

ChurnEventGenerator::ChurnEventGenerator(Args *args) : _size(0)
{
  cout << "ChurnEventGenerator, size = " << (*args)["size"] << endl;
  _size = args->nget("size", 512, 10);
  _proto = (*args)["proto"];
  EventQueue::Instance()->registerObserver(this);
}

void
ChurnEventGenerator::run()
{
  EventQueue::Instance()->go();
}


void
ChurnEventGenerator::kick(Observed *o, ObserverInfo *oi)
{
  cout << "ChurnEventGenerator" << endl;
  P2PEvent *e = New P2PEvent(now() + 1, _proto, (IPAddress) _size+1, "join");
  add_event(e);
}
