#include "observed.h"

Observed::Observed() {
  _observers.clear();
};

void
Observed::registerObserver(Observer *o)
{
  _observers.insert(o);
}

void
Observed::unregisterObserver(Observer *o)
{
  _observers.erase(o);
}

void
Observed::notifyObservers(ObserverInfo *oi)
{
  for(set<Observer*>::const_iterator i = _observers.begin(); i != _observers.end(); ++i)
    (*i)->kick(this, oi);
}
