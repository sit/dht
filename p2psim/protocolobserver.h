#ifndef __PROTOCOL_OBSERVER_H
#define __PROTOCOL_OBSERVER_H

#include "observer.h"

class ProtocolObserver : public Observer {
public:
  virtual bool stabilized() = 0;
};

#endif // __PROTOCOL_OBSERVER_H
