#ifndef __FAILURE_MODEL_H
#define __FAILURE_MODEL_H

#include "p2psim.h"
#include "packet.h"

class FailureModel {
public:
  virtual Time failure_latency(Packet*) = 0;
};

#endif // __FAILURE_MODEL_H
