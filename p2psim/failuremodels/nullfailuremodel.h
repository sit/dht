#ifndef __NULL_FAILURE_MODEL_H
#define __NULL_FAILURE_MODEL_H

#include "p2psim/packet.h"

class NullFailureModel : public FailureModel {
public:
  NullFailureModel() { }
  virtual Time failure_latency(Packet*) { return 0; }
};

#endif // __NULL_FAILURE_MODEL_H
