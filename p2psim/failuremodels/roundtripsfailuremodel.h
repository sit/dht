#ifndef __ROUNDTRIPS_FAILURE_MODEL_H
#define __ROUNDTRIPS_FAILURE_MODEL_H

#include "p2psim/failuremodel.h"
#include "p2psim/args.h"

// this class punishes failed packets by delaying them in factors of half of the
// roundtrip time.  So setting _factor to 2 adds another full roundtrip time.
class RoundtripsFailureModel : public FailureModel {
public:
  RoundtripsFailureModel(Args *a);
  virtual Time failure_latency(Packet*);

private:
  int _factor;
};

#endif // __ROUNDTRIPS_FAILURE_MODEL_H
