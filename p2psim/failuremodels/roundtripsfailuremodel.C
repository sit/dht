#include "roundtripsfailuremodel.h"
#include "p2psim/network.h"

RoundtripsFailureModel::RoundtripsFailureModel(Args *a) : _factor(1)
{
  _factor = a->nget<unsigned>("factor", 1, 10);
}

Time
RoundtripsFailureModel::failure_latency(Packet *p)
{
  return _factor * Network::Instance()->gettopology()->latency(p->src(), p->dst());
}
