#include <assert.h>
#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <map>
#include <stdio.h>
using namespace std;

#include "protocol.h"
#include "protocolfactory.h"
#include "threadmanager.h"
#include "packet.h"
#include "network.h"
#include "p2pevent.h"
#include "node.h"
#include "eventqueue.h"
#include "rpchandle.h"
#include "p2psim.h"

Protocol::Protocol(Node *n) : _node(n), _token(1)
{
}

Protocol::~Protocol()
{
}

Protocol *
Protocol::getpeer(IPAddress a)
{
  return (Network::Instance()->getnode(a)->getproto(proto_name()));
}

unsigned
Protocol::select(RPCSet *hset)
{
  Alt a[hset->size()+1];
  Packet *p;
  map<unsigned, unsigned> index2token;

  int i = 0;
  for(RPCSet::const_iterator j = hset->begin(); j != hset->end(); j++) {
    assert(_rpcmap[*j]);
    a[i].c = _rpcmap[*j]->channel();
    a[i].v = &p;
    a[i].op = CHANRCV;
    index2token[i] = *j;
    i++;
  }
  a[i].op = CHANEND;

  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    return 0;
  }
  assert(i <= (int) hset->size());

  unsigned token = index2token[i];
  assert(token);
  hset->erase(token);
  cancelRPC(token);
  return token;
}


void
Protocol::cancelRPC(unsigned token)
{
  assert(_rpcmap.find(token) != _rpcmap.end());
  delete _rpcmap[token];
  _rpcmap.erase(token);
}
