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

Protocol::Protocol(Node *n) : _node(n)
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

RPCHandle*
Protocol::select(set<RPCHandle*> *hset)
{
  Alt a[hset->size()+1];
  Packet *p;
  map<unsigned, RPCHandle*> xmap;

  int i = 0;
  for(set<RPCHandle*>::const_iterator j = hset->begin(); j != hset->end(); j++) {
    a[i].c = (*j)->channel();
    a[i].v = &p;
    a[i].op = CHANRCV;
    xmap[i] = *j;
    i++;
  }
  a[i].op = CHANEND;

  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    return 0;
  }
  assert(i <= (int) hset->size());

  assert(xmap[i]);
  hset->erase(xmap[i]);
  return xmap[i];
}
