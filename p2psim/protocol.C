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
#include "args.h"
#include "parse.h"
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
Protocol::rcvRPC(RPCSet *hset)
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
    assert(false);
  }
  assert(i <= (int) hset->size());

  unsigned token = index2token[i];
  assert(token);
  hset->erase(token);
  cancelRPC(token);
  return token;
}


bool
Protocol::select(RPCSet *hset)
{
  Alt a[hset->size()+1];
  Packet *p;

  int i = 0;
  for(RPCSet::const_iterator j = hset->begin(); j != hset->end(); j++) {
    assert(_rpcmap[*j]);
    a[i].c = _rpcmap[*j]->channel();
    a[i].v = &p;
    a[i].op = CHANRCV;
    i++;
  }
  a[i].op = CHANNOBLK;

  int noblkindex = i;

  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    assert(false);
  }
  assert(i <= (int) hset->size());
  return i != noblkindex;
}



void
Protocol::cancelRPC(unsigned token)
{
  assert(_rpcmap.find(token) != _rpcmap.end());
  delete _rpcmap[token];
  _rpcmap.erase(token);
}


void
Protocol::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

  string line;
  map<string, Args> xmap;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read protocol string
    string protocol = words[0];
    words.erase(words.begin());

    // this is a variable assignment
    while(words.size()) {
      vector<string> xargs = split(words[0], "=");
      words.erase(words.begin());
      xmap[protocol].insert(make_pair(xargs[0], xargs[1]));
    }
  }

  for(map<string, Args>::const_iterator i = xmap.begin(); i != xmap.end(); ++i)
    ProtocolFactory::Instance()->setprotargs(i->first, i->second);

  in.close();
}
