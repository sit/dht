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


void
Protocol::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    threadexitsall(0);
  }

  string line;
  string protocol = "";
  map<string, Args> xmap;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read protocol string
    if(words[0] == "protocol") {
      words.erase(words.begin());
      protocol = words[0];
      continue;
    }

    // this is a variable assignment
    vector<string> xargs = split(words[0], "=");

    if(protocol == "") {
      cerr << "protocol line missing in " << filename << endl;
      threadexitsall(0);
    }

    xmap[protocol].insert(make_pair(xargs[0], xargs[1]));
  }

  for(map<string, Args>::const_iterator i = xmap.begin(); i != xmap.end(); ++i)
    ProtocolFactory::Instance()->setprotargs(i->first, i->second);

  in.close();
}
