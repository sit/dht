#include <iostream>
using namespace std;

#include <assert.h>

#include "protocolfactory.h"
#include "node.h"
#include "packet.h"

Node::Node(IPAddress ip) : _ip(ip), _pktchan(0)
{
  _pktchan = chancreate(sizeof(Packet*), 0);
  assert(_pktchan);
  _protchan = chancreate(sizeof(string), 0);
  assert(_protchan);
  thread();
}

Node::~Node()
{
  chanfree(_pktchan);
  chanfree(_protchan);
}


void
Node::run()
{
  Alt a[3];
  Packet *p;
  string protname;
  Protocol *prot;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _protchan;
  a[1].v = &protname;
  a[1].op = CHANRCV;

  a[2].op = CHANEND;
  
  while(1) {
    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      case 0:
        // if this is a reply, send it back on the channel where the thread is
        // waiting a reply.  otherwise send it to the protocol's channel
        if(p->reply())
          send(p->channel(), &p);
        else {
          prot = _protmap[p->protocol()];
          if(!prot) {
            cerr << "WARNING: protocol " << p->protocol() << " is not running on " << ip() << endl;
            break;
          }
          send(prot->netchan(), &p);
        }
        break;

      // add protocol to node
      case 1:
        prot = ProtocolFactory::Instance()->create(protname, this);
        assert(prot);
        if(_protmap[protname]) {
          cerr << "warning: " << protname << " already running on node " << ip() << endl;
          delete _protmap[protname];
        }
        _protmap[protname] = prot;
        break;

      default:
        break;
    }
  }
}
