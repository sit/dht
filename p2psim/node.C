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
  /*
  for(PMCI p = _protmap.begin(); p != _protmap.end(); ++p) {
    cout << "deleting protocol" << endl;
    delete p->second;
  }
  */
  _protmap.clear();
}


void
Node::run()
{
  Alt a[4];
  Packet *p;
  string protname;
  Protocol *prot;
  unsigned exit;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _protchan;
  a[1].v = &protname;
  a[1].op = CHANRCV;

  a[2].c = _exitchan;
  a[2].v = &exit;
  a[2].op = CHANRCV;

  a[3].op = CHANEND;
  
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

      //exit
      case 2:
        // send all protocols an exit
        // cout << "exit node channel" << endl;
        for(PMCI p = _protmap.begin(); p != _protmap.end(); ++p)
          send(p->second->exitchan(), 0);
        _protmap.clear();
        delete this;


      default:
        break;
    }
  }
}
