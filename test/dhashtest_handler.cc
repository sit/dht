/*
 * A TESLA handler for DHASH testing purposes
 *
 * Thomer M. Gil (thomer@lcs.mit.edu)
 *
 * Copyright (c) 2001-2 Massachusetts Institute of Technology.
 *
 * This software is being provided by the copyright holders under the GNU
 * General Public License, either version 2 or, at your discretion, any later
 * version. For more information, see the `COPYING' file in the source
 * distribution.
 *
 */
#include "config.h"
#include "default_handler.hh"
#include "dhashtest_handler.hh"

DEFINE_HANDLER(dhashtest, dhashtest_handler, AF_INET, SOCK_DGRAM);

HANDLER_USAGE(dhashtest_handler,
"A transparent DHASHTEST proxy handler.\n"
	      );

dhashtest_handler::dhashtest_handler(init_context& ctxt, bool plumb) :
    flow_handler(ctxt, plumb)
{
  int port = 8002;

  _blockhost.clear();
  _blockhostport.clear();

  _srvfh = ctxt.plumb(*this, AF_INET, SOCK_STREAM);
  sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_port = htons(port);
  _srvfh->bind(address(&sin, sizeof(sin)));
  _srvfh->listen(5);
  _srvfh->accept();
}


void
dhashtest_handler::accept_ready(flow_handler *from)
{
  assert(from == _srvfh);
  acceptret client = _srvfh->accept();
  if(!client)
    return;

  _clients.insert(client.h);
  client.h->set_upstream(this);
  // // upstream->accept_ready(from);
}

/*
int
dhashtest_handler::write(data d)
{
  ts_debug_1("write\n");
  return flow_handler::write(d);
}
*/

bool
dhashtest_handler::handle_instruct(data d)
{
  // handle
  ts_debug_1("client sent %d bytes", d.length());

  instruct_t ins;
  memcpy(ins.b, d.bits(), 12);        // XXX: hack

  if(ins.i.port) {
    pair<int,int> p(ins.i.host, ins.i.port);
    if(ins.i.type == BLOCK) {
      _blockhostport.insert(p);
      ts_debug_1("added %x:%d to blocked\n", ins.i.host, ins.i.port);
    } else if(ins.i.type == UNBLOCK) {
      ts_debug_1("removed %x:%d from blocked\n", ins.i.host, ins.i.port);
      _blockhostport.erase(p);
    }
    else
      return false;
  } else {
    if(ins.i.type == BLOCK) {
      _blockhost.insert(ins.i.host);
      ts_debug_1("added %x to blocked\n", ins.i.host);
      ts_debug_1("size is now %d\n", _blockhost.size());
    } else if(ins.i.type == UNBLOCK) {
      ts_debug_1("removed %x from blocked\n", ins.i.host);
      _blockhost.erase(ins.i.host);
    } else
      return false;
  }

  return true;
}

bool
dhashtest_handler::avail(flow_handler *h, data d)
{
  //
  // Traffic to us
  //
  if(_clients.find(h) != _clients.end()) {
    // close connection
    if(d.length() == 0) {
      ts_debug_1("closed connection");
      return h->close();
    }

    return handle_instruct(d);
  }


  //
  // Traffic to DHash.
  //
  int host = ((sockaddr_in*) d.addr())->sin_addr.s_addr;
  int port = ((sockaddr_in*) d.addr())->sin_port;
  pair<int,int> p(host, port);
  ts_debug_1("host that sent = %x, port = %d\n", host, port);
  ts_debug_1("_blockhost size = %d, _blockhostport size = %d\n",
      _blockhost.size(), _blockhostport.size());
  if((_blockhost.find(host) != _blockhost.end()) ||
     (_blockhostport.find(p) != _blockhostport.end()))
  {
    ts_debug_1("blocked data from %x", host);
    return true;
  }

  ts_debug_1("allowed data from %x", host);
  return upstream->avail(h, d);
}
