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
  string port;
  get_arg("port", port, TESLA_CONTROL_PORT);

  _rblock.clear();
  _wblock.clear();

  _srvfh = ctxt.plumb(*this, AF_INET, SOCK_STREAM);

  if(!_initialized) {
    sockaddr_in sin;
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = INADDR_ANY;
    sin.sin_port = htons(atoi(port.c_str()));
    if(_srvfh->bind(address(&sin, sizeof(sin))) < 0)
      ts_fatal("bind failed, errno = %s", strerror(errno));

    ts_debug_1("dhashtest_handler control port = %d", atoi(port.c_str()));
    if(_srvfh->listen(5) == 1)
      ts_fatal("listen failed");
    if(_srvfh->accept() == 1)
      ts_fatal("accept failed");

    _initialized = true;
  }
}


void
dhashtest_handler::accept_ready(flow_handler *from)
{
  assert(from == _srvfh);
  acceptret client = _srvfh->accept();
  if(!client) {
    ts_debug_1("client connection failed");
    return;
  }

  _clients.insert(client.h);
  client.h->set_upstream(this);
}



int
dhashtest_handler::write(data d)
{
  if(_wisolated ||
      (_wblock.find( ((struct sockaddr_in*)d.addr())->sin_addr.s_addr) != _wblock.end())) {
    ts_debug_1("write isolated: write was blocked.");
    return 0;
  }

  return flow_handler::write(d);
}



bool
dhashtest_handler::handle_instruct(data d)
{
  // handle
  instruct_t ins;
  memcpy(ins.b, d.bits(), 12);        // XXX: hack

  // ISOLATE
  if(ins.i.cmd & ISOLATE) {
    if(ins.i.cmd & READ) {
      ts_debug_1("read isolated!");
      _risolated = true;
    } else if (ins.i.cmd & WRITE) {
      ts_debug_1("write isolated!");
      _wisolated = true;
    } else {
      ts_debug_1("warning: ISOLATE without READ/WRITE. assuming both.");
      _risolated = _wisolated = true;
    }
    return true;
  }
  
  // UNISOLATE
  if(ins.i.cmd & UNISOLATE) {
    if(ins.i.cmd & READ) {
      ts_debug_1("read unisolated!");
      _risolated = false;
    } else if (ins.i.cmd & WRITE) {
      ts_debug_1("write unisolated!");
      _wisolated = false;
    } else {
      ts_debug_1("warning: UNISOLATE without READ/WRITE. assuming both");
      _risolated = _wisolated = false;
    }
    return true;
  }


  // READ or WRITE?
  set<int> *blockset = (set<int>*) 0;
  if(ins.i.cmd & READ)
    blockset = &_rblock;
  else if(ins.i.cmd & WRITE)
    blockset = &_wblock;
  else {
    ts_debug_1("BLOCK/UNBLOCK without READ/WRITE. doing nothing");
    return false;
  }

  if(ins.i.cmd & BLOCK) {
    blockset->insert(ins.i.host);
  } else if(ins.i.cmd & UNBLOCK) {
    blockset->erase(ins.i.host);
  } else {
    ts_debug_1("no command!");
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
    if(d.length() == 0)
      return h->close();
    return handle_instruct(d);
  }


  //
  // Traffic to DHash.
  //
  int host = ((sockaddr_in*) d.addr())->sin_addr.s_addr;
  int port = ((sockaddr_in*) d.addr())->sin_port;
  ts_debug_2("host that sent = %x, port = %d", host, port);
  if(_risolated || (_rblock.find(host) != _rblock.end())) {
    ts_debug_1("blocked data from %x", host);
    return true;
  }

  ts_debug_2("allowed data from %x", host);
  return upstream->avail(h, d);
}
