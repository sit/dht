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
  string id;
  get_arg("port", port, TESLA_CONTROL_PORT);
  get_arg("id", id, "");
  srand(time(0) ^ (getpid() + (getpid() << 15)));

  if("id" == "")
    ts_debug_1("warning: id not set");

  _rdrop.clear();
  _wdrop.clear();

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
  // destination host
  unsigned host = ((struct sockaddr_in*)d.addr())->sin_addr.s_addr;

  // isolated
  if(_wisolated) {
    ts_debug_1("write isolated: write was blocked.");
    return 0;
  }

  // drop
  unsigned dropchance = 0;
  map<unsigned, unsigned>::iterator pos;

  if(_wdropall)
    dropchance = _wdropall;
  else if((pos = _wdrop.find(host)) != _wdrop.end())
    dropchance = pos->second;

  // don't bother to throw a coin if it's 100
  unsigned coin;
  if((coin = dropchance) == 100 ||
     (coin = (1+(int) (100.0*rand()/(RAND_MAX+1.0)))) <= dropchance)
  {
    ts_debug_1("dropped because of %d", coin);
    return 0;
  }

  return flow_handler::write(d);
}



bool
dhashtest_handler::handle_instruct(data d)
{
  // handle
  instruct_t ins;
  memcpy(ins.b, d.bits(), 16);        // XXX: hack

  // ISOLATE
  if(CMD_ARG(ins) & ISOLATE) {
    if(ISOLATE_ARG(ins, flags) & READ) {
      ts_debug_1("read isolated!");
      _risolated = true;
    } else if (ISOLATE_ARG(ins, flags) & WRITE) {
      ts_debug_1("write isolated!");
      _wisolated = true;
    } else {
      ts_debug_1("warning: ISOLATE without READ/WRITE. assuming both.");
      _risolated = _wisolated = true;
    }
    return true;
  }
  
  // UNISOLATE
  if(CMD_ARG(ins) & UNISOLATE) {
    if(ISOLATE_ARG(ins, flags) & READ) {
      ts_debug_1("read unisolated!");
      _risolated = false;
    } else if (ISOLATE_ARG(ins, flags) & WRITE) {
      ts_debug_1("write unisolated!");
      _wisolated = false;
    } else {
      ts_debug_1("warning: UNISOLATE without READ/WRITE. assuming both");
      _risolated = _wisolated = false;
    }
    return true;
  }

  // DROP
  if(CMD_ARG(ins) & DROP) {
    // from specific host
    if(DROP_ARG(ins, host)) {
      map<unsigned, unsigned> *dropmap = (map<unsigned, unsigned>*) 0;
      if(DROP_ARG(ins, flags) & READ)
        dropmap = &_rdrop;
      else if(DROP_ARG(ins, flags) & WRITE)
        dropmap = &_wdrop;
      else {
        ts_debug_1("warning: DROP without READ/WRITE. assuming both");
        return false;
      }

      ts_debug_1("setting drops to %d\n", DROP_ARG(ins, perc));
      if(!DROP_ARG(ins, perc)) {
        dropmap->erase(DROP_ARG(ins, host));
      } else {
        ts_debug_1("inserting host %d", DROP_ARG(ins, host));
        (*dropmap)[DROP_ARG(ins, host)] = DROP_ARG(ins, perc);
      }
      return true;

    // from all
    } else {
      ts_debug_1("dropping %d from all", DROP_ARG(ins, perc));
      if(DROP_ARG(ins, flags) & READ)
        _rdropall = DROP_ARG(ins, perc);
      if(DROP_ARG(ins, flags) & WRITE)
        _wdropall = DROP_ARG(ins, perc);
      return true;
    }
  }

  ts_debug_1("this should have not happened. ignoring command");
  return false;
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

  // block
  if(_risolated) {
    ts_debug_1("blocked data from %x", host);
    return true;
  }

  // drop
  unsigned dropchance = 0;
  map<unsigned, unsigned>::iterator pos;

  if(_rdropall)
    dropchance = _rdropall;
  else if((pos = _rdrop.find(host)) != _rdrop.end())
    dropchance = pos->second;

  if(dropchance) {
    unsigned coinflip = 1+(int) (100.0*rand()/(RAND_MAX+1.0));
    ts_debug_1("coinflip %d vs %d", coinflip, dropchance);
    if(coinflip < dropchance) {
      ts_debug_1("dropped");
      return true;
    }
  }

  ts_debug_2("allowed data from %x", host);
  return upstream->avail(h, d);
}
