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
 * $Id: dhashtest_handler.hh,v 1.1 2002/11/08 05:15:51 thomer Exp $
 *
 */

#ifndef HH_DHASHTEST_HANDLER
#define HH_DHASHTEST_HANDLER

#include "tesla/buf_handler.hh"

#include <sys/socket.h>
#include <utility>
#include <set>

class dhashtest_handler : public flow_handler { public:
  dhashtest_handler(init_context& ctxt, bool plumb = true);
  ~dhashtest_handler() {}
  bool avail(flow_handler*, data);
  void accept_ready(flow_handler *);

private:
  static set<int> _blockhost;
  static set<pair<int, int> > _blockhostport;
  set<flow_handler*> _clients;
  flow_handler *_srvfh;
  bool handle_instruct(data);

  enum instruct_type {
    BLOCK = 0,
    UNBLOCK = 1
  };

  typedef union {
    struct {
      int type;
      int host;
      int port;
    } i;
    char b[12];
  } instruct_t;

  DECLARE_HANDLER;
};

set<int> dhashtest_handler::_blockhost;
set<pair<int, int> > dhashtest_handler::_blockhostport;


#endif
