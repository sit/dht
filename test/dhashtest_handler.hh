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
 * $Id: dhashtest_handler.hh,v 1.2 2002/11/22 07:46:41 thomer Exp $
 *
 */

#ifndef HH_DHASHTEST_HANDLER
#define HH_DHASHTEST_HANDLER

#include "tesla/buf_handler.hh"

#include <sys/socket.h>
#include <utility>
#include <set>

#define TESLA_CONTROL_PORT "8002" // string, for stupid reasons

class dhashtest_handler : public flow_handler { public:
  dhashtest_handler(init_context& ctxt, bool plumb = true);
  ~dhashtest_handler() {}
  bool avail(flow_handler*, data);
  int write(data d);
  void accept_ready(flow_handler *);

private:
  static set<int> _rblock;
  static set<int> _wblock;
  static bool _risolated;
  static bool _wisolated;
  static bool _initialized;

  set<flow_handler*> _clients;
  flow_handler *_srvfh;
  bool handle_instruct(data);

#define READ       0x001
#define WRITE      0x002
#define BLOCK      0x010
#define UNBLOCK    0x020
#define ISOLATE    0x100
#define UNISOLATE  0x200

  typedef union {
    struct {
      unsigned cmd;
      int host;
      int port; // ignored for now
    } i;
    char b[12];
  } instruct_t;

  DECLARE_HANDLER;
};

set<int> dhashtest_handler::_rblock;
set<int> dhashtest_handler::_wblock;
bool dhashtest_handler::_risolated = false;
bool dhashtest_handler::_wisolated = false;
bool dhashtest_handler::_initialized = false;


#endif
