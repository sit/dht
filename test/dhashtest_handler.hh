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
 * $Id: dhashtest_handler.hh,v 1.4 2002/11/26 07:32:08 thomer Exp $
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
  static map<unsigned, unsigned> _rdrop;
  static map<unsigned, unsigned> _wdrop;
  static unsigned _rdropall;
  static unsigned _wdropall;
  static bool _risolated;
  static bool _wisolated;
  static bool _initialized;

  set<flow_handler*> _clients;
  flow_handler *_srvfh;
  bool handle_instruct(data);

  enum rw_t {
    READ = 0,
    WRITE,
    READ_WRITE
  };

  enum cmd_t {
    ISOLATE = 0x1,
    UNISOLATE = 0x2,
    DROP = 0x4
  };

#define CMD_ARG(x)        ((x).oargs.cmd)
#define ISOLATE_ARG(x,y)  ((x).oargs.iargs.isolate.y)
#define DROP_ARG(x,y)     ((x).oargs.iargs.drop.y)

  typedef union {
    struct {
      unsigned cmd;
      union {
        struct {
          unsigned flags;
        } isolate;

        struct {
          unsigned flags;
          unsigned host;
          unsigned perc;
        } drop;
      } iargs;
    } oargs;
    char b[16];
  } instruct_t;



  DECLARE_HANDLER;
};

map<unsigned, unsigned> dhashtest_handler::_rdrop;
map<unsigned, unsigned> dhashtest_handler::_wdrop;
unsigned dhashtest_handler::_rdropall = 0;
unsigned dhashtest_handler::_wdropall = 0;
bool dhashtest_handler::_risolated = false;
bool dhashtest_handler::_wisolated = false;
bool dhashtest_handler::_initialized = false;


#endif
