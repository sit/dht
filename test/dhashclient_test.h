/*
 * dhashclient_test.{C,h} --- superset of normal dhashclient, but can talk to
 * underlying tesla layer
 *
 * Copyright (C) 2002  Thomer M. Gil (thomer@lcs.mit.edu)
 *   		       Massachusetts Institute of Technology
 * 
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __DHASHCLIENT_TEST
#define __DHASHCLIENT_TEST 1


#include "async.h"
#include "dhash.h"
#include "test.h"


// superset of dhashclient
class dhashclient_test : public dhashclient { 
public:
  dhashclient_test::dhashclient_test(str, str, unsigned, unsigned, unsigned);
  ~dhashclient_test()  {}

  enum rw_t {
    READ = 0x01,
    WRITE = 0x02,
    RW = 0x04
  };


  // to isolate nodes from the network
  void isolate(rw_t flags, callback<void, int>::ref cb); // read and/or write
  void unisolate(rw_t flags, callback<void, int>::ref cb);

  // block traffic from a certain host.  (implemented through 100% drop)
  void block(rw_t flags, dhashclient_test *dht, callback<void, int>::ref cb);
  void unblock(rw_t flags, dhashclient_test *dht, callback<void, int>::ref cb);

  // drop fraction of packets from/to all (when dht == 0) or from/to specific
  // host
  void drop(rw_t flags, dhashclient_test *dht, int percentage, callback<void, int>::ref cb);

  // used by other dhashclient_test's
  unsigned ip()              { return _myip; }
  unsigned dhashport()       { return _test_dhashport; }
  unsigned controlport()     { return _test_controlport; }

private:
  str _test_name;
  unsigned _test_dhashport;
  unsigned _test_controlport;
  unsigned _myip;

  // internal use only
  enum cmd_t {
    ISOLATE = 0x1,
    UNISOLATE = 0x2,
    DROP = 0x4
  };

#define CMD_ARG(x)        ((x)->instruct.oargs.cmd)
#define ISOLATE_ARG(x,y)  ((x)->instruct.oargs.iargs.isolate.y)
#define DROP_ARG(x,y)     ((x)->instruct.oargs.iargs.drop.y)

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
          unsigned port;
          unsigned perc;
        } drop;
      } iargs;
    } oargs;
    char b[20];
  } instruct_t;

  class instruct_thunk { public:
    instruct_thunk(callback<void, int>::ref cbx) : cb(cbx) { lockcounter = 0; }
    ~instruct_thunk() {
      if(lockcounter) {
        delete lockcounter;
        lockcounter = 0;
      }
    }

    instruct_t instruct;
    dhashclient_test *blockee;
    int fd;
    callback<void, int>::ref cb;
    ptr<hostent> h;
    int *lockcounter; // cb is called only when cb drops to 0
  };

  instruct_thunk *instruct_init(unsigned cmd, callback<void, int>::ref cb);
  void instruct(instruct_thunk*);
  void instruct_cb1(instruct_thunk*, int);
  void instruct_cb2(instruct_thunk*);
  void empty_cb(int);
};

#endif // __DHASHCLIENT_TEST
