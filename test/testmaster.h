/*
 * testmaster.{C,h} --- provides functionality to instruct slaves to do RPCs.
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

#include "async.h"
#include "dhash.h"
#include "test.h"

#define TESLA_CONTROL_PORT 8002


typedef struct {
  str name;
  int port;
} testslave;


typedef struct {
  unsigned id;
  str p2psocket;
  const testslave *s;
  int unixsocket_fd;
  callback<void>::ref cb;
} conthunk;


class testmaster { public:
  testmaster();
  ~testmaster();

  // has to be done before anything else
  void setup(const testslave slaves[], callback<void>::ref);

  // un/isolate <victim> from the network
  void isolate(int victim, callback<void, int>::ref);
  void unisolate(int victim, callback<void, int>::ref);

  // tell blocker to block traffic from blockee
  void block(int blocker, int blockee, callback<void, int>::ref);
  void unblock(int blocker, int blockee, callback<void, int>::ref);

  // returns dhash client for certain identifier
  dhashclient* dhash(const unsigned id)      { return _clients[id]->dhc; }
  dhashclient* operator[](const unsigned id) { return dhash(id); }





private:
  unsigned _nhosts;
  bool _busy;

  void addnode(const unsigned id, const str p2psocket, const testslave *s,
      const int unixsocket_fd, callback<void>::ref cb);
  void addnode_cb(conthunk tx, const int there_fd);
  void accept_connection(const int unixsocket_fd, const int there_fd);
  void pipe(const int, const int);

  class instruct_thunk { public:
    instruct_thunk(callback<void, int>::ref cb) : cb(cb) {}
    ~instruct_thunk() {}
    int type;
    int blocker;
    int blockee;
    int fd;
    callback<void, int>::ref cb;
    ptr<hostent> h;
  };

  void instruct(instruct_thunk*);
  void instruct_cb(instruct_thunk*, ptr<hostent>, int);
  void instruct_cb2(instruct_thunk*, int);
  void instruct_cb3(instruct_thunk*);

  enum instruct_type {
    BLOCK = 0,
    UNBLOCK,
    ISOLATE,
    UNISOLATE
  };

  typedef union {
    struct {
      int type;
      int host;
      int port;
    } i;
    char b[12];
  } instruct_t;

  const testslave *_slaves;

  typedef struct client {
    unsigned id;
    const testslave *slave;
    str fname;
    ptr<dhashclient> dhc;
    ihash_entry<client> hash_link;

    client(unsigned i, const testslave *s, str f, ptr<dhashclient> d)
    {
      id = i;
      slave = s;
      fname = f;
      dhc = d;
    }
    ~client() {}
  } client;

  typedef ihash<unsigned, client, &client::id, &client::hash_link> clients_t;
  clients_t _clients;
};
