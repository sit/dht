/*
 * testmaster.{C,h} --- a class that provides functionality to instruct
 *    slaves to do nasty DHash things.
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

#include "dhash.h"
#include "ihash.h"


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


class testmaster : public virtual refcount { public:
  testmaster();
  ~testmaster();

  // sets up all local unix domain sockets and remote slaves
  void setup(const testslave[], callback<void>::ref);

  // returns dhash client for certain identifier
  dhashclient* dhash(const unsigned id) {
    return _clients[id]->dhc;
  }

  // returns dhash client for certain identifier
  dhashclient* operator[](const unsigned id) {
    return dhash(id);
  }

private:
  // pipes stuff from unix domain socket to testslave and vice versa
  void pipe(const int, const int);

  // adds a new node
  void addnode(const unsigned, const str, const testslave *,
      const int, callback<void>::ref);

  // callback for addnode
  void addnode_cb(conthunk, const int there_fd);

  // accepts connection from dhashclient
  void accept_connection(const int, const int);


  typedef struct client {
    unsigned id;
    const testslave *slave;
    ptr<dhashclient> dhc;
    ihash_entry<client> hash_link;

    client(unsigned i, const testslave *s, ptr<dhashclient> d)
    {
      id = i;
      slave = s;
      dhc = d;
    }
    ~client() {}
  } client;

  typedef ihash<unsigned, client, &client::id, &client::hash_link> clients_t;
  clients_t _clients;

  unsigned _nhosts;
  bool _busy;

  void refcount_call_finalize() {}
};
