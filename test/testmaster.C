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

#include "async.h"
#include "test.h"
#include "testmaster.h"

testmaster::testmaster() : _nhosts(0)
{
  test_init();
}

testmaster::~testmaster()
{
  DEBUG(2) << "testmaster destructor\n";

  // unregister callbacks
}

void
testmaster::setup(const testslave slaves[], callback<void>::ref cb)
{
  // XXX: possible race condition with _nhosts
  _busy = true;
  for(unsigned i = 0; slaves[i].name != ""; i++) {
    // create a unix socket
    strbuf p2psocket;
    p2psocket << "/tmp/" << slaves[i].name << ":" << slaves[i].port;
    unlink(str(p2psocket));
    int fd = unixsocket(str(p2psocket));
    if(!fd)
      fatal << "couldn't open unix socket " << p2psocket << "\n";
    make_async(fd);

    // create a connection to the other side for this unix socket
    addnode(i, str(p2psocket), &(slaves[i]), fd, cb);
    _nhosts++;
  }
  _busy = false;
}

// pipes from fake domain socket to remote lsd and back
void
testmaster::pipe(const int from, const int to)
{
  strbuf b;

  int r = b.tosuio()->input(from);
  if(!r) {
    warn << "connection broken\n";
    // XXX: do something
    return;
  }

  b.tosuio()->output(to);
  DEBUG(2) << "piping done\n";
}


// Adds a new under id, removing old node under that id.
//
// id: id for connection
// fd: fd for local unix socket
void
testmaster::addnode(const unsigned id, const str p2psocket,
  const testslave *s, const int unixsocket_fd, callback<void>::ref cb)
{
  DEBUG(2) << "setting up " << s->name << ":" << s->port << "\n";

  // remove old entry
  client *c = _clients[id];
  if(c)
    _clients.remove(c);

  conthunk tx = { id, p2psocket, s, unixsocket_fd, cb };
  tcpconnect(s->name, s->port, wrap(this, &testmaster::addnode_cb, tx));
}

// accepts connection from dhashclient that we cfreated and sets up pipe
void
testmaster::accept_connection(const int unixsocket_fd, const int there_fd)
{
  struct sockaddr_in sin;
  unsigned sinlen = sizeof(sin);

  DEBUG(2) << "accept_connection from dhashclient\n";

  int here_fd = accept(unixsocket_fd, (struct sockaddr *) &sin, &sinlen);
  if(here_fd >= 0) {
    // when reading from dhashclient, send to testslave
    fdcb(here_fd, selread, wrap(this, &testmaster::pipe, here_fd, there_fd));
    // when reading from testslave, send to dhashclient
    fdcb(there_fd, selread, wrap(this, &testmaster::pipe, there_fd, here_fd));
  } else if (errno != EAGAIN)
    fatal << "Could not accept slave connection, errno = " << errno << "\n";
}


void
testmaster::addnode_cb(conthunk tx, const int there_fd)
{
  DEBUG(2) << "connected to " << tx.s->name << ":" << tx.s->port << "\n";

  if(there_fd == -1) {
    warn << "could not connect to " << tx.s->name << ":" << tx.s->port << "\n";
    return;
  }

  // listen for incoming connections from dhashclient that we are about to
  // create
  DEBUG(2) << "spawning server behind " << tx.p2psocket << "\n";
  if(listen(tx.unixsocket_fd, 5))
    fatal << "listen\n";
  fdcb(tx.unixsocket_fd, selread, wrap(this, &testmaster::accept_connection, tx.unixsocket_fd, there_fd));


  // we should listen for bullshit on that file descriptor and pipe it to the
  // other side
  DEBUG(2) << "creating dhashclient on " << tx.p2psocket << "\n";
  make_async(there_fd);
  ptr<dhashclient> dhc = New refcounted<dhashclient>(tx.p2psocket);
  if(!dhc)
    fatal << "couldn't create dhashclient on " << tx.p2psocket << "\n";

  // now update value in hash
  client *c = New client(tx.id, tx.s, dhc);
  _clients.insert(c);

  // call callback when master connected to all slaves
  if(!(--_nhosts) && !_busy)
    tx.cb();
}
