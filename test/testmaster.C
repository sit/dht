/*
 * testmaster.{C,h}
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
#include "dns.h"
#include "rxx.h"
#include "testmaster.h"

testmaster::testmaster(const testslave slaves[]) : _nhosts(0), _slaves(slaves)
{
  test_init();
}

testmaster::testmaster(char *filename) : _nhosts(0), _slaves(0)
{
  test_init();
  FILE *f = fopen(filename, "r");
  if(f <= 0)
    fatal << "couldn't open " << filename << ": " << strerror(errno) << "\n";

  int n = 0;
  testslave *slaves = 0;
  while(!feof(f)) {
    // XXX: very stupid
    testslave *newslaves = New testslave[n+1];
    memcpy(newslaves, slaves, n*sizeof(testslave));
    free(slaves);
    slaves = newslaves;

    char l[1024];
    if(!fgets(l, sizeof(l), f))
      break;

    str line = str(l);

    // comment
    rxx commentrxx("^\\s*#");
    if(commentrxx.search(line))
      continue;

    rxx linerxx("^\\s*(\\S+)\\s+(\\d+)\\s+(\\d+)\\s*$");
    if(!linerxx.search(line))
      break;

    slaves[n].name = linerxx[1];
    slaves[n].dhash_port = atoi(linerxx[2]) ? atoi(linerxx[2]) : DEFAULT_PORT;
    slaves[n].control_port = atoi(linerxx[3]) ? atoi(linerxx[3]) : TESLA_CONTROL_PORT;
    n++;
  }
  fclose(f);

  // append an empty entry
  slaves[n].name = str("");

  _slaves = slaves;
}



testmaster::~testmaster()
{
}


void
testmaster::setup(callback<void>::ref cb)
{
  assert(_slaves);
  _busy = true;
  for(unsigned i = 0; _slaves[i].name != ""; i++) {
    // create a unix socket
    str p2psocket = strbuf() << "/tmp/" << _slaves[i].name << ":" << _slaves[i].dhash_port;
    unlink(p2psocket);
    int fd = unixsocket(p2psocket);
    if(!fd)
      fatal << "couldn't open unix socket " << p2psocket << "\n";
    make_async(fd);

    // create a connection to the other side for this unix socket
    addnode(i, p2psocket, &(_slaves[i]), fd, cb);
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
    // XXX: close connection
    return;
  }

  b.tosuio()->output(to);
}

void
testmaster::addnode(const unsigned id, const str p2psocket,
  const testslave *s, const int unixsocket_fd, callback<void>::ref cb)
{
  DEBUG(2) << "setting up " << s->name << ":" << s->dhash_port << "\n";

  // remove old entry
  client *c = _clients[id];
  if(c)
    _clients.remove(c);

  conthunk tx = { id, p2psocket, s, unixsocket_fd, cb };
  tcpconnect(s->name, s->dhash_port, wrap(this, &testmaster::addnode_cb, tx));
}




// accepts connection from dhashclient that we just created and sets up pipe
void
testmaster::accept_connection(const int unixsocket_fd, const int there_fd)
{
  struct sockaddr_in sin;
  unsigned sinlen = sizeof(sin);

  DEBUG(2) << "accept_connection from dhashclient\n";

  int here_fd = accept(unixsocket_fd, (struct sockaddr *) &sin, &sinlen);
  if(here_fd >= 0) {
    // setup pipe between dhashclient and remote slave
    fdcb(here_fd, selread, wrap(this, &testmaster::pipe, here_fd, there_fd));
    fdcb(there_fd, selread, wrap(this, &testmaster::pipe, there_fd, here_fd));
  } else if (errno != EAGAIN)
    fatal << "Could not accept slave connection, errno = " << errno << "\n";
}



void
testmaster::addnode_cb(conthunk tx, const int there_fd)
{
  DEBUG(2) << "connected to " << tx.s->name << ":" << tx.s->dhash_port << "\n";

  if(there_fd == -1) {
    warn << "could not connect to " << tx.s->name << ":" << tx.s->dhash_port << "\n";
    return;
  }

  // listen for incoming connection from dhashclient that we are about to
  // create
  DEBUG(2) << "spawning server behind " << tx.p2psocket << "\n";
  if(listen(tx.unixsocket_fd, 5))
    fatal << "listen\n";
  fdcb(tx.unixsocket_fd, selread, wrap(this, &testmaster::accept_connection, tx.unixsocket_fd, there_fd));


  // create DHash object
  DEBUG(2) << "creating dhashclient on " << tx.p2psocket << "\n";
  make_async(there_fd);
  ptr<dhashclient> dhc = New refcounted<dhashclient>(tx.p2psocket);
  if(!dhc)
    fatal << "couldn't create dhashclient on " << tx.p2psocket << "\n";

  // update hash table
  client *c = New client(tx.id, tx.s, tx.p2psocket, dhc);
  _clients.insert(c);

  // call callback once all slaves are connected
  if(!(--_nhosts) && !_busy)
    tx.cb();
}


// cmd is a bunch of OR-ed flags.  see below.
void
testmaster::instruct(int blocker, int cmd, callback<void, int>::ref cb, int blockee)
{
  instruct_thunk *tx = New instruct_thunk(cb);
  if(!tx)
    fatal << "malloc!\n";
  
  tx->cmd = cmd;
  tx->blocker = blocker;
  tx->blockee = blockee == -1 ? blocker : blockee;

  warn << _slaves[tx->blockee].name << "\n";
  dns_hostbyname(_slaves[tx->blockee].name,
      wrap(this, &testmaster::instruct_cb, tx));
}

void
testmaster::instruct_cb(instruct_thunk *tx, ptr<hostent> h, int err)
{

  if(!h) {
    warn << "dns_hostbyname failed\n";
    tx->cb(0);
    return;
  }

  tx->h = h;
  DEBUG(1) << "tcppconnect to " << _slaves[tx->blocker].name << ":" << TESLA_CONTROL_PORT << "\n";

  int port = _slaves[tx->blocker].control_port ?
      _slaves[tx->blocker].control_port : TESLA_CONTROL_PORT;
  tcpconnect(_slaves[tx->blocker].name, port,
      wrap(this, &testmaster::instruct_cb2, tx));
}

void
testmaster::instruct_cb2(instruct_thunk *tx, int fd)
{
  if(fd < 0) {
    warn << "tcpconnect failed\n";
    tx->cb(0);
    return;
  }

  tx->fd = fd;
  fdcb(fd, selwrite, wrap(this, &testmaster::instruct_cb3, tx));
}

void
testmaster::instruct_cb3(instruct_thunk *tx) 
{
  instruct_t ins;

  ins.i.cmd = tx->cmd;
  ins.i.host = (*(in_addr*)tx->h->h_addr).s_addr;
  ins.i.port = htons(_slaves[tx->blockee].control_port);

  // XXX
  if(write(tx->fd, ins.b, 12) < 0) 
    fatal << "write\n";

  fdcb(tx->fd, selwrite, 0);
  close(tx->fd);
  tx->cb(1);
}
