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

testmaster::testmaster(char *filename) : _nhosts(0)
{
  test_init();
  FILE *f = fopen(filename, "r");
  if(f <= 0)
    fatal << "couldn't open " << filename << ": " << strerror(errno) << "\n";

  while(!feof(f)) {
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

    _slaves.push_back(New testslave(linerxx[1],
        atoi(linerxx[2]) ? atoi(linerxx[2]) : DEFAULT_PORT,
        atoi(linerxx[3]) ? atoi(linerxx[3]) : TESLA_CONTROL_PORT));
  }
  fclose(f);
}


testmaster::testmaster(vec<str> *names, vec<int> *dhp, vec<int> *tcp) : _nhosts(0)
{
  test_init();
  assert(names->size() == dhp->size() && dhp->size() == tcp->size());
  for(unsigned i=0; i<names->size(); i++) {
    _slaves.push_back(New testslave((*names)[i],
        (*dhp)[i] ? (*dhp)[i] : DEFAULT_PORT,
        (*tcp)[i] ? (*tcp)[i] : TESLA_CONTROL_PORT));
  }
}


testmaster::~testmaster()
{
}


void
testmaster::unixdomainsock(int i, str &p2psocket, int &fd)
{
  p2psocket = strbuf() << "/tmp/" << _slaves[i]->name << ":" << _slaves[i]->dhash_port;
  unlink(p2psocket);
  fd = unixsocket(p2psocket);
  if(!fd)
    fatal << "couldn't open unix socket " << p2psocket << "\n";
  make_async(fd);
  if(listen(fd, 5))
    fatal << "listen\n";
}


void
testmaster::dry_setup(callback<void>::ref cb)
{
  DEBUG(1) << "dry_setup\n";
  _nhosts = _slaves.size();

  for(unsigned i = 0; i < _slaves.size(); i++) {
    str p2psocket;
    int fd;
    unixdomainsock(i, p2psocket, fd); // by reference
    fdcb(fd, selread, wrap(this, &testmaster::accept_connection, fd, 0, true));
    dns_hostbyname(_slaves[i]->name, wrap(this, &testmaster::dry_setup_cb, i, p2psocket, cb));
  }
}


void
testmaster::dry_setup_cb(int i, str p2psocket, callback<void>::ref cb,
    ptr<hostent> h, int err)
{
  DEBUG(1) << "dry_setup_cb\n";

  if(!h)
    fatal << "dns_hostbyname " << strerror(err) << "\n";

  ptr<dhashclient_test> dhc = New refcounted<dhashclient_test>(p2psocket,
      _slaves[i]->name, (*(in_addr*)h->h_addr).s_addr,
      _slaves[i]->dhash_port, _slaves[i]->control_port);
  client *c = New client(i, 0, "", dhc);
  _clients.insert(c);

  if(!(--_nhosts))
    cb();
}


void
testmaster::setup(callback<void>::ref cb)
{
  for(unsigned i = 0; i < _slaves.size(); i++) {
    // bogus unix domain socket
    str p2psocket;
    int fd;
    unixdomainsock(i, p2psocket, fd); // by reference

    // create a connection to the other side for this unix socket
    conthunk tx = { i, p2psocket, _slaves[i], fd, cb };
    addnode(tx);
    _nhosts++;
  }
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
testmaster::addnode(conthunk tx)
{
  DEBUG(2) << "setting up " << tx.s->name << ":" << tx.s->dhash_port << "\n";

  // remove old entry
  client *c = _clients[tx.id];
  if(c)
    _clients.remove(c);
  tcpconnect(tx.s->name, tx.s->dhash_port, wrap(this, &testmaster::addnode_cb, tx));
}




// accepts connection from dhashclient that we just created and sets up pipe
void
testmaster::accept_connection(const int unixsocket_fd, const int there_fd, bool bogus = false)
{
  struct sockaddr_in sin;
  unsigned sinlen = sizeof(sin);

  DEBUG(2) << "accept_connection from dhashclient\n";

  int here_fd = accept(unixsocket_fd, (struct sockaddr *) &sin, &sinlen);
  if(bogus)
    return;

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

  // resolve the remote hostname
  dns_hostbyname(tx.s->name, wrap(this, &testmaster::addnode_cb2, tx, there_fd));
}


void
testmaster::addnode_cb2(conthunk tx, const int there_fd, ptr<hostent> h, int err)
{
  if(!h) {
    fatal << "dns_hostbyname failed\n";
  }

  // listen for incoming connection from dhashclient that we are about to
  // create
  /*
  DEBUG(2) << "spawning server behind " << tx.p2psocket << "\n";
  if(listen(tx.unixsocket_fd, 5))
    fatal << "listen\n";
  */
  fdcb(tx.unixsocket_fd, selread, wrap(this, &testmaster::accept_connection, tx.unixsocket_fd, there_fd, false));


  // create DHash object
  DEBUG(2) << "creating dhashclient on " << tx.p2psocket << "\n";
  make_async(there_fd);
  ptr<dhashclient_test> dhc = New refcounted<dhashclient_test>(tx.p2psocket, 
      tx.s->name, (*(in_addr*)h->h_addr).s_addr, tx.s->dhash_port, tx.s->control_port);

  if(!dhc)
    fatal << "couldn't create dhashclient on " << tx.p2psocket << "\n";

  // update hash table
  client *c = New client(tx.id, tx.s, tx.p2psocket, dhc);
  _clients.insert(c);

  // call callback once all slaves are connected
  if(!(--_nhosts))
    tx.cb();
}
