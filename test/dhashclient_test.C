/*
 * dhashclient_test.{C,h} --- superset of normal dhashclient
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


#include "dhashclient_test.h"


dhashclient_test::dhashclient_test(str unix_socket, str name,
    unsigned myip, unsigned dhp, unsigned cntp) : dhashclient(unix_socket)
{
  _test_name = name;
  _test_dhashport = dhp;
  _test_controlport = cntp;
  _myip = myip;
}




void
dhashclient_test::block(rw_t flags, dhashclient_test *dht, callback<void, int>::ref cb)
{
  // drop 100%
  drop(flags, dht, 100, cb);
}

void
dhashclient_test::unblock(rw_t flags, dhashclient_test *dht, callback<void, int>::ref cb)
{
  // drop 0%
  drop(flags, dht, 0, cb);
}

void
dhashclient_test::isolate(rw_t flags, callback<void, int>::ref cb)
{
  instruct_thunk *tx = instruct_init(ISOLATE, cb);
  ISOLATE_ARG(tx, flags) = flags;
  instruct(tx);
}

void
dhashclient_test::unisolate(rw_t flags, callback<void, int>::ref cb)
{
  instruct_thunk *tx = instruct_init(UNISOLATE, cb);
  ISOLATE_ARG(tx, flags) = flags;
  instruct(tx);
}


void
dhashclient_test::drop(rw_t flags, dhashclient_test *dht, int percentage,
    callback<void, int>::ref cb)
{
  printf("drop flags = %x\n", flags);

  // XXX: we cannot drop packets from a specific host/port combination because
  // we don't know the port that lsd uses to send.  what we can do, however, is
  // tell the blockee to drop packets to us.
  //
  // we have to make sure, though, not to do the callback twice
  int *n = 0;
  if((flags & READ) && dht) {
    warn << "cannot drop READS from specific host. doing a symmetric drop WRITE\n";

    if(flags == READ) {
      warn << "full reverse\n";
      dht->drop(WRITE, this, percentage, cb);
      return;
    }

    // XXX: foul hack.  do a reverse drop-call.  lockcounter has to drop to zero
    // before we do the callback.
    warn << "doing partial send\n";
    instruct_thunk *tx = instruct_init(DROP, cb);
    DROP_ARG(tx, flags) = WRITE;
    DROP_ARG(tx, host) = _myip;
    DROP_ARG(tx, port) = _test_dhashport;
    DROP_ARG(tx, perc) = percentage;
    n = New int(2);
    tx->lockcounter = n;
    instruct(tx);

    // casting trickery for gcc
    flags = (rw_t) (((int) flags) &= ~READ);
  }

  instruct_thunk *tx = instruct_init(DROP, cb);
  DROP_ARG(tx, flags) = flags;
  DROP_ARG(tx, host) = dht ? dht->ip() : 0;
  DROP_ARG(tx, port) = dht ? dht->dhashport() : 0;
  DROP_ARG(tx, perc) = percentage;
  if(n) {
    delete tx->lockcounter;
    tx->lockcounter = n;
  }
  instruct(tx);
}


dhashclient_test::instruct_thunk*
dhashclient_test::instruct_init(unsigned cmd, callback<void, int>::ref cb)
{
  instruct_thunk *tx = New instruct_thunk(cb);
  if(!tx)
    fatal << "malloc!\n";

  CMD_ARG(tx) = cmd;
  return tx;
}


void
dhashclient_test::instruct(instruct_thunk *tx)
{
  DEBUG(1) << "tcppconnect to " << _test_name << ":" << _test_controlport << "\n";
  tcpconnect(_test_name, _test_controlport, wrap(this, &dhashclient_test::instruct_cb1, tx));
}

void
dhashclient_test::instruct_cb1(instruct_thunk *tx, int fd)
{
  if(fd < 0) {
    warn << "tcpconnect failed\n";
    if(tx->lockcounter)
      *tx->lockcounter = 0;
    tx->cb(0);
    delete tx;
    return;
  }

  tx->fd = fd;
  fdcb(fd, selwrite, wrap(this, &dhashclient_test::instruct_cb2, tx));
}

void
dhashclient_test::instruct_cb2(instruct_thunk *tx) 
{
  // XXX
  printf("writing cmd = %x\n", CMD_ARG(tx));
  if(write(tx->fd, tx->instruct.b, 20) < 0) 
    fatal << "write\n";

  fdcb(tx->fd, selwrite, 0);
  close(tx->fd);

  // when we're not using a lockcounter, do callback.
  // if we're using a lockcounter, only do callback when it dropped to zero
  if(tx->lockcounter)
    warn << "instruct_cb2, lockcounter set to " << *tx->lockcounter << "\n";
  if(!tx->lockcounter || !(--(*tx->lockcounter)))
    tx->cb(1);
  delete tx;
}
