/*
 *
 * Copyright (C) 2002  James Robertson (jsr@mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
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
 *
 */

#include "block.h"
#include "cs_client.h"
#include "dir.h"
#include "crypt.h"
#include "rxx.h"

int lfd;
char *chord_socket;
char hostname[1024];
int hostport;
strbuf hosturl;

int cs_client::num_active = 0;

void
cs_client::writec ()
{
}

void
cs_client::readcb()
{
  int res;

  if(timeout) {
    timecb_remove(timeout);
    timeout = NULL;
  }

  // test to wait until later to write filedata
  if(f->sleeptest(this)) {
    sleeping = true;
    fdcb(s, selread, NULL);
    return;
  } else
    fdcb(s, selread, wrap (this, &cs_client::readcb));

  res = req.input(s);
  //  warn << (int)this << " read " << res << " bytes from client\n";

  switch(res) {
  case -1:
    if(errno == EAGAIN)
      break;
    perror(progname);
  case 0:
    /* close reading. mostly useless */
    fdcb(s, selread, NULL);
    shutdown(s, 0);
    warn << (int)this << " readcb f->close()\n";
    f->close();
    return;
  }

  while(req.resid() >= BLOCKPAYLOAD) { // FIXME need to search for tag here, too
    f->write((str)req, BLOCKPAYLOAD);
    req.rembytes(BLOCKPAYLOAD);
    //    warn << (int)this << " write " << BLOCKPAYLOAD << " bytes\n";
  }

  strbuf tmp;
  tmp << "\r\n-+" << endtag;
  rxx tagrx((str)tmp);
  //  warn << (int)this << " search " << req.resid() << " bytes\n";
  if(tagrx.search((str)req)) { // FIXME what about tag on the border of 2 packets?
warn << (int)this << " readcb nomore " << req.resid() << "\n";
    f->write((str)req, tagrx.start(0));
    f->close();
    req.rembytes(req.resid());
    fdcb(s, selread, NULL);
    return;
  }
  if((req.resid()<15400) && (req.resid()>15300)) {
    warn << "body: \n" << str(req) << "\n";
  }

  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

void
cs_client::readcb_wakeup()
{
  int res;

  sleeping = false;
  fdcb(s, selread, wrap (this, &cs_client::readcb));

  res = req.input(s);
  warn << (int)this << " read " << res << " bytes from client\n";

  switch(res) {
  case -1:
    if(errno == EAGAIN)
      break;
    perror(progname);
  case 0:
    /* close reading. mostly useless */
    fdcb(s, selread, NULL);
    shutdown(s, 0);
    warn << (int)this << " readcb_wakeup f->close()\n";
    f->close();
    return;
  }

  while(req.resid() >= BLOCKPAYLOAD) {
    f->write((str)req, BLOCKPAYLOAD);
    req.rembytes(BLOCKPAYLOAD);
  }

  strbuf tmp;
  tmp << "\r\n-+" << endtag;
  rxx tagrx((str)tmp);
  if(tagrx.search((str)req)) { // FIXME what about tag on the border of 2 packets?
warn << (int)this << " readcb_wakeup nomore " << req.resid() << "\n";
    f->write((str)req, tagrx.start(0));
    f->close();
    req.rembytes(req.resid());
    fdcb(s, selread, NULL);
    return;
  }

  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

void
cs_client::requestcb()
{
  int res;

  timecb_remove(timeout);
  timeout = NULL;

  res = req.input(s);
  warn << (int)this << " read " << res << " bytes from client\n";

  switch(res) {
  case -1:
    if(errno == EAGAIN)
      break;
    perror(progname); // other error, give up.
  case 0: // no data??
    /* close reading. mostly useless */
    fdcb(s, selread, NULL);
    shutdown(s, 0);
    return;
  }

  switch(reqheaders.parse(&req)) {
  case -1:
    warn << (int)this << " error while parsing client headers\n";
    //    warn << "url: \n" << reqheaders.url << "\n";
    warn << (int)this << " header: \n" << reqheaders.headers << "\n";
    warn << (int)this << " body: \n" << str(req) << "\n";
    break;
  case 1:
    warn << (int)this << " done parsing client headers\n";
    warn << (int)this << " received request " << reqheaders.path << " " << reqheaders.method << "\n";
    if(reqheaders.authorization)
      warn << (int)this << " auth " << reqheaders.authorization << " not handled\n";
    /* got enough data. do something with chord */

#if 0
    warn << "header: \n" << reqheaders.headers << "\n";
    warn << "body: \n" << str(req) << "\n";
#endif

    dir->start(out);

    if(reqheaders.method == "POST")
      input(&req, reqheaders.r_path);
    else {
      dir->output(reqheaders.path, reqheaders.r_path);
      fdcb(s, selread, NULL);
      shutdown(s, SHUT_RD);
      return;
    }
  }

  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

static rxx boundryrx ("boundary=-+(.+)\r\n", "i");

void
cs_client::input(suio *req, str referrer)
{
  //FIXME retry on partial header
  if(!boundryrx.search(reqheaders.headers)) {
    warn << "couldn't find boundry\n";
    warn << "header: \n" << reqheaders.headers << "\n";
    warn << "body: \n" << str(*req) << "\n";
    //    return;
    exit(1);
  }
  endtag = boundryrx[1];
  warn << "found tag " << endtag << "\n";
  strbuf fileregex;
  fileregex << endtag << "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"(.+)\"\r\n.+\r\n\r\n";
  rxx filerx((str)fileregex, "i");
  if(!filerx.search((str)*req)) {
    warn << "couldn't find data\n";
    warn << "header: \n" << reqheaders.headers << "\n";
    warn << "body: \n" << str(*req) << "\n";
    //    return;
    exit(1);
  }
  str name = filerx[1];
warn << (int)this << " trying to post " << name << " to " << referrer <<"\n";
  f->openw(wrap(dir, &dirpage::add_file, name, referrer),
	   wrap(this, &cs_client::writec));
  req->rembytes(filerx.end(0));
//  if(req->resid()) FIXME
//    readcb();
  fdcb (s, selread, wrap (this, &cs_client::readcb));
}

void
cs_client::xfer_done(str status)
{
  warn << (int)this << " xfer_done " << "\n";
  warn << (int)this << " " << status << "\n";
  fdcb(s, selread, NULL);
}

void
cs_client::died()
{
  warn << (int)this << " csc DIED\n";
  timeout = NULL;
  if(sleeping)
    f->sleepdied(this);
  fdcb(s, selread, NULL);
  close(s);
  delete this;
}

cs_client::cs_client(int cfd, callback<void>::ptr am)
  : s(cfd)
{
  warn << (int)this << " new connection\n";
  num_active++;
  sleeping = false;
  accept_more = am;

  f = New melody_file(chord_socket, wrap(this, &cs_client::xfer_done));
  dir = New dirpage(f, (str)hosturl, this);
  out = New cs_output(s, wrap(this, &cs_client::died), this);

  fdcb (s, selread, wrap (this, &cs_client::requestcb));
  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

cs_client::~cs_client()
{
  warn << (int)this << " ~cs_client close " << "\n";
  num_active--;
  if(num_active < 6)
    (*accept_more)();
}


// lifted from webproxy-0.0
struct webproxy {
  const int port;
  int s;

  webproxy(int p);
  ~webproxy();
  void tryaccept();
};

webproxy::webproxy(int p)
  : port(p)
{
  s = inetsocket(SOCK_STREAM, port, INADDR_ANY);
  if(s > 0) {
    make_async(s);
    listen(s, 5);
    fdcb(s, selread, wrap(this, &webproxy::tryaccept));
  }
}

webproxy::~webproxy()
{
  fdcb(s, selread, NULL);
  close(s);
}

void
webproxy::tryaccept()
{
  int new_s;
  struct sockaddr_in *addr;
  unsigned int addrlen = sizeof(struct sockaddr_in);

  if(cs_client::num_active > 6) {
    fdcb(s, selread, NULL);
    return;
  }

  addr = (struct sockaddr_in *)calloc(1, addrlen);
  new_s = accept(s, (struct sockaddr *)addr, &addrlen);
  warn << "connect " << inet_ntoa(addr->sin_addr) << "\n";
  //  if((ntohl((int)(addr->sin_addr.s_addr)) >> 8) == 0x121a04)
    if(new_s > 0) {
      make_async(new_s);
      vNew cs_client(new_s, wrap(fdcb, s, selread, wrap(this, &webproxy::tryaccept))); // FIXME watch for leaks
    } else
      perror(progname);
    //  else
    //    close(new_s);
  free(addr);
}

void
usage (char *progname) 
{
  warn << "chord_socket local_port\n";
  exit(1);
}

void g1(str foo) {}
void g2(const char *foo, int bar, int baz) {}
void g3(int foo) {}

int
main (int argc, char *argv[])
{ 
  setprogname (argv[0]);
  sfsconst_init ();
  make_sync(1);

  if (argc < 3)
    usage (argv[0]);

  chord_socket = argv[1];
  hostport = atoi(argv[2]);
  gethostname(hostname, sizeof(hostname));
  hosturl << hostname << ":" << hostport;

  random_start();
  random_init();

  // check that root dir exists
  melody_file *f = New melody_file(chord_socket, wrap(g1));
  bool err = f->dhash->sync_setactive (1);
  assert (!err);
  dir *dr = New dir(f, wrap(g2), wrap(g3), (cs_client *)66);
  dr->root_test();

  vNew webproxy(hostport); // no leak, stays around forever

  amain();
    
  return(0);
}

// FIXME todos:
// stats stats stats
// graphics
// fix "/"es messing up dir creation in bi2.c
// worry about mime boundary tag on border of two packets and not getting found
// better error checking
// add better inodes
// alphabetize
// add consistant version numbers to filenames, to make replication work
// u/l large files uses lots of mem
// check offset counting during d/ls

// ?? T shutdown crashes. out-of order?
// crashes when trying to retrieve bad keys (now avoiding bad keys?)
// watch memory leaks
// double-check timeouts
// will the back button mess things up?
