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
#include "cs_output.h"
#include <time.h>

int lfd;
char *chord_socket;
char hostname[1024];
int hostport;
strbuf hosturl;
struct ipmask {
  unsigned int ip;
  unsigned int mask;
};
vec<struct ipmask> ipm;
int log;

char bad1[4096], bad2[4096];
void fromurl(const char *url, char *out) {
  unsigned int tmp;

  if(url == NULL) {
    *out = 0;
    return;
  }

  while(*url) {
    if(*url == '%') {
      sscanf(url, "%%%02x", &tmp);
      *out = tmp & 0xff;
      url += 2;
    } else if(*url == '+')
      *out = ' ';
    else
      *out = *url;
    url++;
    out++;
  }
  *out = 0;
}



int cs_client::num_active = 0;

void
cs_client::writec ()
{
}

void
cs_client::put2sleep()
{
  sleeping = true;
  fdcb(s, selread, NULL);
}

void
cs_client::wakeup()
{
  f->next();
}

void
cs_client::readcb()
{
  if(timeout) {
    timecb_remove(timeout);
    timeout = NULL;
  }

  // test to wait until later to write filedata
  if(f->sleeptest(this)) {
    put2sleep();
    return;
  } else
    readcb_actual();
}

void
cs_client::readcb_wakeup()
{
  sleeping = false;
  fdcb(s, selread, wrap (this, &cs_client::readcb));
  readcb_actual();
}

void
cs_client::readcb_actual()
{
  int res = req.input(s);
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
    warn << (int)this << " readcb_actual f->close()\n";
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

  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

static rxx adddirrx ("add_dir=([^;/]+)", "i"); // FIXME limit chars in dir?

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
    strbuf logbuf;
    logbuf << hosturl << " " << time(NULL) << " " << inet_ntoa(ip) << " "; 

    if(reqheaders.method == "POST") {
      if(!strncmp(reqheaders.path.cstr(), "/dstore", 7)) {
	fromurl(reqheaders.r_path, bad2);
	input(&req, bad2);
      } else {
	done = true;
	if(adddirrx.search((str)req)) {
	  logbuf << "adddir " << adddirrx[1] << " to " << reqheaders.r_path << "\n";
	  logbuf.tosuio()->output(log);
	  fsync(log);
	  fromurl(adddirrx[1], bad1);
	  fromurl(reqheaders.r_path, bad2);
	  dir->add_dir(bad1, bad2);
	  fdcb(s, selread, NULL);
	  shutdown(s, SHUT_RD);
	  return;
	} else {
	  // FIXME send error page
	  logbuf << "bad request: " << (str)req << "\n";
	  logbuf.tosuio()->output(log);
	  fsync(log);
	  warn << "bad body: " << (str)req << "\n";
	  return;
	}
      }
    } else {
      if(!strncmp(reqheaders.path.cstr(), "/exit", 5)) {
	logbuf << "exit\n";
	logbuf.tosuio()->output(log);
	fsync(log);
	close(log);
	delete(dir);
	sleep(1); // hack
	out->take("goodbye"); // FIXME not working???
	out->died(); //eep
	exit(0);
      }
      logbuf << "requesting: " << reqheaders.path << "\n";
      logbuf.tosuio()->output(log);
      fsync(log);
      fromurl(reqheaders.path, bad1);
      fromurl(reqheaders.r_path, bad2);
      dir->output(bad1, bad2);
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
    return;
    //    exit(1);
  }
  endtag = boundryrx[1];
  warn << "found tag " << endtag << "\n";
  strbuf fileregex;
  fileregex << endtag << "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"([^;/]+)\"\r\n.+\r\n\r\n";
  rxx filerx((str)fileregex, "i");
  if(!filerx.search((str)*req)) {
    warn << "couldn't find data\n";
    warn << "header: \n" << reqheaders.headers << "\n";
    warn << "body: \n" << str(*req) << "\n";
    return;
    //    exit(1);
  }
  str name = filerx[1];
warn << (int)this << " trying to post " << name << " to " << referrer <<"\n";
 strbuf logbuf;
 logbuf << hosturl << " " << time(NULL) << " " << inet_ntoa(ip) << " " << " addfile " << name << " to " << referrer << "\n";
 logbuf.tosuio()->output(log);
 fsync(log);
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

// should only be called once cs_output is done.
void
cs_client::died()
{
  warn << (int)this << " csc DIED\n";
  timeout = NULL;
  if(sleeping)
    f->sleepdied(this);
  fdcb(s, selread, NULL);
  close(s);
  //  delete(f);
  delete(this);
}

cs_client::cs_client(int cfd, callback<void>::ptr am, in_addr aip)
  : s(cfd), done(false)
{
  warn << (int)this << " new connection\n";
  num_active++;
  sleeping = false;
  accept_more = am;
  ip = aip;

  f = New refcounted<melody_file>(chord_socket, wrap(this, &cs_client::xfer_done));
  dir = New dirpage(f, (str)hosturl, this);
  out = New cs_output(s, wrap(this, &cs_client::died), this, wrap(dir, &dirpage::fileout_stop));

  fdcb (s, selread, wrap (this, &cs_client::requestcb));
  timeout = delaycb(10, 0, wrap(this, &cs_client::died));
}

cs_client::~cs_client()
{
  warn << (int)this << " ~cs_client close " << "\n";
  num_active--;
  if(num_active < 200)
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

  if(cs_client::num_active > 200) { // FIXME no limit?
    fdcb(s, selread, NULL);
    return;
  }

  addr = (struct sockaddr_in *)calloc(1, addrlen);
  new_s = accept(s, (struct sockaddr *)addr, &addrlen);
  warn << "connect " << inet_ntoa(addr->sin_addr) << "\n";

  bool ipm_ok = false;
  for(unsigned int i=0; i<ipm.size(); i++)
    if(ipm[i].ip == (ntohl((int)(addr->sin_addr.s_addr)) & ipm[i].mask))
       ipm_ok = true;
  if(ipm.size() == 0) ipm_ok = true;

  if(ipm_ok)
    if(new_s > 0) {
      make_async(new_s);
      vNew cs_client(new_s, wrap(fdcb, s, selread, wrap(this, &webproxy::tryaccept)), addr->sin_addr); // FIXME watch for leaks
    } else
      perror(progname);
  else {
    //    warn << "refusing connection from outside of ipmask: " << ((ipmask>>24)&0xff) << "." << ((ipmask>>16)&0xff) << "." << ((ipmask>>8)&0xff) << "." << (ipmask&0xff) << "." << "\n";
    warn << "denied by ipmask\n";
    char *bad = "HTTP/1.1 200 OK\r\nServer: melody\r\nPragma: no-cache\r\n\r\nsorry, you are outside the network that this server answers to.\r\nno mp3s for you\r\n";
    write(new_s, bad, strlen(bad));
    close(new_s);
  }
  free(addr);
}

void
usage (char *progname) 
{
  warn << "chord_socket local_port [ip mask]...\n";
  exit(1);
}

void g1(str foo) {}
void g2(const char *foo, int bar, int baz) {}
void g3(int foo, str bar) {}

static rxx ipmrx ("(\\d+).(\\d+).(\\d+).(\\d+)");

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

  strbuf foo;
  foo << hosturl << ".log";
  log = open(str(foo), O_CREAT | O_WRONLY | O_APPEND, 0600);
  strbuf logbuf;
  logbuf << hosturl << " " << time(NULL) << " "; 
  logbuf << "start\n";
  logbuf.tosuio()->output(log);
  fsync(log);

  int cur = 3;
  while((cur+1) < argc) {
    struct ipmask *im = (struct ipmask *)malloc(sizeof(struct ipmask));
    if(im == NULL) {
      perror("main ipmask");
      exit(1);
    }

    if (!ipmrx.search(argv[cur++]))
      usage (argv[0]);
    im->ip = (atoi(ipmrx[1]) << 24) | 
      (atoi(ipmrx[2]) << 16) | 
      (atoi(ipmrx[3]) << 8) | 
      atoi(ipmrx[4]);
    if (!ipmrx.search(argv[cur++]))
      usage (argv[0]);
    im->mask = (atoi(ipmrx[1]) << 24) | 
      (atoi(ipmrx[2]) << 16) | 
      (atoi(ipmrx[3]) << 8) | 
      atoi(ipmrx[4]);

    ipm.push_back(*im);
  }

  random_start();
  random_init();

  // check that root dir exists
  ptr<melody_file>f = New refcounted<melody_file>(chord_socket, wrap(g1));
  bool err = f->dhash->sync_setactive (1);
  assert (!err);
  dir *dr = New dir(f, wrap(g2), wrap(g3), (cs_client *)66);
  dr->root_test(); // FIXME adds ".." everytime??

  vNew webproxy(hostport); // no leak, stays around forever

  amain();
    
  return(0);
}

// FIXME todos:
// adding files is way too hard. horrible commandline utility with idiosyncractic nature. need some graphical drag-and-drop simplicity.

// why not use apache? -can't use real dirs, must use cgi arg instead
//                     -do redirects work?
//                     -can cgi's read in MIME posted files?
//                     -can cgi's throttle outgoing bytes?
//    ** nevermind ->  -how to synchronize multiple users of dhash unix domain socket?
// look at fixed-size buffers. some are "bad"
// reverse sort by time
// make sure error web page is returned for every error condition. (memory leaks otherwise?)
// stats stats stats
// graphics?
// better error checking
// is random num gen (for dir keys) good enough?
// be able to set host ipaddr/hostname
// worry about mime boundary tag on border of two packets and not getting found
// u/l large files uses lots of mem
// check offset counting during d/ls

// ?? T shutdown crashes. out-of order?
// crashes when trying to retrieve bad keys (now avoiding bad keys?)
// watch memory leaks
// double-check timeouts
// will the back button mess things up?
