/*
 * testslave.{C,h} --- a slave that pipes DHash RPCs to lsd
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
#include "test_prot.h"
#include "testslave.h"
#include <ctype.h>

void usage();

// mfd : file descriptor that master is talking over
// lfd : file descriptor that we are talking to lsd over
// socket : unix domain socket to lsd
// 
testslave::testslave(int argc, char *argv[])
  : _master_listener(-1), _masterfd(-1), _lsdsockfd(-1),
    _lsd_listener(-1)
{
  srand(time(NULL) & (getpid() + (getpid() << 15)));


  int opt_masterport = DEFAULT_PORT;
  int opt_lsdport = -1;
  char *opt_j = 0, *opt_l = 0, *opt_S = 0, *opt_r = 0;
  int ch;
  while((ch = getopt(argc, argv, "hs:p:j:l:r:S:m:")) != -1)
    switch(ch) {
    case 'h':
      usage();
      break;
    case 'm':   // port to listen for master (TCP)
      opt_masterport = atoi(optarg);
      break;
    case 'p':   // port to listen for other lsd's (UDP)
      opt_lsdport = atoi(optarg);
      break;
    case 'j':   // -j hostname:port option to lsd
      opt_j = optarg;
      break;
    case 'l':   // -l hostname:port option to lsd
      opt_l = optarg;
      break;
    case 'r':   // -r RPC socket
      opt_r = optarg;
      break;
    case 'S':   // -S <socket> option to lsd
      opt_S = optarg;
      break;
    default:
      usage();
      break;
    }

  argc -= optind;
  argv += optind;

  opt_S = opt_S ? opt_S : getenv("LSD_SOCKET");
  if(!opt_r) 
    fatal << "no -r flag\n";

  // set up RPC channel for master
  _ss = inetsocket(SOCK_DGRAM, atoi(opt_r), INADDR_ANY);
  if(_ss < 0){
    fprintf(stderr, "ticker-server: inetsocket failed\n");
    exit(1);
  }
  _sx = axprt_dgram::alloc(_ss);
  _s = asrv::alloc(_sx, dhash_test_prog_1, wrap(testslave::dispatch, this));

  bool do_lsd = false;
  if(opt_l && opt_j && opt_S)
    do_lsd = true;
  else if(!(opt_l && opt_j && opt_S))
    fatal << "you need to specify -l, -j, and -S to spin off your own lsd\n";

  // listen for connection master
  warn << "listening for master on port " << opt_masterport << "\n";
  _master_listener = inetsocket(SOCK_STREAM, opt_masterport);
  if(_master_listener < 0)
    fatal << "inetsocket _master_listener, fd = " << _master_listener << ", errno = " << strerror(errno) << "\n";
  make_async(_master_listener);
  if(listen(_master_listener, 5))
    fatal << "listen for master\n";
  fdcb(_master_listener, selread, wrap(this, &testslave::accept_master));

  // come up with some random port to run spun off lsd on
  strbuf randstr;
  unsigned rndport = 4096 + (int) (8192.0 * rand() / (RAND_MAX + 4096.0));
  randstr << rndport;

  // listen for connections from lsd's
  warn << "listening for lsd on port " << opt_lsdport << "\n";
  _lsd_listener = inetsocket(SOCK_DGRAM, opt_lsdport);
  if(_lsd_listener < 0)
    fatal << "_lsd_listener\n";
  make_async(_lsd_listener);

  // now return, let amain() be called and spin off lsd in a bit
  if(do_lsd)
    delaycb(2, wrap(this, &testslave::start_lsd, opt_j, opt_l, opt_S, str(randstr)));
}

void
testslave::dispatch(testslave *ts, svccb *sbp)
{
  test_result test_res;

  switch(sbp->proc()){
  case TEST_BLOCK:
    // ts->do_submit(sbp->template getarg<submit_args> (), &submit_res);
    sbp->reply(&test_res);
    break;
  case TEST_UNBLOCK:
    sbp->reply(&test_res);
    break;
  default:
    sbp->reject(PROC_UNAVAIL);
    break;
  }
}


void
testslave::start_lsd(const char *j, const char *l, const char *S, const str p)
{
  if(!fork()) {
    warn << "spinning off lsd\n";
    char fake_host[64];
    strcpy(fake_host, j);
    char *colon = strchr(fake_host, ':');
    *colon = '\0';

    setenv("LSD_FAKEMYPORT", strchr(j, ':')+1, 0);
    setenv("LSD_FAKEMYHOST", fake_host, 0);
    execlp("lsd", "lsd", "-j", j, "-l", l, "-S", S, "-p", p.cstr(), 0);
  }

  // give lsd some time to settle down
  delaycb(2, wrap(this, &testslave::start_lsd2, S, (u_int16_t) atoi(p.cstr())));
}


void
testslave::start_lsd2(const char *S, const u_int16_t p)
{
  // open connection to local lsd socket. this lsd was either running already,
  // or we just spun it off.
  printf("opt_S = %s\n", S);
  _lsdsockfd = unixsocket_connect(S);
  if(_lsdsockfd < 0)
    fatal << "couldn't connect to domain socket\n";
  make_async(_lsdsockfd);

  // WE are going to run on the port that was passed to lsd, and we are going to
  // run lsd on some random other port. set up a pipe between thenm
  fdcb(_lsd_listener, selread, wrap(this, &testslave::udppipe, _lsd_listener, p, -1));
  // fdcb(_lsdportfd, selread, wrap(this, &testslave::udppipe, _lsdportfd, _lsd_listener));

  // setup pipe between for master -> lsd communication
  /*
  warn << "fdcbw\n";
  fdcb(_masterfd, selread, wrap(this, &testslave::tcppipe, _masterfd, _lsdsockfd));
  warn << "fdcbw done 1\n";
  fdcb(_lsdsockfd, selread, wrap(this, &testslave::tcppipe, _lsdsockfd, _masterfd));
  warn << "fdcbw done 2\n";
  */
}

testslave::~testslave()
{
  DEBUG(2) << "testslave destructor\n";
  fdcb(_masterfd, selread, 0);
  fdcb(_lsdsockfd, selread, 0);
}

void
testslave::tcppipe(const int from, const int to)
{
  strbuf readbuf;

  int r = readbuf.tosuio()->input(from);
  if(!r) {
    delete this;
    return;
  }

  readbuf.tosuio()->output(to);
}

void
testslave::udppipe(const int fromfd, const u_int16_t toport, const int tofd)
{
  char buf[4096];
  ssize_t n;
  sockaddr_in sin;
  socklen_t sinlen = sizeof (sin);
  int tofiled = tofd;

  bzero (&sin, sizeof (sin));
  n = recvfrom(fromfd, reinterpret_cast<char *> (buf), sizeof (buf),
                0, reinterpret_cast<sockaddr *> (&sin), &sinlen);

  if(n < 0)
    fatal << "error reading: " << strerror(errno) << "\n";

  // open connection to send this message off
  if(tofiled == -1) {
    tofiled = inetsocket(SOCK_DGRAM);
    if(tofiled < 0)
      fatal << "couldn't open socket";

    // create back pipe to remote lsd
    fdcb(tofiled, selread, wrap(this, &testslave::udppipe, tofiled, ntohs(sin.sin_port), fromfd));
  }

  // write it to the specified port
  sin.sin_port = htons(toport);
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  n = sendto(tofiled, buf, n, 0, reinterpret_cast<sockaddr*> (&sin), sinlen);
}


void
testslave::accept_master()
{
  struct sockaddr_in sin;
  unsigned sinlen = sizeof(sin);

  int fd = accept(_master_listener, (struct sockaddr *) &sin, &sinlen);
  if(fd < 0 && errno != EAGAIN)
    fatal << "Could not accept slave connection, errno = " << errno << "\n";
  _masterfd = fd;
}


void
usage()
{
  warnx << "usage: " << progname << " [-s socket] [-p port]\n";
  warnx << "\twaits for commands from a test program and <more help>\n";
  warnx << "\noptions:\n";
  warnx << "\t-s <socket> : connect to lsd behind unix domain socket <socket>\n";
  warnx << "\t-p <port>   : listen to master on port <port>; default is " << DEFAULT_PORT << "\n";
  warnx << "\t-j <opt>    : pass <opt> as -j to lsd\n";
  warnx << "\t-l <opt>    : pass <opt> as -l to lsd\n";
  warnx << "\t-S <opt>    : pass <opt> as -S to lsd\n";
  warnx << "\nenvironment variables:\n";
  warnx << "\tLSD_SOCKET : default value for -s option\n";
  warnx << "\tTEST_DEBUG : verbosity\n";
  exit(1);
}


#define MAX_ARG 64

int
main(int argc, char *argv[])
{
  setprogname(argv[0]);
  test_init();

  make_sync(1);
  vNew testslave(argc, argv);
  amain();
}
