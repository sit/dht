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
#include "testslave.h"

// fd : file descriptor that master is talking over
// socket : unix domain socket to lsd
testslave::testslave(const int fd, const str socket)
  : _srvfd(-1), _sockfd(-1)
{
  // open raw connection to unix socket
  _sockfd = unixsocket_connect(socket);
  if(!_sockfd)
    fatal << "couldn't connect to domain socket\n";
  _srvfd = fd;

  // setup pipe between remote master and local unix domain socket
  fdcb(_srvfd, selread, wrap(this, &testslave::pipe, _srvfd, _sockfd));
  fdcb(_sockfd, selread, wrap(this, &testslave::pipe, _sockfd, _srvfd));
}


testslave::~testslave()
{
  DEBUG(2) << "testslave destructor\n";
  fdcb(_srvfd, selread, 0);
  fdcb(_sockfd, selread, 0);
  // XXX: how about closing connections?
  close(_srvfd);
  close(_sockfd);
}

void
testslave::pipe(const int from, const int to)
{
  strbuf readbuf;

  int r = readbuf.tosuio()->input(from);
  if(!r) {
    delete this;
    return;
  }
  readbuf.tosuio()->output(to);
}


int server = -1;
void
accept_connection(str socket)
{
  struct sockaddr_in sin;
  unsigned sinlen = sizeof(sin);

  int fd = accept(server, (struct sockaddr *) &sin, &sinlen);
  if(fd >= 0)
    vNew testslave(fd, socket);
  else if (errno != EAGAIN)
    fatal << "Could not accept slave connection, errno = " << errno << "\n";
}


void
usage()
{
  warnx << "usage: " << progname << " [-s socket] [-p port]\n";
  warnx << "\twaits for commands from a test program and <more help>\n";
  warnx << "\noptions:\n";
  warnx << "\t-s <socket> : connect to lsd behind unix domain socket <socket>\n";
  warnx << "\t-p <port>   : listen to master on port <port>; default is " << DEFAULT_PORT << "\n";
  warnx << "\nenvironment variables:\n";
  warnx << "\tTEST_DEBUG : verbosity\n";
  exit(1);
}


int
main(int argc, char *argv[])
{
  setprogname(argv[0]);
  test_init();

  str opt_socket = "";
  int opt_port = DEFAULT_PORT;
  int ch;
  while((ch = getopt(argc, argv, "hks:p:")) != -1)
    switch(ch) {
    case 'h':
      usage();
      break;
    case 's':
      opt_socket = optarg;
      break;
    case 'p':
      opt_port = atoi(optarg);
      break;
    default:
      usage();
      break;
    }
  argc -= optind;
  argv += optind;

  if(opt_socket == "")
    fatal << "socket unknown. use -s <socket> option\n";

  // listen for master
  server = inetsocket(SOCK_STREAM, opt_port);
  if(server < 0)
    fatal << "inetsocket\n";
  make_async(server);
  if(listen(server, 5))
    fatal << "listen\n";
  fdcb(server, selread, wrap(accept_connection, opt_socket));

  make_sync(1);
  amain();
}
