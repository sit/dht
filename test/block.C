/*
 * block.{C,h}  command-line tool to play network tricks
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

#include "testmaster.h"
#include "async.h"
#include <string.h>
#include <unistd.h>

#define BLOCK      0x001 // specific host
#define UNBLOCK    0x002
#define ISOLATE    0x010 // solitary confinement
#define UNISOLATE  0x020
#define DROP       0x100 // drop percentage of packets

void
usage()
{
  fprintf(stderr, "\
Sets and unsets network blockades on running lsd's.\n\
\n\
  block [flags] N[:P]\n\
\n\
  The following flags can be used:\n\
   -r         : read\n\
   -w         : write\n\
\n\
   -d D:M[:P] : drop D percent of packets from/to node M[:P]\n\
   -d D       : drop D percent of packets from/to all\n\
\n\
   -b M       : block reading from/writing to node M \n\
   -u M       : unblock, mutually exclusive with -b\n\
\n\
   -i         : disallow all reading/writing (isolate)\n\
   -f         : unset isolation, mutually exclusive with -i\n\
\n\
   -h         : show this help\n\
\n\
  N and M are hostnames or IP address, P is a port number\n\
\n\
  Examples:\n\
\n\
    block -r -b foo.mit.edu bar.mit.edu\n\
      => tells bar.mit.edu to block all traffic from foo.mit.edu\n\
\n\
    block -rw -b foo.mit.edu bar.mit.edu\n\
      => tells bar.mit.edu to block all traffic to and from foo.mit.edu\n\
\n\
    block -r -i bar.mit.edu:8003\n\
      => tells bar.mit.edu to isolate itself off the network for reading,\n\
         the control port is on port 8003");
  exit(-1);
}





void
done(int ok)
{
  if(!ok)
    fatal << "error\n";
  exit(0);
}


void
set(char *s, vec<str> *names, vec<int> *tcp)
{
  char *c = 0;
  int control_port;

  control_port = TESLA_CONTROL_PORT;
  if((c = strchr(s, ':'))) {
    *c = 0;
    control_port = atoi(c+1);
  }

  tcp->push_back(control_port);
  names->push_back(s);
}


int droprate = 0;
char *dropfrom = 0;


void
start_test(int cmd, dhashclient_test::rw_t flags, testmaster *tm)
{
  if(cmd & BLOCK)
    (*tm)[0]->block(flags, (*tm)[1], wrap(&done));
  else if(cmd & UNBLOCK)
    (*tm)[0]->unblock(flags, (*tm)[1], wrap(&done));
  else if(cmd & ISOLATE)
    (*tm)[0]->isolate(flags, wrap(&done));
  else if(cmd & UNISOLATE)
    (*tm)[0]->unisolate(flags, wrap(&done));
  else if(cmd & DROP)
    (*tm)[0]->drop(flags, 0, droprate, wrap(&done));
}



int
main(int argc, char *argv[])
{
  int cmd = 0;
  int flags = 0;

  vec<str> names;
  vec<int> dhp;
  vec<int> tcp;
  char *blockee = 0;


  char ch;
  while((ch = getopt(argc, argv, "d:hrwifb:u:")) != -1) {
    switch(ch) {
      case 'h':
        usage();
        break;

      case 'r':
        flags |= dhashclient_test::READ;
        break;

      case 'w':
        flags |= dhashclient_test::WRITE;
        break;

      case 'd':
        cmd |= DROP;
        char *c;
        if((c = strchr(optarg, ':'))) {
          *c = 0;
          dropfrom = c+1;
        }
        droprate = atoi(optarg);
        break;


      case 'i':
        if(cmd & UNISOLATE)
          fatal << "ISOLATE/UNISOLATE mutually exclusive";
        cmd |= ISOLATE;
        break;

      case 'f':
        if(cmd & ISOLATE)
          fatal << "ISOLATE/UNISOLATE mutually exclusive";
        cmd |= UNISOLATE;
        break;

      case 'b':
        if(cmd & UNBLOCK)
          fatal << "BLOCK/UNBLOCK mutually exclusive";
        cmd |= BLOCK;
        blockee = optarg;
        break;

      case 'u':
        if(cmd & BLOCK)
          fatal << "BLOCK/UNBLOCK mutually exclusive";
        cmd |= UNBLOCK;
        blockee = optarg;
        break;

      default:
        usage();
    }
  }

  if(!(cmd & (BLOCK|UNBLOCK|ISOLATE|UNISOLATE|DROP))) {
    warn << "nothing to do\n";
    exit(0);
  }

  // BLOCKER
  if(!argv[optind]) {
    warn << "missing argument. use -h for help.\n";
    exit(-1);
  }

  set(argv[optind++], &names, &tcp);
  dhp.push_back(0);

  warn << "BLOCKER host = " << names[0] << ", control_port = " << tcp[0] << "\n";

  // BLOCKEE
  if(cmd & BLOCK || cmd & UNBLOCK) {
    set(blockee, &names, &tcp);
    dhp.push_back(0);
    warn << "BLOCKEE host = " << names[1] << ", control_port = " << tcp[1] << "\n";
  }

  testmaster *tm = New testmaster(&names, &dhp, &tcp);
  tm->dry_setup(wrap(start_test, cmd, (dhashclient_test::rw_t) flags, tm));
  amain();
}
