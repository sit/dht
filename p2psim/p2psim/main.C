/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu),
 *                    Jinyang Li (jinyang@csail.mit.edu)
 *                    Frans Kaashoek (kaashoek@csail.mit.edu).
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "protocols/protocolfactory.h"
#include "protocol.h"
#include "topology.h"
#include "eventgenerator.h"
#include <time.h>
#include <iostream>
using namespace std;

char *topology_file;
char *event_file;
char *protocol_file;
bool vis = false;
bool with_failure_model = true;

void parse_args(int argc, char *argv[]);
void usage();

void
threadmain(int argc, char *argv[])
{
  extern int anyready();

  verbose = getenv("P2PSIM_DEBUG") ? atoi(getenv("P2PSIM_DEBUG")) : 0;

  srandom(time(0) ^ (getpid() + (getpid() << 15)));
  parse_args(argc, argv);

  // Put a protocol on all these nodes.
  Protocol::parse(protocol_file);

  // Creates a network with the appropriate underlying topology.
  Topology::parse(topology_file);

  // Initialize all protocols
  set<Protocol*> all = ProtocolFactory::Instance()->getallprotocols();
  for(set<Protocol*>::const_iterator i = all.begin(); i != all.end(); ++i)
    (*i)->initstate(&all);

  // make sure the network ate all the nodes
  while(anyready())
    yield();

  // Creates an event queue, parses the file, etc.
  // Will fire off the EventQueue
  EventGenerator::parse(event_file);
}


void
parse_args(int argc, char *argv[])
{
  int ch;
  uint seed;

  while ((ch = getopt (argc, argv, "e:fv")) != -1) {
    switch (ch) {
    case 'e':
      seed = atoi(optarg);
      // fprintf(stderr,"srand set seed to %u\n",seed);
      srandom(seed);
      break;
    case 'f':
      with_failure_model = false;
      break;
    case 'v':
      vis = true;
      break;
    default:
      usage ();
    }
  }
  argc -= optind;
  argv += optind;

  if(argc != 3) {
    usage();
    exit(1);
  }

  protocol_file = argv[0];
  topology_file = argv[1];
  event_file = argv[2];
}


void
usage()
{
  cout << "Usage: p2psim [-v] [-f] [-e SEED] PROTOCOL TOPOLOGY EVENTS" << endl;
  cout << "-v       : with vis" << endl;
  cout << "-f       : disable support for failure models" << endl;
  cout << "-e SEED  : set random seed SEED" << endl;
  cout << "PROTOCOL : name of a protocol file" << endl;
  cout << "TOPOLOGY : name of a topology file" << endl;
  cout << "EVENTS   : name of an events file" << endl;
}
