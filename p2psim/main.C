#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <typeinfo>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "chord.h"
#include "network.h"
#include "event.h"
#include "eventqueue.h"
#include "topology.h"

char *topology_file;
char *event_file;
bool vis = false;
  
void parse_args(int argc, char *argv[]);
void usage();

void
threadmain(int argc, char *argv[])
{
  extern int anyready();

  verbose = getenv("P2PSIM_DEBUG") ? atoi(getenv("P2PSIM_DEBUG")) : 0;

  srandom(time(0) ^ (getpid() + (getpid() << 15)));
  parse_args(argc, argv);

  // Creates a network with the appropriate underlying topology.
  Topology::parse(topology_file);

  // make sure the network ate all the nodes
  while(anyready())
    yield();

  // Creates an event queue, parses the file, etc.
  EventQueue::Instance()->parse(event_file);

  // Tell the eventqueue to start processing events.
  EventQueue::Instance()->go();
}

void
parse_args(int argc, char *argv[])
{
  setprogname (argv[0]);

  int ch;
  while ((ch = getopt (argc, argv, "v")) != -1) {
    switch (ch) {
    case 'v':
      {
	vis = true;
	argc--;
	argv++;
      }
      break;
  default:
    usage ();
    }
  };

  if(argc != 3) {
    usage();
    exit(1);
  }

  topology_file = argv[1];
  event_file = argv[2];
}


void
usage()
{
  cout << "Usage: p2psim [-v] TOPOLOGY EVENTS" << endl;
}
