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
  
void parse_args(int argc, char *argv[]);
void usage(char *);

void
threadmain(int argc, char *argv[])
{
  extern int anyready();

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
  if(argc <= 2) {
    usage(argv[0]);
    exit(1);
  }

  if(!strcmp(argv[1], "-h")) {
    usage(argv[0]);
    exit(0);
  }

  topology_file = argv[1];
  event_file = argv[2];
}


void
usage(char *argv0)
{
  cout << argv0 << " [-h] TOPOLOGY EVENTS" << endl;
}
