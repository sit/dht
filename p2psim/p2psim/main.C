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

  // make sure the network ate all the nodes
  while(anyready())
    yield();

  // Creates an event queue, parses the file, etc.
  EventGenerator::parse(event_file);

  // will be done by one of the event generators
  // EventQueue::Instance()->go();
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
  cout << "Usage: p2psim [-v] [-b <degree>] [-f <fingers>] [-r <resilience>] [-s <succ>] PROTOCOL TOPOLOGY EVENTS" << endl;
}
