#include "testmaster.h"
#include "async.h"
#include <string.h>
#include <unistd.h>

void
usage()
{
  fprintf(stderr, "\
Usage: block [-u] BLOCKER[:PORT] BLOCKEE[:PORT]\n\
\n\
Instructs BLOCKER to drop traffic from BLOCKEE.\n\
PORT in BLOCKER:PORT should be the TESLA/lsd control port, %d by default.\n\
PORT in BLOCKEE:PORT instructs to be block a specific port.  All if omitted.\n\
-u              : unblock\
", TESLA_CONTROL_PORT);
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
set(char *s, testslave *t)
{
  char *c = 0;

  t->port = 0;
  if((c = strchr(s, ':'))) {
    *c = 0;
    t->port = atoi(c+1);
  }
  t->name = s;
}

int
main(int argc, char *argv[])
{
  int mode = 0;
  testslave slaves[2];


  char ch;
  while((ch = getopt(argc, argv, "hu")) != -1) {
    switch(ch) {
      case 'h':
        usage();
        break;
      case 'u':
        mode = 1;
        break;
      default:
        usage();
    }
  }

  if(argc < 3)
    usage();

  // BLOCKER
  set(argv[optind++], &slaves[0]);
  warn << "BLOCKER host = " << slaves[0].name << ", port = " << slaves[0].port << "\n";

  // BLOCKEE
  set(argv[optind++], &slaves[1]);
  warn << "BLOCKEE host = " << slaves[1].name << ", port = " << slaves[1].port << "\n";

  testmaster tm(slaves);
  if(mode == 0)
    tm.block(0, 1, wrap(&done));
  else
    tm.unblock(0, 1, wrap(&done));

  amain();
}
