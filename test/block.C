#include "testmaster.h"
#include "async.h"
#include <string.h>
#include <unistd.h>

void
usage()
{
  fprintf(stderr, "\
To drop traffic from BLOCKEE in BLOCKER:\n\
\n\
  block [-u] BLOCKER[:P1] BLOCKEE[:P2]\n\
\n\
  BLOCKER/BLOCKEE are host names or IP addresses.\n\
  P1 is the TESLA/lsd control port; default %d.\n\
  If P2 is specified, only traffic from host/port combination is blocked.\n\
\n\
To isolate a machine completely:\n\
  block -i BLOCKEE[:P1]\n\
\n\
To undo isolation:\n\
  block -f BLOCKEE[:P1]\n\
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
  while((ch = getopt(argc, argv, "hifu")) != -1) {
    switch(ch) {
      case 'h':
        usage();
        break;
      case 'u':
        mode = 1;
        break;
      case 'i':
        mode = 2;
        break;
      case 'f':
        mode = 3;
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
  if(mode == 0 || mode == 1) {
    set(argv[optind++], &slaves[1]);
    warn << "BLOCKEE host = " << slaves[1].name << ", port = " << slaves[1].port << "\n";
  }

  testmaster tm(slaves);
  if(mode == 0)
    tm.block(0, 1, wrap(&done));
  else if(mode == 1)
    tm.unblock(0, 1, wrap(&done));
  else if(mode == 2)
    tm.isolate(0, wrap(&done));
  else if(mode == 3)
    tm.unisolate(0, wrap(&done));


  amain();
}
