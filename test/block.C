#include "testmaster.h"
#include "async.h"
#include <string.h>
#include <unistd.h>

void
usage()
{
  fprintf(stderr, "\
Sets and unsets network blockades on running lsd's.\n\
\n\
  block [flags] N[:P]\n\
\n\
  The following flags can be used:\n\
   -r   : read\n\
   -w   : write\n\
\n\
   -b M : block reading from/writing to node M \n\
   -u M : unblock, mutually exclusive with -b\n\
\n\
   -i   : disallow all reading/writing (isolate)\n\
   -f   : unset isolation, mutually exclusive with -i\n\
\n\
   -h   : show this help\n\
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
set(char *s, testslave *t)
{
  char *c = 0;

  t->control_port = TESLA_CONTROL_PORT;
  if((c = strchr(s, ':'))) {
    *c = 0;
    t->control_port = atoi(c+1);
  }
  t->name = s;
}



int
main(int argc, char *argv[])
{
  int cmd = 0;
  testslave slaves[2];
  char *blockee = 0;


  char ch;
  while((ch = getopt(argc, argv, "hrwifb:u:")) != -1) {
    switch(ch) {
      case 'h':
        usage();
        break;

      case 'r':
        cmd |= READ;
        break;

      case 'w':
        cmd |= WRITE;
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

  if(!(cmd & (BLOCK|UNBLOCK|ISOLATE|UNISOLATE))) {
    warn << "nothing to do\n";
    exit(0);
  }

  // BLOCKER
  if(!argv[optind]) {
    warn << "missing argument. use -h for help.\n";
    exit(-1);
  }

  set(argv[optind++], &slaves[0]);
  warn << "BLOCKER host = " << slaves[0].name << ", control_port = " << slaves[0].control_port << "\n";

  // BLOCKEE
  if(cmd & BLOCK || cmd & UNBLOCK) {
    set(blockee, &slaves[1]);
    warn << "BLOCKEE host = " << slaves[1].name << ", control_port = " << slaves[1].control_port << "\n";
  }

  testmaster *tm = New testmaster(slaves);
  if(cmd & BLOCK || cmd & UNBLOCK)
    tm->instruct(0, cmd, wrap(&done), 1);
  else
    tm->instruct(0, cmd, wrap(&done));
  amain();
}
