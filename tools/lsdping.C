#include <async.h>
#include <arpc.h>

#include <chord_types.h>
#include <chord_prot.h>
#include <fingers_prot.h>
#include <misc_utils.h>
#include <id_utils.h>

#include "rpclib.h"

static int persecond (1);
static int outstanding;
void process (int64_t starttime, int seqno, chord_node dst, 
	      chord_nodelistextres *lst, clnt_stat status);
void doit (chord_node dst, int seqno);

void
process (int64_t starttime, int seqno, chord_node dst, 
	 chord_nodelistextres *lst,
	 clnt_stat status)
{
  if (status) {
    warn << seqno << ": RPC error\n";
  } else {
    warn << seqno << ": from " << dst.r << " " << (getusec() - starttime)/1000 << "msecs \n";
  }

  delete lst;
  outstanding--;
  if (outstanding == 0)
    delaycb(1, wrap(&doit, dst, seqno + 1));
}

void doit (chord_node dst, int seqno) {
  for (int i = 0; i < persecond; i++) {
    int64_t usec = getusec ();
    ptr<chordID> ga = New refcounted<chordID> (dst.x);
    chord_nodelistextres *lst = New chord_nodelistextres ();
    doRPC (dst, chord_program_1, CHORDPROC_GETSUCC_EXT,
	   ga, lst,
	   wrap(&process, usec, seqno + i, dst, lst));
    outstanding++;
  }
}

static const char *usage = "lsdping: [-n numpersecond] host port vnodenum";

int
main (int argc, char *argv[])
{
  int ch;
  while ((ch = getopt (argc, argv, "t:n:")) != -1)
    switch (ch) {
    case 'n':
      persecond = atoi (optarg);
      break;
    case 't':
      rpclib_timeout = atoi (optarg);
      break;
    default:
      fatal << usage << "\n";
      break;
    }

  argc -= optind;
  argv += optind;
  
  if (argc < 3) 
    fatal << usage << "\n";

  chord_node dst;

  if (inet_addr (argv[0]) == INADDR_NONE) {
    // yep, this still blocks.
    struct hostent *h = gethostbyname (argv[0]);
    if (!h)
      fatal << "Invalid address or hostname: " << argv[0] << "\n";
    struct in_addr *ptr = (struct in_addr *) h->h_addr;
    dst.r.hostname = inet_ntoa (*ptr);
  } else {
    dst.r.hostname = argv[0];
  }

  dst.r.port = atoi (argv[1]);
  dst.vnode_num = atoi (argv[2]);
  dst.x = make_chordID (dst.r.hostname, dst.r.port, dst.vnode_num);

  doit (dst, 0);

  amain ();
}
