#include <arpc.h>
#include <chord_types.h>
#include <chord_prot.h>
#include <fingers_prot.h>
#include <misc_utils.h>
#include <id_utils.h>
#include "rpclib.h"
#include <async.h>
#include <dns.h>

int outstanding = 0;
bool do_reverse_lookup = false;


void
dns_lookup_cb (int i, chord_node z, chord_node_ext y,
	       ptr<hostent> he, int err) {

  if (!err) z.r.hostname = he->h_name;

  warnx << i << ".\t"
	<< z.x << " "
	<< z.r.hostname << " "
	<< z.r.port << " "
	<< z.vnode_num << " "
	<< y.a_lat << " "
	  << y.a_var << " "
	<< y.nrpc
	<< "\n";
  if (do_reverse_lookup) {
    outstanding--;
    if (outstanding == 0)
      exit (0);
  }
}

void
printreslist_cb (str desc, chord_nodelistextres *res, clnt_stat status)
{
  if (status) {
    warnx << "no " << desc << ": " << status << "\n";
    return;
  }

  warnx << "=== " << desc << "\n";
  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    struct in_addr ar;
    chord_node_ext y = res->resok->nlist[i];
    chord_node z = make_chord_node (y.n);
    inet_aton (z.r.hostname, &ar);
    if (do_reverse_lookup) {
      outstanding++;
      dns_hostbyaddr (ar, wrap (&dns_lookup_cb, i, z, y));
    } else 
      dns_lookup_cb (i, z, y, NULL, 1);
  }
  delete res;
  outstanding--;
  if (outstanding == 0)
    exit (0);
}




void
print_successors (const chord_node &dst)
{
  ptr<chordID> ga = New refcounted<chordID> (dst.x);
  chord_nodelistextres *lst = New chord_nodelistextres ();
  doRPC (dst, chord_program_1, CHORDPROC_GETSUCC_EXT,
	 ga, lst,
	 wrap (printreslist_cb, "successors", lst));
  outstanding++;
}

void
print_fingers (const chord_node &dst)
{
  ptr<chordID> ga = New refcounted<chordID> (dst.x);
  chord_nodelistextres *lst = New chord_nodelistextres ();
  doRPC (dst, fingers_program_1, FINGERSPROC_GETFINGERS_EXT,
	 ga, lst,
	 wrap (printreslist_cb, "fingers", lst));
  outstanding++;
}

int
main (int argc, char *argv[])
{
  chord_node dst;
  if (argc != 4) {
    if (strcmp(argv[4], "-r") == 0) do_reverse_lookup = true;
    else
      fatal << "Usage: nodeq host port vnode\n";
  }
  if (inet_addr (argv[1]) == INADDR_NONE) {
    // yep, this still blocks.
    struct hostent *h = gethostbyname (argv[1]);
    if (!h)
      fatal << "Invalid address or hostname: " << argv[1] << "\n";
    struct in_addr *ptr = (struct in_addr *) h->h_addr;
    dst.r.hostname = inet_ntoa (*ptr);
  } else {
    dst.r.hostname = argv[1];
  }
  
  dst.r.port = atoi (argv[2]);
  dst.vnode_num = atoi (argv[3]);
  dst.x = make_chordID (dst.r.hostname, dst.r.port, dst.vnode_num);

  outstanding++;
  print_successors (dst);
  while (outstanding > 1) acheck ();
  outstanding = 0;
  print_fingers (dst);

  amain ();
}
  

