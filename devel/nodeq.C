#include <async.h>
#include <dns.h>
#include <arpc.h>
#include <aios.h>

#include <chord_types.h>
#include <chord_prot.h>
#include <accordion_prot.h>
#include <fingers_prot.h>
#include <merkle_sync_prot.h>
#include <misc_utils.h>
#include <id_utils.h>
#include <merkle_misc.h>

#include "rpclib.h"

char *usage = "Usage: nodeq [-l] [-r] host port vnode\n";

int outstanding = 0;
bool do_reverse_lookup = false;
bool do_listkeys = false;
bool do_accordion = false;

struct node {
  int i;
  chord_node_ext y;
  str hostname;

  node (int i_, chord_node_ext n) : i (i_), y (n), hostname ("") {}
};

vec<ptr<node> > successors;
vec<ptr<node> > predecessors;
vec<ptr<node> > fingers;

void
printlist (str desc, vec<ptr<node> > &lst)
{
  if (lst.size () == 0)
    return;

  aout << "=== " << desc << "\n";
  for (size_t i = 0; i < lst.size (); i++) {
    chord_node z = make_chord_node (lst[i]->y.n);
    str h;
    if (lst[i]->hostname.len () > 0)
      h = lst[i]->hostname;
    else
      h = z.r.hostname;
    aout << i << ".\t"
	 << z.x << " "
	 << h << " "
	 << z.r.port << " "
	 << z.vnode_num << " "
	 << z.knownup << " "
	 << z.age << " "
	 << z. budget << " "
	 << lst[i]->y.a_lat << " "
	 << lst[i]->y.a_var << " "
	 << lst[i]->y.nrpc << " "
	 << z.coords[0] << " "
	 << z.coords[1] << " "
	 << z.coords[2] << " "
	 << z.e
	 << "\n";
  }
}

void
finish ()
{
  printlist ("predecessors", predecessors);
  printlist ("successors", successors);
  printlist ("fingers", fingers);
  exit (0);
}

void
dns_lookup_cb (ptr<node> nn, ptr<hostent> he, int err)
{
  if (!err)
    nn->hostname = he->h_name;
  
  outstanding--;
  if (outstanding == 0)
    finish ();
}

void
processreslist_cb (str desc, vec<ptr<node> > *list,
		   chord_nodelistextres *res, clnt_stat status)
{
  outstanding--;
  if (status) {
    warnx << "no " << desc << ": " << status << "\n";
    if (outstanding == 0)
      finish ();
    return;
  }

  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    ptr<node> nn = New refcounted<node> (i, res->resok->nlist[i]);
    list->push_back (nn);
    
    if (do_reverse_lookup) {
      outstanding++;
      struct in_addr ar;
      ar.s_addr = htonl (res->resok->nlist[i].n.machine_order_ipv4_addr);
      dns_hostbyaddr (ar, wrap (&dns_lookup_cb, nn));
    }
  }
  delete res;
  
  if (outstanding == 0)
    finish ();
}

void
print_successors (const chord_node &dst)
{
  ptr<chordID> ga = New refcounted<chordID> (dst.x);
  chord_nodelistextres *lst = New chord_nodelistextres ();
  doRPC (dst, chord_program_1, CHORDPROC_GETSUCC_EXT,
	 ga, lst,
	 wrap (processreslist_cb, "successors", &successors, lst));
  outstanding++;
}

void
print_fingers (const chord_node &dst)
{
  ptr<chordID> ga = New refcounted<chordID> (dst.x);
  chord_nodelistextres *lst = New chord_nodelistextres ();
  if (do_accordion) 
    doRPC (dst, accordion_program_1, ACCORDIONPROC_GETFINGERS_EXT,
	 ga, lst,
	 wrap (processreslist_cb, "fingers", &fingers, lst));
  else 
    doRPC (dst, fingers_program_1, FINGERSPROC_GETFINGERS_EXT,
	 ga, lst,
	 wrap (processreslist_cb, "fingers", &fingers, lst));
  outstanding++;
}

void
print_predecessors (const chord_node &dst)
{
  ptr<chordID> ga = New refcounted<chordID> (dst.x);
  chord_nodelistextres *lst = New chord_nodelistextres ();
  doRPC (dst, chord_program_1, CHORDPROC_GETPRED_EXT,
	 ga, lst,
	 wrap (processreslist_cb, "predecessors", &predecessors, lst));
  outstanding++;
}

void
getkeys_cb (const chord_node dst, ref<getkeys_arg> arg, ref<getkeys_res> res,
	    clnt_stat err)
{
  outstanding--;
  if (err) {
    warnx << "no keys: " << err << "\n";
    if (outstanding == 0)
      finish ();
    return;
  } else if (res->status != MERKLE_OK) {
    warnx << "protocol error " << res->status << "\n";
    if (outstanding == 0)
      finish ();
    return;
  }

  for (u_int i = 0; i < res->resok->keys.size (); i++) {
    const chordID &key2 = res->resok->keys[i];
    aout << key2 << "\n";
    arg->rngmin = incID (key2);
  }
  
  if (res->resok->morekeys) {
    doRPC (dst, merklesync_program_1, MERKLESYNC_GETKEYS,
	   arg, res,
	   wrap (getkeys_cb, dst, arg, res));
    outstanding++;
  } else {
    finish ();
  }
}

void
print_keys (const chord_node &dst)
{
  ref<getkeys_arg> arg = New refcounted<getkeys_arg> ();
  arg->ctype  = DHASH_CONTENTHASH;
  arg->rngmin = 0;
  arg->rngmax = decID (arg->rngmin);
  ref<getkeys_res> res = New refcounted<getkeys_res> ();
  doRPC (dst, merklesync_program_1, MERKLESYNC_GETKEYS,
	 arg, res,
	 wrap (getkeys_cb, dst, arg, res));
  outstanding++;
}

int
main (int argc, char *argv[])
{
  int ch;
  while ((ch = getopt (argc, argv, "rlm")) != -1)
    switch (ch) {
    case 'r':
      do_reverse_lookup = true;
      break;
    case 'l':
      do_listkeys = true;
      break;
    case 'm':
      do_accordion = true;
      break;
    default:
      fatal << usage;
      break;
    }

  argc -= optind;
  argv += optind;
  
  if (argc != 3)
    fatal << usage;

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

  if (do_listkeys) {
    print_keys (dst);
  } else {
    print_predecessors (dst);
    print_successors (dst);
    print_fingers (dst);
  }
  amain ();
}
  

