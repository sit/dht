#include "chord.h"
#include <id_utils.h>
#include "misc_utils.h"
#include "math.h"
#include "rxx.h"
#include "async.h"
#include "transport_prot.h"

#define TIMEOUT 10

chordID wellknown_ID = -1;
ptr<axprt_dgram> dgram_xprt;

bool verify = false;
vec<chord_node> sequential;

void getsucc_cb (chord_nodelistextres *res, clnt_stat err);
void doRPCcb (chordID ID, int procno, dorpc_res *res, void *out, aclnt_cb cb, 
	      clnt_stat err);
void
setup () 
{
  int dgram_fd = inetsocket (SOCK_DGRAM);
  if (dgram_fd < 0) fatal << "Failed to allocate dgram socket\n";
  dgram_xprt = axprt_dgram::alloc (dgram_fd, sizeof(sockaddr), 230000);
  if (!dgram_xprt) fatal << "Failed to allocate dgram xprt\n";
}

ptr<aclnt>
get_aclnt (str host, unsigned short port)
{
  sockaddr_in saddr;
  bzero(&saddr, sizeof(sockaddr_in));
  saddr.sin_family = AF_INET;
  inet_aton (host.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (port);

  ptr<aclnt> c = aclnt::alloc (dgram_xprt, transport_program_1, 
			       (sockaddr *)&(saddr));

  return c;
}

void
doRPC (chordID to, str host, u_short port, int procno, const void *in, void *out, aclnt_cb cb)
{
  ptr<aclnt> c = get_aclnt (host, port);
  if (c == NULL) 
    fatal << "doRPC: couldn't aclnt::alloc\n";

 //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();

  //header
  arg->dest_id = to;
  arg->src_id = bigint (0);
  arg->src_vnode_num = 0;
  arg->progno = chord_program_1.progno;
  arg->procno = procno;
  
  //marshall the args ourself
  xdrproc_t inproc = chord_program_1.tbl[procno].xdr_arg;
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), (void *)in))) {
    fatal << "failed to marshall args\n";
  } else {
    int args_len = x.uio ()->resid ();
    arg->args.setsize (args_len);
    void *marshalled_args = suio_flatten (x.uio ());
    memcpy (arg->args.base (), marshalled_args, args_len);
    free (marshalled_args);

    dorpc_res *res = New dorpc_res (DORPC_OK);

    c->timedcall (TIMEOUT, TRANSPORTPROC_DORPC, 
		  arg, res, wrap (&doRPCcb, to, procno, res, out, cb));
    
  }
}  


void
doRPCcb (chordID ID, int procno, dorpc_res *res, void *out, aclnt_cb cb, 
	 clnt_stat err)
{
  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);
  xdrproc_t proc = chord_program_1.tbl[procno].xdr_res;
  assert (proc);
  if (!proc (x.xdrp (), out)) {
    fatal << "failed to unmarshall result\n";
    cb (RPC_CANTSEND);
  } else 
    cb (err);
}

void
verify_succlist (const vec<chord_node> &zs)
{
  size_t sz = zs.size ();
  chord_node x = sequential.pop_front ();
  // ensure we talked to who we think we should be talking to.
  assert (x.x == zs[0].x);

  if (sequential.size () == 0) {
    for (size_t i = 1; i < sz; i++) {
      sequential.push_back (zs[i]);
    }
  } else {
    bool bad = false;
    vec<chord_node> newseq;
    size_t i = 1, j = 0;
    while (i < sz && j < sequential.size ()) {
      if (sequential[j].x == zs[i].x) {
	newseq.push_back (sequential[j]);
	j++; i++;
      } else {
	bad = true;
	strbuf s;
	s << "  sequential[" << j << "] = " << sequential[j] << "\n";
	s << "  nlist[" << i << "] = " << zs[i] << "\n";
	if (sequential[j].x < zs[i].x) {
	  warnx << "nlist missing a successor!\n";
	  newseq.push_back (sequential[j]);
	  j++;
	} else {
	  warnx << "sequential missing a successor!\n";
	  newseq.push_back (zs[i]);
	  i++;
	}
	warnx << s;
      }
    }
    while (i < sz) {
      newseq.push_back (zs[i++]);
    }
    if (j < sequential.size ()) {
      bad = true;
      newseq.push_back (sequential[j++]);
    }
    if (bad) {
      for (size_t k = 0; k < zs.size (); k++)
	warnx << "nlist[" << k << "]: " << zs[k] << "\n";
      for (size_t k = 0; k < sequential.size (); k++)
	warnx << "sequential[" << k << "]: " << sequential[k] << "\n";
      warnx << "\n";
    }
    sequential.clear ();
    sequential = newseq;
  }
}

void
getsucc (chordID n, str host, u_short port)
{
  chord_nodelistextres *res = New chord_nodelistextres ();
  doRPC (n, host, port, CHORDPROC_GETSUCC_EXT, &n, res,
	 wrap (&getsucc_cb, res));
}

void
getsucc_cb (chord_nodelistextres *res, clnt_stat err)
{
  assert (err == 0 && res->status == CHORD_OK);
  assert (res->resok->nlist.size () >= 2);

  size_t sz = res->resok->nlist.size ();
  vec<chord_node> zs;
  for (size_t i = 0; i < sz; i++) {
    chord_node z = make_chord_node (res->resok->nlist[i].n);
    zs.push_back (z);
  }
  delete res;

  chord_node next;
  if (verify) {
    verify_succlist (zs);
    next = sequential[0];
  } else {
    next = zs[1];
  }

  // Print the "next" node we are going to contact.
  chordID n    = next.x;
  str host     = next.r.hostname;
  u_short port = next.r.port;
  int index    = next.vnode_num;
  assert (index >= 0);
  warnx << n << " " << host << " " << port << " " << index << "\n";

  // wrapped around ring. done.
  if (next.x == wellknown_ID)
    exit (0);
  
  if (next.x != zs[1].x)
    warnx << "XXX succlist had wrong successor!!!\n";
  getsucc (next.x, next.r.hostname, next.r.port);
}

void 
usage ()
{
  fatal << "walk [-v] -j <host>:<port>\n";
}

int
main (int argc, char** argv) 
{
  setprogname (argv[0]);
  random_init ();
  setup ();

  str host = "not set";
  unsigned short port = 0;

  int ch;
  while ((ch = getopt (argc, argv, "j:v")) != -1) {
    switch (ch) {
    case 'j': 
      {
	char *bs_port = strchr(optarg, ':');
	if (!bs_port) usage ();
	*bs_port = 0;
	bs_port++;
	if (inet_addr (optarg) == INADDR_NONE) {
	  //yep, this blocks
	  struct hostent *h = gethostbyname (optarg);
	  if (!h) {
	    warn << "Invalid address or hostname: " << optarg << "\n";
	    usage ();
	  }
	  struct in_addr *ptr = (struct in_addr *)h->h_addr;
	  host = inet_ntoa (*ptr);
	} else
	  host = optarg;

	port = atoi (bs_port);

	break;
      }
    case 'v':
      verify = true;
      break;
    default:
      usage ();
      break;
    }
  }

  if (host == "not set")
    usage ();
    
  wellknown_ID = make_chordID (host, port, 0);
  chord_node wellknown_node;
  wellknown_node.x = wellknown_ID;
  wellknown_node.r.hostname = host;
  wellknown_node.r.port = port;
  wellknown_node.vnode_num = 0;
  sequential.push_back (wellknown_node);
  getsucc (wellknown_ID, host, port);


  amain ();
}
