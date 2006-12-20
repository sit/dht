#include <chord.h>
#include <modlogger.h>
#include <id_utils.h>
#include "merkle.h"
#include "merkle_tree_disk.h"
#include <location.h>
#include <transport_prot.h>
#include <comm.h>

static struct {
  ptr<merkle_tree> tree;
  ptr<merkle_server> server;
  ptr<asrv> srv;
} SERVER;

static struct {
  ptr<merkle_tree> tree;
  ptr<merkle_syncer> syncer;
  ptr<aclnt> clnt;
} SYNCER;

vec<chordID> keys_for_server;
vec<chordID> keys_for_syncer;

str server_index = "/tmp/server.index.mrk";
str server_internal = "/tmp/server.internal.mrk";
str server_leaf = "/tmp/server.leaf.mrk";
str syncer_index = "/tmp/syncer.index.mrk";
str syncer_internal = "/tmp/syncer.internal.mrk";
str syncer_leaf = "/tmp/syncer.leaf.mrk";

static void
sendblock (bigint blockID, bool missingLocal)
{
  if (missingLocal)
    keys_for_syncer.push_back (blockID);
  else
    keys_for_server.push_back (blockID);
}

static void
doRPCcb (xdrproc_t proc, dorpc_res *res, aclnt_cb cb, void *out, clnt_stat err)
{
  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);

  if (err) {
    warnx << "doRPC: err = " << err << "\n";
  } else if (!proc (x.xdrp (), out)) {
    warnx << "failed to unmarshall result\n";
    cb (RPC_CANTSEND);
    return;
  }
  cb (err);
  delete res;
}

// called by syncer to perform merkle RPC to server
static void
doRPC (RPC_delay_args *a)
{
  //form the transport RPC
  ptr<dorpc_arg> arg = New refcounted<dorpc_arg> ();
  // Other fields don't matter.
  arg->progno = a->prog.progno;
  arg->procno = a->procno;

  xdrproc_t inproc = a->prog.tbl[a->procno].xdr_arg;
  xdrproc_t outproc = a->prog.tbl[a->procno].xdr_res;
  assert (outproc);
  xdrsuio x (XDR_ENCODE);
  if ((!inproc) || (!inproc (x.xdrp (), (void *)a->in))) {
    fatal << "failed to marshall args\n";
  } 
  int args_len = x.uio ()->resid ();
  arg->args.setsize (args_len);
  void *marshalled_args = suio_flatten (x.uio ());
  memcpy (arg->args.base (), marshalled_args, args_len);
  free (marshalled_args);

  dorpc_res *res = New dorpc_res (DORPC_OK);
  SYNCER.clnt->call (TRANSPORTPROC_DORPC, arg, res,
                     wrap (&doRPCcb, outproc, res, a->cb, a->out));
}


vec<const rpc_program *> handledProgs;
vec<cbdispatch_t> handlers;

static void 
transport_dispatch (svccb *sbp)
{
  if (!sbp) {
    warnx << "transport server eof\n";
    return;
  }

  dorpc_arg *arg = sbp->Xtmpl getarg<dorpc_arg> ();

  chord_node_wire nw;
  bzero (&nw, sizeof (chord_node_wire));

  const rpc_program *prog (NULL);
  unsigned int i (0);
  for (unsigned int i = 0; i < handledProgs.size (); i++)
    if (arg->progno == (int)handledProgs[i]->progno) {
      prog = handledProgs[i];
      break;
    }
  char *arg_base = (char *)(arg->args.base ());
  int arg_len = arg->args.size ();
  
  xdrmem x (arg_base, arg_len, XDR_DECODE);
  xdrproc_t proc = prog->tbl[arg->procno].xdr_arg;
  assert (proc);
  
  void *unmarshalled_args = prog->tbl[arg->procno].alloc_arg ();
  if (!proc (x.xdrp (), unmarshalled_args)) {
    warn << "dispatch: error unmarshalling arguments: "
	 << arg->progno << "." << arg->procno << "\n";
    xdr_delete (prog->tbl[arg->procno].xdr_arg, unmarshalled_args);
    sbp->replyref (rpcstat (DORPC_MARSHALLERR));
    return;
  }
  user_args *ua = New user_args (sbp, unmarshalled_args,
				 prog, arg->procno, 0);
  ua->me_ = New refcounted<location> (make_chord_node (nw));
  handlers[i] (ua);
}

// called by server to register handler fielding merkle RPCs
static void
addHandler (const rpc_program &prog, cbdispatch_t cb)
{
  handledProgs.push_back (&prog);
  handlers.push_back (cb);
}

void
finish ()
{
  // Force all destructors to be called, hopefully.
  handledProgs.clear ();
  handlers.clear ();

  SYNCER.syncer = NULL;
  SERVER.server = NULL;
  SYNCER.clnt = NULL;
  SERVER.srv  = NULL;
  SYNCER.tree = NULL;
  SERVER.tree = NULL;

  unlink (server_index);
  unlink (server_internal);
  unlink (server_leaf);

  unlink (syncer_index);
  unlink (syncer_internal);
  unlink (syncer_leaf);

}

void
setup ()
{
  warn << " => setup() +++++++++++++++++++++++++++\n";
  err_flush ();

  SERVER.tree = New refcounted<merkle_tree_disk> (server_index, 
						  server_internal, server_leaf,
						  true);
  SYNCER.tree = New refcounted<merkle_tree_disk> (syncer_index, 
						  syncer_internal, syncer_leaf,
						  true);

  // these are closed by axprt_stream's dtor, right? 
  int fds[2];
  assert (socketpair (AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  warn << "sockets: " << fds[0] << ":" << fds[1] << "\n";
  SERVER.srv = asrv::alloc (axprt_stream::alloc (fds[0]), transport_program_1);
  SYNCER.clnt = aclnt::alloc (axprt_stream::alloc (fds[1]), transport_program_1);
  assert (SERVER.srv && SYNCER.clnt);
  SERVER.srv->setcb (wrap (&transport_dispatch));

  SYNCER.syncer = New refcounted<merkle_syncer> (0, DHASH_CONTENTHASH,
						 SYNCER.tree, 
						 wrap (doRPC),
						 wrap (sendblock));
  SERVER.server = New refcounted<merkle_server> (SERVER.tree);
  addHandler(merklesync_program_1,
      wrap(SERVER.server, &merkle_server::dispatch));
  warn << " <= setup() DONE!\n";
  err_flush ();
}


void
addrand (ptr<merkle_tree> tr, int count)
{
  for (int i = 0; i < count; i++) {
    if (i == 40000)
      exit (0);

    merkle_hash key;
    key.randomize ();
    tr->insert (key);

    if ((i % 1000) == 0) {
      warn << "a) inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}

void
dump_stats ()
{
  warn << "\n\n=======================================================================\n";
  merkle_node *serv_root = SERVER.tree->get_root();
  merkle_node *sync_root = SYNCER.tree->get_root();
  warn << "SERVER.tree->root " << serv_root->hash << " cnt " << serv_root->count << "\n";
  warn << "SYNCER.tree->root " << sync_root->hash << " cnt " << sync_root->count << "\n";
  SERVER.tree->lookup_release(serv_root);
  SYNCER.tree->lookup_release(sync_root);

  warn  << "+++++++++++++++++++++++++SERVER.tree++++++++++++++++++++++++++++++\n";
  SERVER.tree->compute_stats ();
  SERVER.tree->check_invariants ();
  // SERVER.tree->dump ();
  warn  << "+++++++++++++++++++++++++SYNCER.tree++++++++++++++++++++++++++++++\n";
  SYNCER.tree->compute_stats ();
  SYNCER.tree->check_invariants ();
  // SYNCER.tree->dump ();
}
 
void
test (uint progress, uint data_points)
{
  u_int64_t large_sz = (1 << 30) / (1 << 13);  // 1 GB
  large_sz /= 100;
  u_int64_t small_sz = (large_sz * progress) / (data_points - 1);

  warn << "\n\n\n\n############################################################\n";
  warn << "REPLICA TEST " << large_sz << "/" << small_sz << "\n";
  setup ();
  if (progress % 2) {
    addrand (SERVER.tree, large_sz);
    addrand (SYNCER.tree, small_sz);
  } else {
    addrand (SERVER.tree, small_sz);
    addrand (SYNCER.tree, large_sz);
  }

  warn  << "+++++++++++++++++++++++++server tree++++++++++++++++++++++++++++++\n";
  SERVER.tree->compute_stats ();
  warn  << "+++++++++++++++++++++++++syncer tree++++++++++++++++++++++++++++++\n";
  SYNCER.tree->compute_stats ();
  err_flush();

  warn << "\n\n************************* RUNNING TEST ************************\n";
  bigint rngmin = 0;
  bigint rngmax = (bigint (1) << 160)  - 1;
  SYNCER.syncer->sync (rngmin, rngmax);

  while (!SYNCER.syncer->done ()) {
    acheck ();

    while (keys_for_server.size ()) {
      chordID k = keys_for_server.pop_front ();
      merkle_hash key = to_merkle_hash (k);
      SERVER.tree->insert (key);
    }
    while (keys_for_syncer.size ()) {
      chordID k = keys_for_syncer.pop_front ();
      merkle_hash key = to_merkle_hash (k);
      SYNCER.tree->insert (key);
    }
  }

  warn << "\n\n*********************** DONE *****************************\n";

  dump_stats ();
  finish ();
}



int
main (int argc, char *argv[])
{
  if (argc > 1)
    modlogger::setmaxprio (modlogger::TRACE);
  u_int start_point = 0;
  u_int data_points = 100;
  for (u_int i = start_point; i < data_points; i++) 
    test (i, data_points);
}
