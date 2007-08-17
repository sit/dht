#include <chord.h>
#include <modlogger.h>
#include <id_utils.h>
#include <db.h>
#include "merkle.h"
#include "merkle_tree_disk.h"
#include "merkle_tree_bdb.h"
#include <location.h>
#include <transport_prot.h>
#include <comm.h>

// {{{ Globals
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

u_int32_t nkeyspushed = 0;
u_int32_t nkeyspulled = 0;
vec<chordID> keys_for_server;
vec<chordID> keys_for_syncer;
// }}}

// {{{ Merkle Tree setup/teardown harness
struct harness_t {
  harness_t () {}
  virtual ~harness_t () {}
};
ptr<harness_t> harness (NULL);

struct mem_harness_t : public harness_t {
  mem_harness_t () {
    SERVER.tree = New refcounted<merkle_tree_mem> ();
    SYNCER.tree = New refcounted<merkle_tree_mem> ();
  }
  ~mem_harness_t () {
  }
};

str server_index = "/tmp/server.index.mrk";
str server_internal = "/tmp/server.internal.mrk";
str server_leaf = "/tmp/server.leaf.mrk";
str syncer_index = "/tmp/syncer.index.mrk";
str syncer_internal = "/tmp/syncer.internal.mrk";
str syncer_leaf = "/tmp/syncer.leaf.mrk";

str server_index_ro = "/tmp/server.index.mrk.ro";
str server_internal_ro = "/tmp/server.internal.mrk.ro";
str server_leaf_ro = "/tmp/server.leaf.mrk.ro";
str syncer_index_ro = "/tmp/syncer.index.mrk.ro";
str syncer_internal_ro = "/tmp/syncer.internal.mrk.ro";
str syncer_leaf_ro = "/tmp/syncer.leaf.mrk.ro";

struct disk_harness_t : public harness_t {
  disk_harness_t () {
    SERVER.tree = New refcounted<merkle_tree_disk> (server_index,
	server_internal, server_leaf, true);
    SYNCER.tree = New refcounted<merkle_tree_disk> (syncer_index,
	syncer_internal, syncer_leaf, true);
  }
  ~disk_harness_t () {
    unlink (server_index);
    unlink (server_internal);
    unlink (server_leaf);
    unlink (server_index_ro);
    unlink (server_internal_ro);
    unlink (server_leaf_ro);

    unlink (syncer_index);
    unlink (syncer_internal);
    unlink (syncer_leaf);
    unlink (syncer_index_ro);
    unlink (syncer_internal_ro);
    unlink (syncer_leaf_ro);
  }
};

struct bdb_harness_t : public harness_t {
  bdb_harness_t () {
    SERVER.tree = New refcounted<merkle_tree_bdb> ("/tmp/server.bdb", false, false);
    SYNCER.tree = New refcounted<merkle_tree_bdb> ("/tmp/syncer.bdb", false, false);
  }
  ~bdb_harness_t () {
    system ("rm -rf /tmp/server.bdb");
    system ("rm -rf /tmp/syncer.bdb");
  }
};

str mode ("bdb");
ptr<harness_t> allocate_harness ()
{
  if (mode == "bdb")
    return New refcounted<bdb_harness_t> ();
  else if (mode == "disk")
    return New refcounted<disk_harness_t> ();
  else if (mode == "mem")
    return New refcounted<mem_harness_t> ();
  else
    fatal << "Unknown mode " << mode << "\n";
  return NULL;
}

// }}}
// {{{ RPC Magic
static void
doRPCcb (xdrproc_t proc, dorpc_res *res, aclnt_cb cb, void *out, clnt_stat err)
{
  xdrmem x ((char *)res->resok->results.base (), 
	    res->resok->results.size (), XDR_DECODE);

  if (err) {
    warnx << "doRPC: err = " << err << "\n";
    assert (!err);
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
  x.uio ()->copyout (arg->args.base ());

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
// }}}
// {{{ Testing framework - setup/finish/addrand/dump_stats
static void
sendblock (bigint blockID, bool missingLocal)
{
  if (missingLocal)
    keys_for_syncer.push_back (blockID);
  else
    keys_for_server.push_back (blockID);
}

void
dump_stats ()
{
  warn  << "+++++++++++++++++++++++++SERVER.tree++++++++++++++++++++++++++++++\n";
  SERVER.tree->compute_stats ();
  // SERVER.tree->dump ();
  warn  << "+++++++++++++++++++++++++SYNCER.tree++++++++++++++++++++++++++++++\n";
  SYNCER.tree->compute_stats ();
  // SYNCER.tree->dump ();
}

void
setup ()
{
  warn << "===> setup() +++++++++++++++++++++++++++\n";

  harness = allocate_harness ();

  // these are closed by axprt_stream's dtor, right? 
  int fds[2];
  assert (socketpair (AF_UNIX, SOCK_STREAM, 0, fds) == 0);
  warn << "  sockets: " << fds[0] << ":" << fds[1] << "\n";
  SERVER.srv = asrv::alloc (axprt_stream::alloc (fds[0]), transport_program_1);
  SYNCER.clnt = aclnt::alloc (axprt_stream::alloc (fds[1]), transport_program_1);
  assert (SERVER.srv && SYNCER.clnt);
  SERVER.srv->setcb (wrap (&transport_dispatch));

  SYNCER.syncer = New refcounted<merkle_syncer> (0, DHASH_CONTENTHASH,
						 SYNCER.tree, 
						 wrap (doRPC),
						 wrap (sendblock));
  SERVER.server = New refcounted<merkle_server> (SERVER.tree);
  addHandler (merklesync_program_1,
      wrap (SERVER.server, &merkle_server::dispatch));
  err_flush ();
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

  harness = NULL;
}

void
addrand (ptr<merkle_tree> tr, int count)
{
  for (int i = 0; i < count; i++) {
    merkle_hash key;
    key.randomize ();
    tr->insert (key);
    if ((i % 1000) == 0) {
      warn << "inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}

void
removesome (ptr<merkle_tree> tr, int count, bool rand = false)
{
  static bigint idmax = (bigint (1) << 160) - 1;
  int maxtries = 2*count;
  int nremoved = 0;
  while (nremoved < count && maxtries > 0) {
    maxtries--;
    merkle_hash key (0);
    if (rand)
      key.randomize ();
    vec<chordID> keys = tr->get_keyrange (static_cast<bigint> (key), idmax, count);
    while (keys.size () && (nremoved < count)) {
      nremoved++;
      tr->remove (keys.back ());
      keys.pop_back ();
    }
  }
  warnx << "Removed " << nremoved << " keys\n";
}

void
check_invariants ()
{
  warn << "Checking server invariants... ";
  SERVER.tree->check_invariants ();
  warn << "OK\n";
  warn << "Checking syncer invariants... ";
  SYNCER.tree->check_invariants ();
  warn << "OK\n";
}

void check_equal_roots ()
{
  warn << "Checking that roots are equal... ";
  merkle_node *serv_root = SERVER.tree->get_root ();
  merkle_node *sync_root = SYNCER.tree->get_root ();
  if (serv_root->hash != sync_root->hash) {
    warn << "SERVER.tree->root " << serv_root->hash << " cnt " << serv_root->count << "\n";
    warn << "SYNCER.tree->root " << sync_root->hash << " cnt " << sync_root->count << "\n";
    fatal << "NOT OK!\n";
  }
  SERVER.tree->lookup_release (serv_root);
  SYNCER.tree->lookup_release (sync_root);
  warn << "OK\n";
}
// }}}
 
void
runsync (chordID rngmin, chordID rngmax, bool perturb = false)
{
  nkeyspushed = 0;
  nkeyspulled = 0;
  SYNCER.syncer->sync (rngmin, rngmax);
  while (!SYNCER.syncer->done ()) {
    if (perturb) {
      long int bits = random ();
      ptr<merkle_tree> t = ((bits>>1)&0x1) ? SYNCER.tree : SERVER.tree;
      if (bits & 0x1)
	addrand (t, 64);
      else
	removesome (t, 64);
    }
    acheck ();

    while (keys_for_server.size ()) {
      chordID k = keys_for_server.pop_front ();
      merkle_hash key (k);
      SERVER.tree->insert (key);
      nkeyspushed++;
    }
    while (keys_for_syncer.size ()) {
      chordID k = keys_for_syncer.pop_front ();
      merkle_hash key (k);
      SYNCER.tree->insert (key);
      nkeyspulled++;
    }
  }
}

int
main (int argc, char *argv[])
{
  if (argc > 2)
    modlogger::setmaxprio (modlogger::TRACE);
  if (argc == 2)
    mode = argv[1];

  // Make sure no old state remains on disk.
  finish ();

  bigint idzero = 0;
  bigint idmax  = (bigint (1) << 160)  - 1;

  // Empty A, Empty B, Complete range
  // ==> A should equal B.
  // ==> Resync should move no keys.
  setup ();
  runsync (idzero, idmax);
  check_invariants ();
  assert (nkeyspushed == 0);
  assert (nkeyspulled == 0);
  check_equal_roots ();
  runsync (idzero, idmax);
  check_invariants ();
  assert (nkeyspushed == 0);
  assert (nkeyspulled == 0);
  dump_stats ();
  finish ();
 
  // Empty A, Non-empty B, Complete range
  // ==> A should equal B.
  // ==> Resync should move no keys.
  setup ();
  addrand (SERVER.tree, 256);
  runsync (idzero, idmax);
  check_invariants ();
  check_equal_roots ();
  runsync (idzero, idmax);
  check_invariants ();
  assert (nkeyspushed == 0);
  assert (nkeyspulled == 0);
  dump_stats ();
  finish ();

  for (size_t c = 0; c < 10; c++) {
    setup ();
    addrand (SERVER.tree, 512);
    vec<chordID> allkeys = SERVER.tree->get_keyrange (0, idmax, 512);
    chordID a = make_randomID ();
    chordID b = make_randomID ();
    warnx << "sync " << a << " " << b << "\n";
    bhash<chordID, hashID> filtered;
    filtered.clear ();
    for (size_t i = 0; i < allkeys.size (); i++) {
      if (betweenbothincl (a, b, allkeys[i]))
	filtered.insert (allkeys[i]);
    }
    runsync (a, b);
    assert (nkeyspulled == filtered.size ());
    assert (nkeyspushed == 0);
    warnx << "Expecting " << filtered.size () << " keys\n";
    vec<chordID> y = SYNCER.tree->get_keyrange (0, idmax, 512);
    bool bad = false;
    for (size_t i = 0; i < y.size (); i++) {
      if (!filtered[y[i]]) {
	warnx << "Unexpected key: " << y[i] << "\n";
	bad = true;
      }
      filtered.remove (y[i]);
    }
    if (filtered.size ()) {
      warnx << "Missing " << filtered.size () << " keys\n";
      warnx << "(syncer pulled " << nkeyspulled << ")\n";
      bad = true;
    }
    assert (!bad);
    finish ();
  }

  for (size_t c = 0; c < 10; c++) {
    setup ();
    addrand (SERVER.tree, 512);
    addrand (SYNCER.tree, 512);
    chordID a = make_randomID ();
    chordID b = make_randomID ();
    warnx << "sync " << a << " " << b << "\n";
    bhash<chordID, hashID> filtered;
    filtered.clear ();
    vec<chordID> allkeys = SERVER.tree->get_keyrange (0, idmax, 512);
    for (size_t i = 0; i < allkeys.size (); i++) {
      if (betweenbothincl (a, b, allkeys[i]))
	filtered.insert (allkeys[i]);
    }
    unsigned int expected = filtered.size ();
    allkeys = SYNCER.tree->get_keyrange (0, idmax, 512);
    for (size_t i = 0; i < allkeys.size (); i++) {
      if (betweenbothincl (a, b, allkeys[i]))
	filtered.insert (allkeys[i]);
    }
    runsync (a, b);
    warnx << "Expecting " << expected << " keys\n";
    assert (nkeyspulled == expected);
    vec<chordID> y = SYNCER.tree->get_keyrange (0, idmax, 1024);
    bool bad = false;
    for (size_t i = 0; i < y.size (); i++) {
      if (!betweenbothincl (a, b, y[i]))
	continue;
      if (!filtered[y[i]]) {
	warnx << "Unexpected key: " << y[i] << "\n";
	bad = true;
	continue;
      }
      filtered.remove (y[i]);
    }
    if (filtered.size ()) {
      warnx << "Missing " << filtered.size () << " keys\n";
      warnx << "(syncer pulled " << nkeyspulled << ")\n";
      qhash_slot<chordID, void> *slot = filtered.first ();
      while (slot) {
	warnx << "  " << slot->key << "\n";
	slot = filtered.next (slot);
      }
      
      bad = true;
    }
    assert (!bad);
    finish ();
  }

  // Non-empty A, Empty B, Complete range
  // ==> A should equal B.
  // ==> Resync should move no keys.
  setup ();
  addrand (SYNCER.tree, 1024);
  runsync (idzero, idmax);
  check_invariants ();
  check_equal_roots ();
  runsync (idzero, idmax);
  check_invariants ();
  assert (nkeyspushed == 0);
  assert (nkeyspulled == 0);
  dump_stats ();
  finish ();

  // Non-empty A, Non-empty B, Complete range
  // ==> A should equal B.
  // ==> Resync should move no keys.
  setup ();
  addrand (SYNCER.tree, 4097);
  addrand (SERVER.tree, 4097);
  runsync (idzero, idmax);
  check_invariants ();
  check_equal_roots ();
  runsync (idzero, idmax);
  check_invariants ();
  assert (nkeyspushed == 0);
  assert (nkeyspulled == 0);

  // Now test resynchronization after writes.
  addrand (SERVER.tree, 257);
  runsync (idzero, idmax);
  check_equal_roots ();
  assert (nkeyspulled == 257);
  assert (nkeyspushed == 0);
  addrand (SYNCER.tree, 257);
  runsync (idzero, idmax);
  check_equal_roots ();
  assert (nkeyspulled == 0);
  assert (nkeyspushed == 257);

  addrand (SERVER.tree, 3);
  addrand (SYNCER.tree, 5);
  runsync (idzero, idmax);
  check_equal_roots ();
  assert (nkeyspulled == 3);
  assert (nkeyspushed == 5);

  dump_stats ();
  finish ();

  setup ();
  addrand (SYNCER.tree, 4097);
  addrand (SERVER.tree, 4097);
  runsync (idzero, idmax, true);
  runsync (idzero, idmax, false);
  check_invariants ();
  check_equal_roots ();
  finish ();

  // XXX Should we test various degrees of commonality in A/B?
  //
  // Same as above, but for a partial range.
  // The results are that A will not equal B, but resync won't
  // exchange any keys.
}

/* vim:set foldmethod=marker: */
