#include "merkle.h"

struct {
  ptr<dbfe> db;
  ptr<merkle_tree> tree;
  ptr<merkle_server> server;
  ptr<asrv> srv;
} SERVER;

struct {
  ptr<dbfe> db;
  ptr<merkle_tree> tree;
  ptr<merkle_syncer> syncer;
  ptr<aclnt> clnt;
} SYNCER;

#define SERVER_DBNAME "db1"
#define SYNCER_DBNAME "db2"

ptr<dbrec> FAKE_DATA = New refcounted<dbrec> ("FAKE", strlen ("FAKE"));


// XXX: PUT THIS FUNCTION IN THE MERKLE DIRECTORY
static ptr<dbfe>
create_database (char *dbname)
{
  ptr<dbfe> db = New refcounted<dbfe> ();

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 1000);
  opts.addOption("opt_nodesize", 4096);

  // XXX: DONT UNLINK: READ THE DB AND POPULATE THE MERKLE TREE 
  unlink (dbname);
  if (int err = db->opendb(dbname, opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
  return db;
}


vec<XXX_SENDBLOCK_ARGS> keys_for_server;
vec<XXX_SENDBLOCK_ARGS> keys_for_syncer;

// called by syncer to send block to server
static void
sendblock (bigint blockID, bool last, callback<void>::ref cb)
{
  XXX_SENDBLOCK_ARGS args (0, blockID, last, cb);
  keys_for_server.push_back (args);
}

// called by server to send block to syncer
static void
sendblock2 (XXX_SENDBLOCK_ARGS *a)
{
  keys_for_syncer.push_back (*a);
}

// called by syncer to perform merkle RPC to server
static void
doRPC (RPC_delay_args *a)
{
  SYNCER.clnt->call (a->procno, a->in, a->out, a->cb);
}

// called by server to register handler fielding merkle RPCs
static void
addHandler (const rpc_program &prog, cbdispatch_t cb)
{
  SERVER.srv->setcb (cb);
}


void
setup ()
{
  warn << " => setup() +++++++++++++++++++++++++++\n";
  err_flush ();

  SERVER.db = create_database (SERVER_DBNAME);
  SYNCER.db = create_database (SYNCER_DBNAME);
  SERVER.tree = New refcounted<merkle_tree> (SERVER.db);
  SYNCER.tree = New refcounted<merkle_tree> (SYNCER.db);

  // these are closed by axprt_stream's dtor, right? 
  int fds[2];
  assert (pipe (fds) == 0);
  warn << "PIPES: " << fds[0] << ":" << fds[1] << "\n";
  SERVER.srv = asrv::alloc (axprt_stream::alloc (fds[0]), merklesync_program_1);
  SYNCER.clnt = aclnt::alloc (axprt_stream::alloc (fds[1]), merklesync_program_1);
  assert (SERVER.srv && SYNCER.clnt);

  SYNCER.syncer = New refcounted<merkle_syncer> (SYNCER.tree, 
						 wrap (doRPC),
						 wrap (sendblock));
  SERVER.server = New refcounted<merkle_server> (SERVER.tree, 
						 wrap (addHandler),
						 wrap (sendblock2));
  warn << " <= setup() DONE!\n";
  err_flush ();
}


void
addrand (ptr<merkle_tree> tr, int count)
{
  for (int i = 0; i < count; i++) {
    err_flush ();

    if (i == 40000)
      exit (0);

    merkle_hash key;
    key.randomize ();
    block *b = New block (key, FAKE_DATA);
    assert (!database_lookup (SERVER.db, b->key));
    assert (!database_lookup (SYNCER.db, b->key));
    tr->insert (b);

    if ((i % 1000) == 0) {
      warn << "a) inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}


void
addrand (ptr<merkle_tree> tr1, ptr<merkle_tree> tr2, int count)
{
  for (int i = 0; i < count; i++) {
    warn << "i=" << i << "\n";
    err_flush ();

    if (i == 40000)
      exit (0);


    merkle_hash key;
    key.randomize();
    block *b = New block (key, FAKE_DATA);
    assert (!database_lookup (SERVER.db, key));
    assert (!database_lookup (SYNCER.db, key));
    tr1->insert (b);
    tr2->insert (New block (key, FAKE_DATA));

    if ((i % 1000) == 0) {
      warn << "b) inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}

void
addinc (ptr<merkle_tree> tr, int count)
{

  for (int i = 0; i < count; i++) 
    tr->insert (New block (merkle_hash(i), FAKE_DATA));
}

void
addinc (ptr<merkle_tree> tr1, ptr<merkle_tree> tr2, int count)
{
  for (int i = 0; i < count; i++) {
    tr1->insert (New block (merkle_hash(i), FAKE_DATA));
    tr2->insert (New block (merkle_hash(i), FAKE_DATA));
  }
}


void
dump_stats (merkle_syncer::mode_t m)
{
  warn << "\n\n=======================================================================\n";
  warn << "SERVER.tree->root " << SERVER.tree->root.hash << " cnt " << SERVER.tree->root.count << "\n";
  warn << "SYNCER.tree->root " << SYNCER.tree->root.hash << " cnt " << SYNCER.tree->root.count << "\n";

  warn  << "+++++++++++++++++++++++++SERVER.tree++++++++++++++++++++++++++++++\n";
  SERVER.tree->compute_stats ();
  warn  << "+++++++++++++++++++++++++SYNCER.tree++++++++++++++++++++++++++++++\n";
  SYNCER.tree->compute_stats ();

  //warn  << "++++++++++++++++++++++++++SERVER.db+++++++++++++++++++++++++++++++\n";
  //SERVER.tree->db->dump_stats ();
  //warn  << "++++++++++++++++++++++++++SYNCER.db+++++++++++++++++++++++++++++++\n";
  //SYNCER.tree->db->dump_stats ();
  //warn  << "++++++++++++++++++++++++client1+++++++++++++++++++++++++++++\n";
  //host1->clnt->dump_stats ();
  //warn  << "++++++++++++++++++++++++client2+++++++++++++++++++++++++++++\n";
  //host2->clnt->dump_stats ();

  if (m == merkle_syncer::BIDIRECTIONAL)
    assert (SERVER.tree->root.hash == SYNCER.tree->root.hash);

}


 
void
test (merkle_syncer::mode_t m, uint progress, uint data_points)
{
  uint64 large_sz = (1 << 30) / (1 << 13);  // 1 GB
  large_sz /= 100;
  uint64 small_sz = (large_sz * progress) / (data_points - 1);


  warn << "\n\n\n\n############################################################\n";
  warn << "REPLICA TEST " << large_sz << "/" << small_sz << "\n";
  setup ();
  // SERVER.tree -- holds large_sz blocks
  // SYNCER.tree -- holds small_sz blocks

  if (m == merkle_syncer::BIDIRECTIONAL) {
    addrand (SERVER.tree, SYNCER.tree, small_sz);
    addrand (SERVER.tree, large_sz - small_sz);
  } else {
    addrand (SERVER.tree, large_sz);
    addrand (SYNCER.tree, small_sz);
  }

  warn  << "+++++++++++++++++++++++++server tree++++++++++++++++++++++++++++++\n";
  SERVER.tree->compute_stats ();
  warn  << "+++++++++++++++++++++++++syncer tree++++++++++++++++++++++++++++++\n";
  SYNCER.tree->compute_stats ();
  err_flush();

  warn << "\n\n ************************* RUNNING TEST ************************\n";
  bigint rngmin  = 0;
  bigint rngmax = (bigint (1) << 160)  - 1;
  SYNCER.syncer->sync (rngmin, rngmax, m);

  while (!SYNCER.syncer->done ()) {
    acheck ();

    while (keys_for_server.size ()) {
      XXX_SENDBLOCK_ARGS args = keys_for_server.pop_front ();
      merkle_hash key = to_merkle_hash(dhash::id2dbrec(args.blockID));
      block b (key, FAKE_DATA);
      SERVER.tree->insert (&b);
      (*args.cb) ();
    }

    while (keys_for_syncer.size ()) {
      XXX_SENDBLOCK_ARGS args = keys_for_syncer.pop_front ();
      SYNCER.syncer->recvblk (args.blockID, args.last);
      (*args.cb) ();
    }
  }

  warn << "\n\n *********************** DONE *****************************\n";

  dump_stats (m);
}



int
main ()
{
  merkle_syncer::mode_t m = merkle_syncer::UNIDIRECTIONAL;
  u_int start_point = 0;
  u_int data_points = 100;
  for (u_int i = start_point; i < data_points; i++) 
    test (m, i, data_points);
}
