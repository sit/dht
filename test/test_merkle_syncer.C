#include "merkle.h"

uint global_count_zeroes_in_getnode_reply  = 0;
uint global_calls  = 0;
uint global_replies  = 0;

dbfe *db1 = NULL;
dbfe *db2 = NULL;
merkle_tree *tree1 = NULL;
merkle_tree *tree2 = NULL;
merkle_syncer *host1 = NULL;
merkle_syncer *host2 = NULL;

#define DBNAME1 "tmpdb1"
#define DBNAME2 "tmpdb2"



static int
dbcompare (ref<dbrec> a, ref<dbrec> b)
{
  merkle_hash ax = to_merkle_hash (a);
  merkle_hash bx = to_merkle_hash (b);
  if (ax < bx) {
    //warn << "dbcompare " << ax << " < " << bx << "\n";
    return -1;
  } else if (ax == bx) {
    //warn << "dbcompare " << ax << " == " << bx << "\n";
    return 0;
  } else {
    //warn << "dbcompare " << ax << " > " << bx << "\n";
    return 1;
  }
}


static dbfe *
create_database (char *dbname)
{
  dbfe *db = New dbfe ();
  db->set_compare_fcn (wrap (&dbcompare));

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  unlink (dbname);
  if (int err = db->opendb(dbname, opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
  return db;
}


void
setup ()
{
  warn << " => setup() +++++++++++++++++++++++++++\n";
  err_flush ();

  bool bye = false;
  if (db1 != NULL) {
    warn << "global_calls: " << global_calls << "\n";
    warn << "global_replies: " << global_replies << "\n";
    err_flush ();
    bye = true;
  }

  delete db1;
  delete db2;

#if 0
  if (tree1)
    tree1->dump ();
  warn << " => past tree1->dump()\n";
  err_flush ();
#endif

  delete tree1;

#if 0
  warn << " => past delete tree1()\n";
  err_flush ();
#endif

  delete tree2;
  delete host1;
  delete host2;

  if (bye) {
    //warn << " ************ PLANNED EXIT\n";
    //exit (0);
  }


  db1 = create_database (DBNAME1);
  db2 = create_database (DBNAME2);
  tree1 = New merkle_tree (db1);
  tree2 = New merkle_tree (db2);

  // these are closed by axprt_stream's dtor, right? 
  int fds[2];
  assert (pipe (fds) == 0);

  warn << "PIPES: " << fds[0] << ":" << fds[1] << "\n";

  host1 = New merkle_syncer (tree1, fds[0]);
  host2 = New merkle_syncer (tree2, fds[1]);
  warn << " <= setup() DONE!\n";
  err_flush ();
}


void
addrand (merkle_tree *tr, int count)
{
  for (int i = 0; i < count; i++) {
    err_flush ();

    if (i == 40000)
      exit (0);

    block *b = New block ();
    assert (!database_lookup (db1, b->key));
    assert (!database_lookup (db2, b->key));
    tr->insert (b);

    if ((i % 1000) == 0) {
      warn << "a) inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}


void
addrand (merkle_tree *tr1, merkle_tree *tr2, int count)
{
  for (int i = 0; i < count; i++) {
    warn << "i=" << i << "\n";
    err_flush ();

    if (i == 40000)
      exit (0);


    block *b = New block ();
    assert (!database_lookup (db1, b->key));
    assert (!database_lookup (db2, b->key));
    tr1->insert (b);
    tr2->insert (New block (b->key));

    if ((i % 1000) == 0) {
      warn << "b) inserted " << i << " blocks..of " << count << "\n";
      err_flush ();
    }
  }
}

void
addinc (merkle_tree *tr, int count)
{
  for (int i = 0; i < count; i++)
    tr->insert (New block (i));
}

void
addinc (merkle_tree *tr1, merkle_tree *tr2, int count)
{
  for (int i = 0; i < count; i++) {
    tr1->insert (New block (i));
    tr2->insert (New block (i));
  }
}



void
setup_trees1 ()
{
  for (int i = 1; i < 4; i++)
    tree1->insert (New block (i));
  tree1->dump();

  for (int i = 4; i < 10; i++)
    tree2->insert (New block (i));
  tree2->dump();
}


void
setup_trees2 ()
{
  for (int i = 1; i < 40; i++)
    tree1->insert (New block (i));
  tree1->dump();

  for (int i = 1; i < 4; i++)
    tree2->insert (New block (i));
  tree2->dump();
}



void
setup_trees3 ()
{
  for (int i = 1; i < 40; i++)
    tree1->insert (New block (i));
  tree1->dump();

  for (int i = 40; i < 44; i++)
    tree2->insert (New block (i));
  tree2->dump();
}


void
setup_trees4 ()
{
  for (int i = 1; i < 66; i++)
    tree1->insert (New block (i));
  tree1->dump();

  tree2->dump();
}


void
setup_trees5 ()
{
  tree1->dump();

  for (int i = 1; i < 66; i++)
    tree2->insert (New block (i));

  tree2->dump();
}



void
setup_trees9 ()
{
  for (int i = 1; i <= 6000; i++) {
    tree1->insert (New block ());
    tree2->insert (New block ());
  }

  for (int i = 1; i <= 4000; i++) {
    block *b = New block ();
    tree1->insert (b);
    tree2->insert (New block (b->key));
  }

}


void
scen1 ()
{
  for (int i = 1; i <= 100000; i++) {
    if (i % 10000 == 0)
      warn << " i = " << i << "\n";
    tree1->insert (New block ());
    tree2->insert (New block ());
  }
}


void
dump_stats (cbv::ref nextfunc)
{
  warn << "\n\n=======================================================================\n";
  warn << "ZEROES IN GETNODE REPLY " << global_count_zeroes_in_getnode_reply << "\n";
  warn << "tree1->root " << tree1->root.hash << " cnt " << tree1->root.count << "\n";
  warn << "tree2->root " << tree2->root.hash << " cnt " << tree2->root.count << "\n";

  warn  << "+++++++++++++++++++++++++tree1++++++++++++++++++++++++++++++\n";
  tree1->compute_stats ();
  warn  << "+++++++++++++++++++++++++tree2++++++++++++++++++++++++++++++\n";
  tree2->compute_stats ();
  //warn  << "++++++++++++++++++++++++++db1+++++++++++++++++++++++++++++++\n";
  //tree1->db->dump_stats ();
  //warn  << "++++++++++++++++++++++++++db2+++++++++++++++++++++++++++++++\n";
  //tree2->db->dump_stats ();
#if 0
  warn  << "++++++++++++++++++++++++client1+++++++++++++++++++++++++++++\n";
  host1->clnt->dump_stats ();
  warn  << "++++++++++++++++++++++++client2+++++++++++++++++++++++++++++\n";
  host2->clnt->dump_stats ();
#else
  warn << "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n";
#endif

  //assert (tree1->root.hash == tree2->root.hash);

  if (nextfunc == cbv_null)
    exit (0);
  else
    nextfunc ();
}


void
test1 ()
{
  warn << "\n\n\n\n############################################################\n";
  setup ();
  addrand (tree1, 10000);
  addrand (tree2, 10000);
  host1->sync (wrap (&dump_stats, wrap (&test1)));
}

 
void
test (merkle_syncer::mode_t m, uint large_size, uint small_size, cbv::ref cb = cbv_null)
{
  warn << "\n\n\n\n############################################################\n";
  warn << "REPLICA TEST " << large_size << "/" << small_size << "\n";
  setup ();
  // tree1 -- holds large_size blocks
  // tree2 -- holds small_size blocks

  if (m == merkle_syncer::BIDIRECTIONAL) {
    addrand (tree1, tree2, small_size);
    addrand (tree1, large_size - small_size);
  } else {
    addrand (tree1, large_size);
    addrand (tree2, small_size);
  }

  warn  << "+++++++++++++++++++++++++tree1++++++++++++++++++++++++++++++\n";
  tree1->compute_stats ();
  warn  << "+++++++++++++++++++++++++tree2++++++++++++++++++++++++++++++\n";
  tree2->compute_stats ();
  err_flush();

  host1->sync (wrap (&dump_stats, cb), m);
}


void
do_tests (merkle_syncer::mode_t m, uint progress = 0)
{
  const uint data_points = 100;

  warn << "do_tests: " << progress << "\n";
  err_flush ();

  if (progress >= data_points) {
    warn << "Replica tests done -- bitching!\n";
    exit (0);
  }

  uint64 large_sz = (1 << 30) / (1 << 13);  // 1 GB

  large_sz /= 10;

  uint64 small_sz = (large_sz * progress) / (data_points - 1);

  test (m, large_sz, small_sz, wrap (do_tests, m, progress + 1));
}


int
main ()
{
  //replica_test ( 131072, 3971 );
  //do_tests (merkle_syncer::BIDIRECTIONAL, 0);
  do_tests (merkle_syncer::UNIDIRECTIONAL, 0);
  amain ();
}
