#include "merkle.h"
#define NEWDB

#ifdef NEWDB
ptr<dbfe> db = NULL;
#else
database *db = NULL;
#endif

#ifdef NEWDB
#define DBNAME "tmpdb"
#endif

ptr<dbrec> FAKE_DATA = New refcounted<dbrec> ("FAKE", strlen ("FAKE"));

void
dumpdb ()
{
  merkle_hash prevkey(0);
  warn << ">> dumpdb\n";
  ptr<dbEnumeration> iter = db->enumerate ();
  ptr<dbPair> entry = iter->nextElement (todbrec(merkle_hash(0)));
  for (int i = 0; entry ; i++) {
    merkle_hash key = to_merkle_hash (entry->key);
    warn << "[" << i << "] " << key << "\n";
    assert (prevkey <= key);
    entry = iter->nextElement ();
    prevkey = key;
  }
  warn << "<< dumpdb\n";
}
  

void
insert_blocks (merkle_tree *mtree, int upto, bool random)
{
  for (int i = 1; i <= upto; i++) {
    merkle_hash key;
    if (random) 
      key.randomize ();
    else
      key = i;

    mtree->insert (New block (key, FAKE_DATA));
    //mtree->dump ();

    if (i % 1000 == 0) {
      warn << "inserted " << i << " blocks..\n";
      //mtree->dump ();
      //dumpdb ();
      mtree->check_invariants ();
    }
  }
  mtree->check_invariants ();

}


void
insert_incr (merkle_tree *mtree, int upto)
{
  insert_blocks (mtree, upto, false);
}


void
insert_rand (merkle_tree *mtree, int upto)
{
  insert_blocks (mtree, upto, true);
}


#ifdef NEWDB
void
remove_all (merkle_tree *mtree)
{
  ptr<dbEnumeration> iter = db->enumerate ();
  ref<dbrec> zero = todbrec(merkle_hash(0));
  ptr<dbPair> entry = iter->nextElement (zero);
  while (entry) {
    block b (to_merkle_hash (entry->key), FAKE_DATA);
    // XXX change remove's interface to take just the key
    mtree->remove (&b);
    entry = iter->nextElement ();
  }
}

#else
void
remove_all (merkle_tree *mtree)
{
  u_int i = 0;
  block *b;
  while (1) {
    b = mtree->db->cursor (0);
    if (!b)
      break;
    mtree->remove (b);
    delete b;
    i++;
    if (i % 1000 == 0) {
      warn << "removed " << i << " blocks..\n";
      mtree->check_invariants ();
    }
  }
}
#endif


void
test_incr (int upto)
{
  warn << "\n=================== test_incr upto " << upto << "\n";
  merkle_tree mtree (db);
  insert_incr (&mtree, upto);
  remove_all (&mtree);
  mtree.dump ();
}


void
test_rand (int upto)
{
  warn << "\n=================== test_rand upto " << upto << "\n";
  merkle_tree mtree (db);
  insert_rand (&mtree, upto);
  remove_all (&mtree);
  mtree.dump ();
}

static void
create_database ()
{
#ifdef NEWDB
  unlink (DBNAME);

  db = New refcounted<dbfe> ();

  //set up the options we want
  dbOptions opts;
  opts.addOption("opt_async", 1);
  opts.addOption("opt_cachesize", 1000);
  opts.addOption("opt_nodesize", 4096);

  if (int err = db->opendb(DBNAME, opts)) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (-1);
  }
#else
  db = New database ();
#endif
}


int
main ()
{
  // XXX call random_init () ???
  //     -- between tests...???

  create_database ();

  test_incr (0);

  test_incr (1);
  test_incr (64);
  test_incr (65);
  test_incr (64 * 64);
  test_incr (64 * 64 + 1);
  test_incr (10000);

  do {
    test_rand (1);
    test_rand (64);
    test_rand (65);
    test_rand (64 * 64);
    test_rand (64 * 64 + 1);
    test_rand (10000);
  } while (0);


#ifdef NEWDB
  unlink (DBNAME);
#endif
}

