#include "merkle.h"

void
insert_blocks (merkle_tree *mtree, int upto, bool random)
{
  for (int i = 1; i <= upto; i++) {
    if (random)
      mtree->insert (New block ());
    else
      mtree->insert (New block (i));

    //mtree->dump ();

    if (i % 1000 == 0) {
      warn << "inserted " << i << " blocks..\n";
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


void
remove_incr (merkle_tree *mtree, int upto)
{
  for (int i = upto; i >= 1; i--) {
    block *b = mtree->db->lookup (merkle_hash (i));
    mtree->remove (b);
    delete b;
    if (i % 1000 == 0) {
      warn << "removed " << i << " blocks..\n";
      mtree->check_invariants ();
    }
  }
  mtree->check_invariants ();
}


void
remove_rand (merkle_tree *mtree)
{
  merkle_hash key;
  database *db = mtree->db;
  int i = 1;
  while (db->first ()) {
    key.randomize ();
    block *b = db->cursor (key);
    if (!b)
      continue;
    mtree->remove (b);
    delete b;
    if (i % 1000 == 0) {
      warn << "removed " << i << " blocks..\n";
      mtree->check_invariants ();
    }
    i++;
  }
  mtree->check_invariants ();
}


void
test_incr (int upto)
{
  warn << "\n=================== test_incr upto " << upto << "\n";
  database db;
  merkle_tree mtree (&db);
  insert_incr (&mtree, upto);
  remove_incr (&mtree, upto);
  mtree.dump ();
}


void
test_rand (int upto)
{
  warn << "\n=================== test_rand upto " << upto << "\n";
  database db;
  merkle_tree mtree (&db);
  insert_rand (&mtree, upto);
  remove_rand (&mtree);
  mtree.dump ();
}


int
main ()
{
  // XXX call random_init () ???
  //     -- between tests...???

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
}
