#include "async.h"
#include "skiplist.h"

struct item {
  item (int k, char *f) : key (k), foo (f) {}
  int key;
  char *foo;
  sklist_entry<item> sklink;
};

void print_item (item *p)
{
  warnx << p->key << " " << p->foo << "\n";
}

int main (int argc, char *argv[])
{
  skiplist<item,int,&item::key,&item::sklink> test;

  // single item case
  item *s = New item (1, "second");
  test.insert (s);
  item *t = test.search (1);
  assert (t == s);
  t = test.closestpred (2);
  assert (t == s);

  assert (test.last () == s);
 
  // now there are two.
  t = New item (0, "first");
  test.insert (t);
  item *u = test.search (1);
  assert (u == s);
  u = test.search (0);
  assert (u == t);
  
  assert (test.last () == s);

  u = test.closestpred (1);
  assert (u == t);

  u = test.closestsucc (0);
  assert (u == s);
  
  // insert at end and before beginning
  item *v = New item (2, "third");
  test.insert (v);
  item *w = New item (-1, "zeroeth");
  test.insert (w);
  
  assert (test.last () == v);
  
  warnx << "All added...\n";
  test.traverse (wrap (print_item));

  // No dupes
  for (int i = -1; i < 3; i++) {
    item *z = New item (i, "no dupes!");
    bool b = test.insert (z);
    assert (!b);
  }

  // walking the list manually
  u = test.last ();
  assert (u == v);
  u = test.prev (u);
  assert (u == s);
  u = test.prev (u);
  assert (u == t);
  u = test.prev (u);
  assert (u == w);
  u = test.prev (u);
  assert (u == NULL);

  // pred and succ methods
  u = test.closestpred (-2);
  assert (u == v);
  u = test.closestpred (-1);
  assert (u == v);
  u = test.closestpred (0);
  assert (u == w);
  u = test.closestpred (1);
  assert (u == t);
  u = test.closestpred (2);
  assert (u == s);
  u = test.closestpred (3);
  assert (u == v);

  u = test.closestsucc (-2);
  assert (u == w);
  u = test.closestsucc (-1);
  assert (u == t);
  u = test.closestsucc (0);
  assert (u == s);
  u = test.closestsucc (1);
  assert (u == v);
  u = test.closestsucc (2);
  assert (u == w);
  u = test.closestsucc (3);
  assert (u == w);
  
  // Removing from the middle
  u = test.remove (1);
  assert (u == s);
  warnx << "'second' removed...\n";
  test.traverse (wrap (print_item));
  u = test.first ();
  assert (u == w);
  u = test.next (u);
  assert (u == t);
  u = test.next (u);
  assert (u == v);
  assert (test.prev (v) == t);
  u = test.next (u);
  assert (u == NULL);
  assert (test.last () == v);

  // Remove from front
  u = test.remove (-1);
  warnx << "'zeroeth' removed...\n";
  test.traverse (wrap (print_item));
  u = test.first ();
  assert (u == t);
  assert (test.prev (u) == NULL);
  u = test.next (u);
  assert (u == v);
  assert (test.prev (u) == t);
  u = test.next (u);
  assert (u == NULL);

  // Remove from end
  u = test.remove (2);
  assert (u == v);
  warnx << "'third' removed...\n";
  test.traverse (wrap (print_item));
  assert (test.last () == t);

  // Remove non-existent
  u = test.remove (5);
  assert (u == NULL);

  // Remove last.
  u = test.remove (0);
  assert (u == t);
  assert (test.first () == NULL);
  warnx << "'first' removed...\n";
  test.traverse (wrap (print_item));

  for (int i = 0; i < 500; i++) {
    test.insert (New item (2*i, "blah"));
  }
  for (int i = 0; i < 500; i++) {
    test.insert (New item (2*i + 1, "blah"));
  }
  
  item *a = test.first ();
  item *b = test.next (a);
  while (b) {
    if (b->key != a->key + 1) {
      test.traverse (wrap (print_item));
      fatal << "forwards a key = " << a->key << ", b key = " << b->key << "\n";
    }
    a = b;
    b = test.next (a);
  }
  
  b = test.last ();
  a = test.prev (b);
  while (a) {
    if (b->key - 1 != a->key) {
      test.traverse (wrap (print_item));
      fatal << "backwards a key = " << a->key << ", b key = " << b->key << "\n";
    }
    b = a;
    a = test.prev (b);
  }
  test.traverse (wrap (print_item));
  test.rtraverse (wrap (print_item));
  return 0;
}

