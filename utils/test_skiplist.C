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

  // w[-1,zeroth] t[0,first] s[1,second] v[2,third]
  
  srandom (time (NULL));

  // single item case
  item *s = New item (1, "second");
  test.insert (s); assert (test.repok ());
  item *t = test.search (1);
  assert (t == s);
  t = test.closestpred (2);
  assert (t == s);

  assert (test.last () == s);
  assert (test.size () == 1);
 
  // now there are two.
  t = New item (0, "first");
  test.insert (t); assert (test.repok ());
  item *u = test.search (1);
  assert (u == s);
  u = test.search (0);
  assert (u == t);
  
  assert (test.last () == s);

  u = test.closestpred (1);
  assert (u == t);

  u = test.closestpred (0);
  assert (u == s);
  
  u = test.closestsucc (0);
  assert (u == t);

  u = test.closestsucc (1);
  assert (u == s);

  assert (test.size () == 2);
  
  // insert at end and before beginning
  item *v = New item (2, "third");
  test.insert (v);
  assert (test.size () == 3);
  item *w = New item (-1, "zeroeth");
  test.insert (w);
  assert (test.size () == 4);  
  assert (test.last () == v);
  
  warnx << "All added...\n";
  test.traverse (wrap (print_item));

  // No dupes
  for (int i = -1; i < 3; i++) {
    item *z = New item (i, "no dupes!");
    bool b = test.insert (z);
    assert (!b);
    assert (test.repok ());
    assert (test.size () == 4);
    delete z;
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
  assert (u == w);
  u = test.closestsucc (0);
  assert (u == t);
  u = test.closestsucc (1);
  assert (u == s);
  u = test.closestsucc (2);
  assert (u == v);
  u = test.closestsucc (3);
  assert (u == w);
  
  // Removing from the middle
  u = test.remove (1);
  assert (u == s);
  warnx << "'second' removed...\n";
  delete u;
  assert (test.repok ());
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
  delete u;
  assert (test.repok ());
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
  delete u;
  assert (test.repok ());
  
  test.traverse (wrap (print_item));
  assert (test.last () == t);

  // Remove non-existent
  u = test.remove (5);
  assert (u == NULL);
  assert (test.repok ());

  // Remove last.
  u = test.remove (0);
  assert (u == t);
  assert (test.first () == NULL);
  warnx << "'first' removed...\n";
  delete u;
  assert (test.repok ());

  test.traverse (wrap (print_item));
  warnx << "Testing interlaced inserts with order checking (1).\n";

  for (unsigned int i = 0; i < 1031; i++) {
    unsigned int z = (i * 5) % 1031;
    test.insert (New item (z, "blah"));
    assert (test.size () == i + 1);
    assert (test.repok ());
  }
  
  for (int i = 0; i < 1031; i++) {
    unsigned int z = (i * 7) % 1031;
    item *c = test.search (z);
    assert (c->key == (int) z);
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
  
  for (unsigned int i = 0; i < 1031; i++) {
    unsigned int z = (i * 11) % 1031;
    item *c = test.remove (z);
    assert (c->key == (int) z);
    delete c;
    assert (test.repok ());
    assert (test.size () == 1031 - i - 1);
  }
    
  assert (test.first () == NULL);
  
  warnx << "Testing interlaced inserts and removes (2).\n";

  for (int i = 0; i < 257; i++) {
    unsigned int z = (i * 13) % 513;
    test.insert (New item (z, "foo"));
    assert (test.repok ());
  }
  for (int i = 0; i < 257; i++) {
    unsigned int z = (i * 13) % 513;
    unsigned int y = (i * 3) % 513;
    item *c = test.remove (z);
    assert (c->key == (int) z);
    delete c; c = NULL;
    assert (test.repok ());
    item *d = New item (y, "baz");
    if (!test.insert (d))
      delete d;

    assert (test.repok ());
  }

  vec<item *> tmp;
  for (int i = 0; i < 257; i++) {
    unsigned int y = (i * 3) % 513;
    item *c = test.remove (y);
    if (c)
      tmp.push_back (c);
    assert (test.repok ());
  }
  warnx << "Testing re-insert of removed items.\n";
  for (unsigned int i = 0; i < tmp.size (); i++) {
    test.insert (tmp[i]);
    test.repok ();
  }
  for (unsigned int i = 0; i < tmp.size (); i++) {
    item *u = test.remove (tmp[i]->key);
    assert (u == tmp[i]);
    delete u;
  }
  tmp.clear ();

  assert (test.first () == NULL);
  test.repok ();

  warnx << "Testing random inserts and removes.\n";
  // Some random stuff
  for (int i = 200; i < 305; i += random() % 13) {
    item *c = New item (i, "qux");
    if (!test.insert (c))
      delete c;
    assert (test.repok ());
  }
  for (int i = 0; i < 230; i += random () % 11) {
    item *c = New item (i, "quz");
    if (!test.insert (c))
      delete c;
    assert (test.repok ());
  }
  while (test.first () != NULL) {
    int i = random () % 305;
    item *c = test.closestsucc (i);
    item *d = test.remove (c->key);
    delete d;
    assert (c == d);
    assert (test.repok ());
  }

  assert (test.repok ());

  return 0;
}

