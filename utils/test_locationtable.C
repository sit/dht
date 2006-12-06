#include "id_utils.h"
#include "chord.h"
#include <chord_types.h>
#include "configurator.h"
#include "locationtable.h"
#include "location.h"

void printtable (ptr<locationtable> locs, str header, str prefix)
{
  if (header.len ())
    warnx << header << "\n";
  
  ptr<location> l = locs->first_loc ();
  while (l) {
    warnx << prefix << l->id () << " " << l->alive () << "\n";
    l = locs->next_loc (l->id ());
  }
}

int
main (int argc, char *argv[])
{
  ptr<locationtable> locs = New refcounted<locationtable> (10);

  Configurator::only ().set_int ("chord.max_vnodes", 1024);
  
  chord_node n;
  ptr<location> l;

  n.r.hostname = "192.168.0.1";
  n.r.port = 11977;
  n.vnode_num = 0;
  n.age = 0;
  n.x = make_chordID (n.r.hostname, n.r.port, n.vnode_num);
  n.coords.setsize (Coord::NCOORD + Coord::USING_HT);
  warnx << "base insert... ";
  l = locs->insert (n);
  assert (l);

  assert (locs->size () == 1);
  assert (locs->usablenodes() == 1);
  assert (locs->cached (l->id ()));
  assert (!locs->pinned (l->id ()));
  warnx << "ok\n";

  warnx << "setting dead... ";
  l->set_alive (false);
  assert (locs->usablenodes () == 0);
  warnx << "ok\n";

  warnx << "flushing... ";
  locs->flush ();
  assert (locs->size () == 0);
  warnx << "ok\n";

  warnx << "reinsert... ";
  locs->insert (l);
  assert (locs->size () == 1);
  assert (locs->usablenodes () == 0);
  assert (locs->cached (l->id ()));
  assert (locs->lookup (l->id ()) == l);
  warnx << "ok\n";

  warnx << "back to life... ";
  l->set_alive (true);
  assert (locs->usablenodes () == 1);

  warnx << "pinning sole node... ";
  locs->pin (n.x, 0); 
  warnx << "ok\n";
  warnx << "verifying pinned... ";
  assert (locs->pinned (l->id ()));
  warnx << "ok\n";
  warnx << "attempting flush of pin node... ";
  locs->flush ();
  assert (locs->cached (l->id ()));
  assert (locs->lookup (l->id ()) == l);
  assert (locs->size () == 1);
  warnx << "failed as expected.\n";

  warnx << "pinning a successor and attempting flush... ";
  locs->pin (n.x, 1);
  locs->flush ();
  assert (locs->cached (l->id ()));
  warnx << "ok\n";

  warnx << "resetting locationtable... ";
  locs = New refcounted<locationtable> (10);
  warnx << "ok\n";

  locs->insert (l);
  warnx << "pinning self, pred and succlist... ";
  locs->pin (n.x, 0);
  locs->pin (n.x, -1);
  locs->pin (n.x, 16);
  warnx << " ok\n";

  warnx << "flushing... ";
  locs->flush ();
  assert (locs->cached (l->id ()));
  warnx << "ok\n";

  warnx << "testing with 2 nodes... ";
  n.vnode_num = 1;
  n.x = make_chordID (n.r.hostname, n.r.port, n.vnode_num);
  l = locs->insert (n);
  assert (locs->size () == 2);
  assert (locs->pinned (n.x));
  locs->flush ();
  assert (locs->size () == 2);
  warnx << "ok\n";

  warnx << "pinning new self, pred and succlist and then flushing... ";
  locs->pin (n.x, 0);
  locs->pin (n.x, -1);
  locs->pin (n.x, 16);
  locs->flush ();
  assert (locs->cached (l->id ()));
  assert (locs->size () == 2);
  warnx << "ok\n";

  warnx << "inserting 10 nodes... ";
  for (unsigned int i = 0; i < 10; i++) {
    n.r.hostname = "192.168.1.1";
    n.r.port = 1000 + i;
    n.vnode_num = i;
    n.x = make_chordID (n.r.hostname, n.r.port, i);
    l = locs->insert (n);
    assert (l);
    assert (l->alive ());
    assert (locs->size () == 2 + i + 1);
    assert (locs->usablenodes () == 2 + i + 1);
  }
  warnx << "ok\n";

  warnx << "testing duplicate insert... ";
  ptr<location> w = locs->insert (n);
  assert (l == w);
  assert (locs->size () == 12);
  assert (locs->usablenodes () == 12);
  warnx << "ok\n";

  printtable (locs, "insert", "");
}

