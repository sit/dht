
#ifndef __DHASH_SUCCLIST_OPT_H__
#define __DHASH_SUCCLIST_OPT_H__

bool
use_succlist (vec<ptr<location> > succs, ptr<location> pred)
{
  if (succs.size () == 0)
    return false;
  return (succs [succs.size ()-1]->id () == pred->id ());
}

ptr<location>
find_succ_from_list (vec<ptr<location> > nodes, chordID x)
{
  bool first = true;
  chordID nbar;
  ptr<location> xl = 0;
  for (unsigned i=0; i<nodes.size (); i++) {
    ptr<location> l = nodes [i];
    if (!l->alive ())
      continue;
    if (first || between (x, nbar, l->id ())) {
      nbar = l->id ();
      xl = l;
      first = false;
    }
  }
  return xl;
}

ptr<location>
find_pred_from_list (vec<ptr<location> > nodes, chordID x)
{
  bool first = true;
  chordID nbar;
  ptr<location> xl = 0;
  for (unsigned i=0; i<nodes.size (); i++) {
    ptr<location> l = nodes [i];
    if (!l->alive ())
      continue;
    if (first || between (nbar, x, l->id ())) {
      nbar = l->id ();
      xl = l;
      first = false;
    }
  }
  return xl;
}

vec<chord_node>
get_succs_from_list (vec<ptr<location> > nodes, chordID x)
{
  // basically convert succ list into chord_node, removing x's
  // predecessor. but we need to return a sorted list, so we do it
  // with find_successor.

  vec<chord_node> r;
  ptr<location> p = find_pred_from_list (nodes, x);
  chordID y = x;
 
  while (1) {
    ptr<location> s = find_succ_from_list (nodes, y);
    y = s->id ();
    if (y == p->id ())
      break;
    chord_node n;
    s->fill_node (n);
    r.push_back (n);
    // warn << "succ of " << x << ": " << y << "\n";
  }

  return r;
}

#endif

