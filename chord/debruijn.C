#include "chord.h"

#include "fingerlike.h"
#include "debruijn.h"
#include <location.h>
#include <locationtable.h>

#include <configurator.h>

struct debruijn_init {
  debruijn_init ();
} di;

debruijn_init::debruijn_init ()
{
  Configurator::only ().set_int ("debruijn.logbase", 2);
}

debruijn::debruijn () {}

void 
debruijn::init (ptr<vnode> v, ptr<locationtable> locs)
{
  myvnode = v;
  locations = locs;
  myID = v->my_ID ();

  mydoubleID = doubleID (myID, logbase);
  locations->pin (mydoubleID, -1);
  warn << myID << " de bruijn: double :" << mydoubleID << "\n";
}

ptr<location>
debruijn::debruijnptr ()
{
  return locations->closestpredloc (mydoubleID);
}

ptr<location>
debruijn::closestsucc (const chordID &x)
{
  ptr<location> s = myvnode->my_location ();
  ptr<location> succ = locations->closestsuccloc (myID + 1);
  ptr<location> n;

  if (betweenrightincl (myID, succ->id (), x)) s = succ;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ->id () + 1);
    if (betweenrightincl (myID, n->id (), x) && between (x, s->id (), n->id ())) {
      s = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID); 
  if (betweenrightincl (myID, n->id (), x) && between (x, s->id (), n->id ())) {
    s = n;
  }

  return s;
}

//XXX ignores failed node list.
ptr<location>
debruijn::closestpred (const chordID &x, vec<chordID> f)
{
  ptr<location> succ = locations->closestsuccloc (myID + 1);
  ptr<location> p;
  ptr<location> n;

  if (betweenrightincl (myID, succ->id (), x)) p = myvnode->my_location ();
  else p = succ;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ->id () + 1);
    if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
      p = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID);
  if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
    p = n;
  }

  return p;
}

ptr<location>
debruijn::closestpred (const chordID &x)
{
  ptr<location> succ = locations->closestsuccloc (myID + 1);
  ptr<location> p;
  ptr<location> n;

  if (betweenrightincl (myID, succ->id (), x)) p = myvnode->my_location ();
  else p = succ;

  for (int i = 1; i < logbase; i++) {
    n = locations->closestsuccloc (succ->id () + 1);
    if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
      p = n;
    }
    succ = n;
  }

  // use closestpred, because we pinned pred
  n = locations->closestpredloc (mydoubleID);
  if (between (myID, x, n->id ()) && between (p->id (), x, n->id ())) {
    p = n;
  }

  return p;
}

void
debruijn::stabilize ()
{
  myvnode->find_successor (mydoubleID, 
			   wrap (this, &debruijn::finddoublesucc_cb));
}

void
debruijn::finddoublesucc_cb (vec<chord_node> s, route search_path, chordstat status)
{
  if (status) {   
    warnx << myID << ": finddoublesucc_cb: failure status " << status << "\n";
  } else {
    //  warnx << myID << ": finddoublesucc_cb: " << mydoubleID << " is " << s 
    //  << "\n";
  }
}

void
debruijn::print (strbuf &outbuf)
{
  outbuf << myID << ": double: " << mydoubleID
	 << " : d " << locations->closestpredloc (mydoubleID)->id () << "\n";
}


class dbiter : public fingerlike_iter {
  friend class debruijn;
public:
  dbiter () : fingerlike_iter () {};
};

ref<fingerlike_iter>
debruijn::get_iter ()
{
  ref<dbiter> iter = New refcounted<dbiter> ();
  ptr<location> id = locations->closestpredloc (mydoubleID);
  iter->nodes.push_back (id);  

  return iter;
}
