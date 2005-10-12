#include "chord.h"
#include "accordion_table.h"
#include <id_utils.h>
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include "modlogger.h"
#include <configurator.h>
#include <math.h>

#define MINTIMEOUT 60
#define NUM_ASKED 5
#define TIMEOUT_THRES 0.9

#define attrace modlogger ("accordion_table")

accordion_table::accordion_table (ptr<accordion> v, ptr<locationtable> l) 
  : myvnode (v), 
  locations (l)
{
  myID = v->my_ID ();
  mingap = 0;
  nout = 0;
}

void
accordion_table::start ()
{
  //directly ask my successor for a lot of
  //routing entries
  d_sec = ((1+NUM_ASKED) * BYTES_PER_ID)/myvnode->get_budget ();
  d_msec = (1000 *(1+NUM_ASKED) * BYTES_PER_ID)/myvnode->get_budget () - d_sec * 1000;
  assert(d_sec > 0 || d_msec > 0);
  attrace << (myID>>144) << " :start d_sec " << d_sec << " d_msec " << d_msec << "\n";
  explore_table ();
}

void
accordion_table::explore_table ()
{
  int bavail = myvnode->get_bavail ();
  if (bavail > 0) {
    unsigned para = myvnode->get_para ();
    myvnode->explored ();
    vec<ptr<location> > v = biggest_gap (para);
    bool rand = false;
    if (!v.size ()) {
      v = random_nbr (para);
      rand = true;
    }
    assert (v.size () == 2);
    if (v[0]->id () != myID) {
      //explore the chosen neighbor's routing table
      if (nout < 10) { //don't let too many exploration messages outstanding at the same time
	nout++;
	myvnode->fill_gap (v[0], v[1], wrap (this,&accordion_table::fill_gap_cb, v[0]));
      }
      attrace << (myID>>144) << ": explore_table sz " << locations->usablenodes() 
      << " " << (v[0]->id ()>>144) << " rand " << (rand?1:0) << " end " << (v[1]->id ()>>144) << " mingap " 
      << mingap << " bavail " << bavail << " para " << para << " nout " << nout << "\n";
    } else
      attrace << (myID>>144) << ": explore_table " << (myID>>144) << " dropped\n";
  }else
    attrace << (myID>>144) << ": explore_table  bavail " << bavail << "\n";

  delaycb (d_sec, d_msec * 1000000, wrap (this, &accordion_table::explore_table));
}

void
accordion_table::fill_gap_cb (ptr<location> p, 
    vec<chord_node> nlist, chordstat err)
{
  nout--;
  if (nout < 0)
    nout = 0;
  if (err) {
    warnx << "accordion_table::fill_gap_cb RPC failure " << err << "\n";
    p->set_alive (false);//XXX jy:to be fixed
  }else {
    strbuf newinfo;
    newinfo << (myID>>144) << ": fill_gap_cb from " << (p->id ()>>144)
      << " ( ";
    //the first id in the list is always the node itself
    for (unsigned int i = 0; i < nlist.size (); i++) {
      newinfo << (nlist[i].x>>144) << "," << nlist[i].knownup << "," << nlist[i].age << " ";
      locations->insert (nlist[i]);
    }
    attrace << newinfo << ") \n";
    if ((nlist.size () < NUM_ASKED) 
      && (nlist.size () > 1)) {
      chordID dist = distance (p->id (),nlist[0].x) >> 144;
      if (dist > mingap)
	mingap = dist;
    }
  }
}

vec<ptr<location> >
accordion_table::random_nbr (unsigned p)
{
  chordID randID = make_randomID ();
  ptr<location> cur = locations->closestsuccloc (randID);
  ptr<location> next = cur;
  double ti;
  double to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;
  do {
      next = locations->closestsuccloc (next->id ()+1);
      ti = (double) next->knownup ()
	/ (double) (next->knownup () + next->age ());
  } while (((ti < to_thres) || !next->alive ()) && next->id ()!=myID) ;
  vec<ptr<location> > v;
  v.setsize (2);
  v[0] = cur;
  v[1] = next;
  return v;
}

vec<ptr<location> >
accordion_table::biggest_gap (unsigned p)
{
  ptr<location> cur = locations->closestsuccloc (myID+1);
  ptr<location> next = cur;
  double maxscaled_gap = 0.0;
  chordID gap, dist;
  double to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;

  //jy: this takes a complete scan of the routing table,
  //not the most efficient way to figure out which node to 
  //obtain information
  double ti;
  vec<ptr<location> > v;
  v.setsize (0);
  while (cur->id ()!=myID) {
    do {
      next = locations->closestsuccloc (next->id ()+1);
      ti = (double) next->knownup ()
	/ (double) (next->knownup () + next->age ());
    } while ((ti < to_thres|| (!next->alive ())) && (next->id ()!=myID));
    gap = distance (cur->id (),next->id ()) >> 144;
    dist = distance (myID, cur->id ()) >> 144;
    /*
    attrace << (myID>>144) << " mingap " << mingap << " gap " << gap << " cur " 
    << (cur->id ()>>144) << " init_age " << cur->init_age () << " age " << cur->age () << " next " << (next->id ()>>144) 
    << " next_inita " << next->init_age () << " pinned " << (locations->pinned (cur->id ())?1:0) 
    << " maxscaled_gap " << (int(maxscaled_gap * 100))<<"\n";
    */
    if (((double) gap.getu64 ()/(double) dist.getu64 ()) > maxscaled_gap) {
      while ((!cur->init_age () || !cur->alive () )
	&& cur->id ()!=next->id ()) {
	cur = locations->closestsuccloc (cur->id () + 1);
      }
      if (cur->init_age ()) {
	maxscaled_gap = (double) gap.getu64 ()/(double) dist.getu64 ();
	if (!v.size ()) 
	  v.setsize (2);
	v[0] = cur;
	v[1] = next;
      }
    }
    cur = next;
  }
  return v;
}

void
accordion_table::del_node (const chordID x)
{
  ptr<location> l = locations->lookup (x);
  attrace << (myID>>144) << " deleting nbr " << (x>>144) << "\n";
  //l->set_alive (false);
  l->set_loss ();
}

vec<ptr<location> >
accordion_table::nexthops (const chordID &x, unsigned p, vec<ptr<location> > tried)
{
  ptr<location> cur;
  cur = locations->closestpredloc (x);
  attrace << (myID>>144) << " key " << (x>>144) << " wow " << (cur->id ()>>144) << " age " 
    << cur->age () << " knownup " << cur->knownup () << " loss " << cur->get_loss () << "\n";
  chordID mypred = locations->closestpredloc (myID - 1)->id ();
  chordID mysuccdist = locations->closestsuccloc (myID + 1)->id ();
  mysuccdist = (mysuccdist >> 143);
  chordID mydist = distance (myID,x);
  double ndist, delay = 1000000.0;
  double to_thres;
  to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;
  
  unsigned i = 0;
  chordID dist,mindist;

  vec<float> ds;
  vec<ptr<location> > fs;
  vec<float> ts;

  int tti = tried.size () - 1;
  while (i < (8*p)) {
    dist = distance (cur->id (), x);
    if (!between (myID, x, cur->id ())
	||((dist > (mydist/2)) && (fs.size())))
      break;

    float ti = (float) cur->knownup () 
      / (float) (cur->knownup () + cur->age ());
    bool pinned = locations->pinned (cur->id ());
    char loss = cur->get_loss ();
    if (cur->alive ()) {
      while (tti > 0) {
	if (betweenrightincl (myID, tried[tti]->id (), cur->id ()))
	  break;
	tti--;
      }
      if (tti > 0 && tried[tti]->id () == cur->id ())
	break;
      bool good = (!loss
	  && ((ti > to_thres) || (cur->age ()< MINTIMEOUT) || pinned));
      i++;
      dist = (dist >> 144);
      if (i == 1) {
	mindist = dist;
	ndist = 1.0;
      }else 
	ndist = (double) dist.getu64 () / (double)mindist.getu64 ();
      float coord_d = Coord::distance_f (myvnode->my_location ()->coords (), cur->coords ())/2.0;
      attrace << (myID>>144) << " key " << (x>>144) << " next " 
	<< (cur->id ()>>144) << " coord_dist " << ((int)coord_d) 
	<< " budget " << cur->budget () << " dist " << dist
	<< " ndist " << ((int)(ndist*100))
	<< " knownup " << cur->knownup () << " age " << cur->age () 
	<< " ti " << (int(100*ti)) << " good " << (good?1:0) << " loss " 
	<< loss << "\n";
      delay = ndist * coord_d / cur->budget ();
      unsigned j = ds.size();
      ds.setsize (j+1);
      fs.setsize (j+1);
      ts.setsize (j+1);
      while (j > 0 && 
	  ((delay < ds[j-1] && good)|| (ti > ts[j-1]))) {
	ds[j] = ds[j-1];
	fs[j] = fs[j-1];
	ts[j] = ts[j-1];
	j--;
      }
      if (good)
	ts[j] = 1.0;
      else {
	ts[j] = ti;
	ts[j] = ts[j]/(loss+1);
      }
      ds[j] = delay;
      fs[j] = cur;
      j = fs.size ();
      while (j>p) {
	ds.pop_back ();
	fs.pop_back ();
	ts.pop_back ();
	j--;
      }
      if (p == 1 && good && dist < mysuccdist)
	break;
    }else {
      attrace << (myID>>144) << " key " << (x>>144) << " next " << 
	(cur->id ()>>144) << " not fresh knowup " << cur->knownup ()
	<< " age " << cur->age () <<  " ti " << (int(100*ti)) << " to " 
	<< (int(to_thres*100)) << " alive " 
	<< (cur->alive ()?1:0) << " pinned? " << (pinned?1:0) << "\n";
    }
    cur = locations->closestpredloc (cur->id ()-1);
  }
  assert (fs.size () <= p);
  return fs;
}

vec<ptr<location> >
accordion_table::get_fingers (ptr<location> src, chordID end, unsigned p)
{
  src = locations->insert (src);
  vec<ptr<location> > fs;
  ptr<location> cur;
  ptr<location> mysucc = locations->closestsuccloc (myID+1);
  double ti;
  double to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;

  bool inverted;
  if (betweenrightincl (src->id (), end, myID)) {
    inverted = false;
    cur = locations->closestpredloc (end-1);
    if (between (myID, end, cur->id ())) {
	assert(between (myID, end, mysucc->id ()));
    }else {
      assert(betweenrightincl (myID, mysucc->id (), end));
    }
  } else {
    inverted = true;
    cur = locations->closestsuccloc (end+1);
  }

  strbuf inserted;
  while ((fs.size () < 2*NUM_ASKED) 
      && cur->id ()!= myID 
      && (!inverted && between (myID, end, cur->id ()) || (inverted && between (end, myID, cur->id ())))) {
    ti = (double) cur->knownup () / (double) (cur->knownup () + cur->age ());
    if (cur->alive () && 
	(ti > to_thres) || (cur->age ()< MINTIMEOUT) || locations->pinned (cur->id ())) {
      fs.setsize (fs.size ()+1);
      int i = fs.size()-1;
      while ((i > 0) &&  
	  //make sure fs is sorted 
	  Coord::distance_f (src->coords (), cur->coords ()) < 
	  Coord::distance_f (src->coords (), fs[i-1]->coords ()))  {
	fs[i] = fs[i-1];
	i--;
      }
      fs[i] = cur;
    }
    if (!inverted) 
      cur = locations->closestpredloc (cur->id ()-1);
    else
      cur = locations->closestsuccloc (cur->id ()+1);
  }
  if (fs.size () == 0) {
    if (!inverted)
      cur = mysucc;
    else
      cur = locations->closestpredloc (myID-1);
    unsigned j = 0;
    while ((cur->id ()!= myID) && j < (NUM_ASKED-1)) {
      ti = (double) cur->knownup () / (double) (cur->knownup () + cur->age ());
      if ((ti > to_thres) 
	  || (cur->age ()< MINTIMEOUT) 
	  || locations->pinned (cur->id ())) {
	fs.setsize(j+1);
	fs[j++] = cur;
      }
      if (!inverted)
	cur = locations->closestsuccloc (cur->id () + 1);
      else
	cur = locations->closestpredloc (cur->id ()-1);
    } 
  }else {
    unsigned fsz = fs.size ();
    while (fsz > NUM_ASKED) {
      fs.pop_back ();
      fsz--;
    }
    bool seen_succ = false;
    for (unsigned i = 0; i < fs.size (); i++) {
      if (fs[i]->id () == mysucc->id ())
	seen_succ = true;
    }
    if (!seen_succ) 
      fs[fs.size()-1] = mysucc;
  }
  assert (!fs.size () || fs[0]->id ()!=myID);
  return fs;
}

void
accordion_table::fill_nodelistresext (chord_nodelistextres *res)
{
  unsigned p = myvnode->get_para ();
  double to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;
  double ti;
  unsigned size = 0;

  ptr<location> cur = locations->closestsuccloc (myID+1);
  while (cur->id () != myID) {
    if (cur->alive ()) {
      bool pinned = locations->pinned (cur->id ());  
      ti = (double) cur->knownup () / (double) (cur->knownup () + cur->age ());
      if (ti > to_thres || pinned || cur->age () < MINTIMEOUT) {
	res->resok->nlist.setsize (size + 1);
	cur->fill_node_ext (res->resok->nlist[size]);
	size++;
      }
    }
    cur = locations->closestsuccloc (cur->id () + 1);
  }
}
