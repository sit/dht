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
}

void
accordion_table::explore_table ()
{
  int bavail = myvnode->get_bavail ();
  if (bavail > 0) {
    chordID end;
    ptr<location> n = biggest_gap (end);
    if (!n) {
      attrace << "explore_table " << myID << " use random\n";
      n = random_nbr (end);
    }
    if (n->id () != myID) {
      myvnode->bytes_sent (40);//40 bytes for exploration/reply

      //explore the chosen neighbor's routing table
      myvnode->fill_gap (n, end, wrap (this,&accordion_table::fill_gap_cb, n));
    
      attrace << " explore_table " << myID << " sz " << locations->usablenodes() 
	<< " " << n->id () << " end " << end << " mingap " 
	<< mingap << " bavail " << bavail << "\n";
    } else
      attrace << " explore_table " << myID << " dropped\n";
  }else
    attrace << " explore_table " << myID << " bavail " << bavail << "\n";
}

void
accordion_table::fill_gap_cb (ptr<location> p, 
    vec<chord_node> nlist, chordstat err)
{
  if (err) {
    warnx << "accordion_table::fill_gap_cb RPC failure " << err << "\n";
    p->set_alive (false);//XXX jy:to be fixed
  }else {
    strbuf newinfo;
    newinfo << myID << ":explore_table_cb " << nlist.size () << " ( ";
    //the first id in the list is always the node itself
    for (unsigned int i = 0; i < nlist.size (); i++) {
      newinfo << nlist[i].r << " " << nlist[i].vnode_num << " ";
      locations->insert (nlist[i]);
    }
    attrace << newinfo << ") \n";
    if (((nlist.size ()+1) < NUM_ASKED) 
      && (nlist.size () > 1)) {
      chordID dist = distance (nlist[0].x,nlist[1].x);
      if (dist > mingap)
	mingap = dist;
    }
  }
}

ptr<location>
accordion_table::random_nbr (chordID &end)
{
  chordID randID = make_randomID ();
  ptr<location> cur = locations->closestsuccloc (randID);
  end = (locations->closestsuccloc (cur->id ()+1))->id ();
  return cur;
}

ptr<location>
accordion_table::biggest_gap (chordID &end)
{
  ptr<location> cur = locations->closestsuccloc (myID);
  ptr<location> next;
  double maxscaled_gap = 0.0;
  ptr<location> pred_gap = NULL;
  chordID gap, dist;

  //jy: this takes a complete scan of the routing table,
  //not the most efficient way to figure out which node to 
  //obtain information
  while (cur->id ()!=myID) {
    next = locations->closestsuccloc (cur->id ()+1);
    gap = distance (cur->id (),next->id ());
    dist = distance (myID, cur->id ());
    if (gap > mingap
	&& (!locations->pinned (cur->id ())) //jy: skip nodes in successor list
	&& ((double) gap.getu64 ()/(double) dist.getu64 ()) > maxscaled_gap) {
      maxscaled_gap = (double) gap.getu64 ()/(double) dist.getu64 ();
      pred_gap = cur;
      end = next->id ();
    }
    cur = next;
  }
  return pred_gap;
}

void
accordion_table::del_node (const chordID x)
{
  ptr<location> l = locations->lookup (x);
  l->set_alive (false);
}

vec<ptr<location> >
accordion_table::nexthops (const chordID &x, unsigned p)
{
  ptr<location> cur;
  cur = locations->closestpredloc (x);
  chordID mypred = locations->closestpredloc (myID - 1)->id ();
  chordID mysucc = locations->closestsuccloc (myID + 1)->id ();
  chordID mydist = distance (myID,x);
  double ndist, delay = 1000000.0;
  double to_thres;
  to_thres = p > 1? (1-(exp(log(1-TIMEOUT_THRES)/(double)p))):TIMEOUT_THRES;
  
  time_t now = getusec () / 1000000;
  unsigned i = 0;
  chordID dist,mindist;

  vec<float> ds;
  vec<ptr<location> > fs;

  while (i < (8*p)) {
    dist = distance (cur->id (), x);
    if (!between (myID, x, cur->id ())
	||((dist > (mydist/2)) && (fs.size())))
      break;

    double ti = (double) cur->knownup () 
      / (double) (cur->knownup () + (now-cur->updatetime () + cur->age ()));
    if (cur->alive () && 
	(ti < to_thres || ((now-cur->updatetime ())< MINTIMEOUT))) {
      i++;
      dist = (dist >> 128);
      if (i == 1) {
	mindist = dist;
	ndist = 1.0;
      }else 
	ndist = (double) dist.getu64 () / (double)mindist.getu64 ();
      float coord_d = Coord::distance_f (myvnode->my_location ()->coords (), cur->coords ());
      attrace << "me: " << myID << " key " << x << " next " << i 
	<< " id " << cur->id () << " coord_dist " << ((int)coord_d) << 
	" budget " << cur->budget () << " ndist " << ((int)ndist) << 
	" knownup " << cur->knownup () << " age " << cur->age () << "\n";
      if (ndist > 1.0 || i==1) {
      }else {
	fprintf(stderr,"%.2f: %.2f %.2f\n", ndist, (double) dist.getu64 (),
	    (double) mindist.getu64 () );
	assert(0);
      }

      delay = ndist * coord_d / cur->budget ();
      unsigned j = ds.size();
      ds.setsize (j+1);
      fs.setsize (j+1);
      while (j > 0 && delay < ds[j-1]) {
	ds[j] = ds[j-1];
	fs[j] = fs[j-1];
	j--;
      }
      ds[j] = delay;
      fs[j] = cur;
    }
    cur = locations->closestpredloc (cur->id ()-1);
  }

  unsigned fsz = fs.size ();
  while (fsz > p) {
    fs.pop_back ();
    fsz--;
  }
  attrace << "me: " << myID << " key " << x << " next " << fs.size () << 
    " best " << (fs.size ()? fs[0]->id ():0) << "\n";
  return fs;
}

void
accordion_table::fill_gap_nodelistres (chord_nodelistres *res, ptr<location> src, chordID end)
{
  vec<ptr<location> > fs = get_fingers (src, end);
  res->resok->nlist.setsize (fs.size ());
  for (size_t i = 0; i < fs.size (); i++)
    fs[i]->fill_node (res->resok->nlist[i]);
}

vec<ptr<location> >
accordion_table::get_fingers (ptr<location> src, chordID end)
{
  src = locations->insert (src);
  vec<ptr<location> > fs;
  ptr<location> cur = locations->closestpredloc (end-1);
  ptr<location> mysucc = locations->closestsuccloc (myID+1);
  double ti;

  if (between (myID, end, cur->id ())) {
    assert(between (myID, end, mysucc->id ()));
  }else {
    assert(betweenrightincl (myID, mysucc->id (), end));
  }

  time_t now = getusec () / 1000000;
  strbuf inserted;
  int num = 0;
  while ((fs.size () < 2*NUM_ASKED) && between (myID, end, cur->id ())) {
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
    inserted << "mia " << cur->address () << " " << cur->vnode () << " " ;
    do {
      cur = locations->closestpredloc (cur->id ()-1);
      ti = (double) cur->knownup () 
	/ (double) (cur->knownup () + (now-cur->updatetime () + cur->age ()));
    } while ((ti < TIMEOUT_THRES) &&  (now-cur->updatetime ())> MINTIMEOUT) ;
    num++;
    assert(num < 20);
  }
  if (fs.size () == 0) 
    assert (betweenrightincl (myID, mysucc->id (), end));
  attrace << " gotinfo " << myID << " src " << src->id () 
    << " " << fs.size () << " (" << inserted << ") end " << end 
    << " succ " << mysucc->id () << "\n";

  unsigned fsz = fs.size ();
  while (fsz > NUM_ASKED) {
    fs.pop_back ();
    fsz--;
  }

  assert(fs.size () <= NUM_ASKED);
  return fs;
}

