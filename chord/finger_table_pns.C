#include "chord.h"
#include <id_utils.h>
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include "finger_table_pns.h"
#include "coord.h"

ptr<finger_table> 
finger_table_pns::produce_finger_table (ptr<vnode> v, ptr<locationtable> l)
{
  return New refcounted<finger_table_pns> (v, l);
}

finger_table_pns::finger_table_pns (ptr<vnode> v, ptr<locationtable> l)
  : finger_table (v, l), fp (0)
{
  for (int i = 0; i < NBIT; i++) 
    pns_fingers[i] = NULL;
}

ptr<location>
finger_table_pns::finger (int i)
{
  ptr<location> l;
  l = pns_fingers[i];
  if (!l)
    l = finger_table::finger (i);

  return l;
}

ptr<location>
finger_table_pns::operator[] (int i)
{
  return finger (i);
}


void
finger_table_pns::stats ()
{
  warnx << "PNS finger table\n";
}


void
finger_table_pns::stabilize_finger ()
{
  //make sure we keep the "real" fingers up to date
  finger_table::stabilize_finger ();

  //now check on one of the real fingers to make sure 
  // that we have the fastest finger in the "fan" in our table
  // find the first unique finger after fp
  int cur = fp % NBIT;
  chordID last_checked = finger_table::finger(cur)->id ();
  int checked = 0; //don't keep looping if there is one unique finger
  while (last_checked == finger_table::finger (cur)->id() && checked++ < NBIT) {
    fp++; 
    cur = fp % NBIT;
  }
  
  if (checked < NBIT) {
    ptr<location> real_finger = finger_table::finger (cur);

    warn << "fetching succlist from " << real_finger->id () << "(" << cur << ")\n";
    myvnode->get_succlist (real_finger, wrap (this, &finger_table_pns::getsucclist_cb, cur));
  }

}

void
finger_table_pns::getsucclist_cb (int target_finger, vec<chord_node> succs, chordstat err)
{
  if (err) {
    warn << "error fetching succlist\n";
  } else {
    //examine each of the successors and take the closest one as our finger
    float mindist = -1;
    int best_succ = -1;
    vec<float> my_coords = myvnode->my_location ()->coords ();

    chordID left = starts[target_finger];
    chordID right;
    if (target_finger < NBIT)
      right = starts[target_finger + 1];
    else 
      right = myvnode->my_ID ();

    if (!betweenbothincl(left, right, succs[0].x)) { //the real finger must be in range
      warn << succs[0].x << " not in [" << left << "," << right << "]\n";
    }

    //XXX debug
    int real_lat = 0;;


    for (u_int i = 1; i < succs.size (); i++) {
      //this is a recursive closeness measure;
      vec<float> candidate_coords;
      for (u_int c = 0; c < succs[i].coords.size (); c++)
	candidate_coords.push_back (succs[i].coords[c]);
      float curdist = Coord::distance_f (my_coords, candidate_coords);
      if ((mindist < 0 || curdist < mindist) &&
	  betweenbothincl (right, left, succs[i].x)) {
	if (mindist < 0) real_lat = (int)curdist;
	best_succ = i;
	mindist = curdist;
      }
    }

    if (best_succ > 0) {
      warn << "new PNS finger is the " << best_succ << "th successor. " 
	   << "latency: " << (int)mindist << " is better than " << real_lat << "\n";
      pns_fingers[target_finger] = locations->insert (succs[best_succ]);
    }
  }
}
