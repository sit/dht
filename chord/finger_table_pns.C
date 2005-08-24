#include "chord.h"
#include <id_utils.h>
#include <misc_utils.h>
#include <location.h>
#include <locationtable.h>
#include <modlogger.h>
#include "finger_table_pns.h"
#include "coord.h"

#define trace   modlogger ("finger_table_pns", modlogger::TRACE)
#define warning modlogger ("finger_table_pns", modlogger::WARNING)

ptr<finger_table> 
finger_table_pns::produce_finger_table (ptr<vnode> v, ptr<locationtable> l)
{
  return New refcounted<finger_table_pns> (v, l);
}

finger_table_pns::finger_table_pns (ptr<vnode> v, ptr<locationtable> l)
  : finger_table (v, l), fp (0)
{
  for (int i = 0; i < NBIT; i++)
    pnsfingers[i] = NULL;
}

finger_table_pns::~finger_table_pns ()
{
}

ptr<location>
finger_table_pns::finger (int i)
{
  ptr<location> pnsf = pnsfingers[i];

  if (pnsf && pnsf->alive ())
    return pnsf;

  // XXX try to find some other PNS finger that is good enough.
#if 0
  if (pnsf) 
    trace << myvnode->my_ID () << ": falling through to real finger "
	  << i << " because the PNS finger was dead\n";
  else
    trace << myvnode->my_ID () << ": falling through to real finger "
	  << i << " because the PNS finger was missing\n";
#endif

  pnsfingers[i] = NULL;
  return finger_table::finger (i);
}

ptr<location>
finger_table_pns::operator[] (int i)
{
  return finger (i);
}


void
finger_table_pns::stats ()
{
  for (int i = 0; i < NBIT; i++)
    {
      trace << myvnode->my_ID () << " " << i << "th PNS finger :" 
	    << pnsfingers[i]->id() << " vs. " << finger(i)->id () 
	    << "\n";
    }
}

void
finger_table_pns::stabilize_finger ()
{
  // make sure we keep the "real" fingers up to date
  finger_table::stabilize_finger ();

  // now check on one of the real fingers to make sure 
  // that we have the fastest finger in the "fan" in our table
  // find the first unique finger after fp
  int left = fp;
  ptr<location> real_finger = finger_table::finger (fp);
  int checked = 0; // don't keep looping if there is one unique finger
  while (real_finger == finger_table::finger (fp) && checked++ < NBIT) {
    fp++;
    fp = fp % NBIT;
  }
  // fp is ready for next iteration of stabilize_finger
  
  if (checked < NBIT) {
    myvnode->get_succlist (real_finger,
			   wrap (this, &finger_table_pns::getsucclist_cb,
				 left, fp));
  }

}

void
finger_table_pns::getsucclist_cb (int l, int r, vec<chord_node> succs,
				  chordstat err)
{
  if (err) {
    warning << "error fetching succlist: " << err << "\n";
  } else {
    float mindist = -1;
    int best_succ = -1;
    Coord my_coords = myvnode->my_location ()->coords ();

    chordID left = starts[l];
    chordID right;
    if (r == 0) {
      right = myvnode->my_ID ();
    } else {
      right = starts[r];
    }
    
    int real_lat = 0;
    
    // examine each of the successors and take the closest one as our finger
    for (u_int i = 0; i < succs.size (); i++) {
      // We restrict fingers to be within the real range.
      if (!betweenleftincl (left, right, succs[i].x)) {
	trace << myvnode->my_ID () << ": terminating early for fingers "
	      << l << " thru " << r << "; i = " << i << ".\n";
	break;
      }
      
      // Update coordinates, age, etc if node is known.
      ptr<location> l = locations->lookup (succs[i].x);
      if (l) {
	locations->insert (succs[i]);
	if (!l->alive ())
	  continue;
      }

      Coord candidate_coords (succs[i]);
      float curdist = Coord::distance_f (my_coords, candidate_coords);
      trace << myvnode->my_ID () << ": candidate " << i << ": "
	    << succs[i].r.hostname << " at distance " << (int)curdist << "\n";

      // this is a recursive closeness measure
      if (mindist < 0 || curdist < mindist) {
	if (mindist < 0) real_lat = (int) curdist;
	best_succ = i;
	mindist = curdist;
      }
    }

    if (best_succ > 0) {
      trace << myvnode->my_ID () << ": new PNS finger " 
	    << l << " is successor "
	    << best_succ << "; latency: " << (int)mindist
	    << " is better than " << real_lat << "\n";
      ptr<location> nl = locations->insert (succs[best_succ]);
      int i = l;
      do {
	pnsfingers[i] = nl;
	i++;
      } while (i < r);
    }
  }
}
