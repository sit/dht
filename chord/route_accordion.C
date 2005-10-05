#include "accordion.h"

#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <modlogger.h>

#define trace modlogger ("route_accordion", modlogger::TRACE)

void
route_accordion::first_hop (cbhop_t cbi, ptr<chordID> guess)
{
  cb = cbi;

  ptr<recroute_route_arg> ra = New refcounted<recroute_route_arg> ();

  ra->routeid = arouteid_;
  ra->succs_desired = desired_;
  v->my_location ()->fill_node (ra->origin);
  ra->x = x;
  ra->retries = 0;
  trace << (v->my_ID ()>>144) << ": new route_accordion::first_hop: key " 
    << (x>>144) << " desired = " << desired_ << "\n";

  vnode *vp = v;
  ((accordion *) vp)->doaccroute (NULL, ra);

  clock_gettime (CLOCK_REALTIME, &start_time_);
}


