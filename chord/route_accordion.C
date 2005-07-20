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
  v->my_location ()->fill_node (ra->origin);
  ra->x = x;
  ra->retries = 0;
  ra->succs_desired = desired_/2;
  if (!ra->succs_desired) 
    ra->succs_desired = 1;
  ra->retries = 0x8000000; //first bit signifies this is the primary path
  trace << v->my_ID () << ": new route_accordion::first_hop: desired = "
	<< desired_ << "\n";

  vnode *vp = v;
  ((accordion *) vp)->doaccroute (NULL, ra);

  clock_gettime (CLOCK_REALTIME, &start_time_);
}


