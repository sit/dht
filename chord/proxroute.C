#include "proxroute.h"

#include "succ_list.h"
#include "finger_table.h"
#include "toe_table.h"

#include <configurator.h>
#include <coord.h>
#include <location.h>
#include <modlogger.h>
#include <misc_utils.h>

ref<vnode>
proxroute::produce_vnode (ref<chord> _chordnode, 
			  ref<rpc_manager> _rpcm,
			  ref<location> _l)
{
  return New refcounted<proxroute> (_chordnode, _rpcm, _l);
}

proxroute::proxroute (ref<chord> _chord,
		      ref<rpc_manager> _rpcm,
		      ref<location> _l)
  : fingerroute (_chord, _rpcm, _l)
{
  toes = New refcounted<toe_table> (mkref (this), locations);
  stabilizer->register_client (toes);
  addHandler (prox_program_1, wrap (this, &proxroute::dispatch));
}

proxroute::~proxroute () {}

void
proxroute::print (strbuf &outbuf) const
{
  // XXX maybe should call parent print.
  outbuf << "======== " << myID << " ====\n";
  fingers->print (outbuf);
  successors->print (outbuf);

  outbuf << "pred : " << my_pred ()->id () << "\n";
  if (toes) {
    outbuf << "------------- toes ----------------------------------\n";
    toes->print (outbuf);
  }
  outbuf << "=====================================================\n";
}

void
proxroute::dispatch (user_args *a)
{
  if (a->prog->progno != prox_program_1.progno) {
    fingerroute::dispatch (a);
    return;
  }
  
  switch (a->procno) {
  case PROXPROC_NULL:
    a->reply (NULL);
    break;
  case PROXPROC_GETTOES:
    {
      prox_gettoes_arg *ta = a->template getarg<prox_gettoes_arg> ();
      dogettoes (a, ta);
    }
    break;
  case PROXPROC_FINDTOES:
    {
      prox_findtoes_arg *ta = a->template getarg<prox_findtoes_arg> ();
      dofindtoes (a, ta);
    }
    break;
  default:
    a->reject (PROC_UNAVAIL);
    break;
  }
}

// return my table
void
proxroute::dogettoes (user_args *sbp, prox_gettoes_arg *ta)
{
  chord_nodelistextres res (CHORD_OK);
  toes->fill_nodelistresext (&res);
  sbp->reply (&res);
}

// find toes approprate for n's table
void
proxroute::dofindtoes (user_args *sbp, prox_findtoes_arg *ta)
{
  chord_nodelistres res (CHORD_OK);
  vec<chordID> r;
  vec<ptr<location> > rr;
  vec<float> coords;
  chord_node_wire n;
  unsigned int maxret;

  if (toes) {
    float maxd = toes->level_to_delay (ta->level);
    n = ta->n;
    maxret = toes->get_target_size(0);
    for (unsigned int i = 0; i < n.coords.size (); i++)
      coords.push_back ((float)n.coords[i]);

    //iterate through toe table and return at most distance away from n
    for (unsigned int l = 0; l < MAX_LEVELS; l++){
      vec<ptr<location> > t = toes->get_toes (l);
      for (unsigned int i = 0; i < t.size(); i++){
	if (in_vector(r, t[i]->id ()))
	   continue;
	if (Coord::distance_f (coords, t[i]->coords()) < maxd){ 
	  r.push_back (t[i]->id ());
	  rr.push_back (t[i]);
	  if (r.size() >= maxret)
	    break;
	}
      }
      if (r.size() >= maxret)
	break;
    }

    //warn << "find toes found " << r.size() << "\n";
    
    res.resok->nlist.setsize (r.size ());
    for (unsigned int i = 0; i < r.size (); i++)
      rr[i]->fill_node (res.resok->nlist[i]);
  } 
  
  //for(unsigned int i = 0 ; i < res.resok->nlist.size() ; i++){
  //  warn << "node " << res.resok->nlist[i].x << "\n";
  //}

  sbp->reply (&res);
  
}

static inline
bool candidate_is_closer (const ptr<location> &c, // new candidate node
			  const chordID &myID,
			  const chordID &x, // target
			  const vec<chordID> &failed, // avoid these
			  const vec<float> &qc, // their coords
			  float &mindist)   // previous best dist
{
  // Don't give back nodes that querier doesn't want.
  if (in_vector (failed, c->id ())) return false;
  // Do not overshoot, do not go backwards.
  if (!between (myID, x, c->id ())) return false;
      
  vec<float> them = c->coords ();
  if (!them.size ())
    return false; // XXX weird.
      
  // See if this improves the distance.
  float newdist = Coord::distance_f (qc, them);
  if (mindist < 0 || newdist < mindist) {
#if 0	
    char dstr[24];
    modlogger log = modlogger ("proximity-toe");
    log << "improving " << x << " from ";
    sprintf (dstr, "%10.2f", mindist);
    log << dstr << " to ";
    sprintf (dstr, "%10.2f", newdist);
    log << dstr << "\n";
#endif /* 0 */	
    mindist = newdist;
    return true;
  }
  return false;
}

/*
 * Find the best proximity or ID space move we can make for the chord
 * querier located at n, towards x.
 *
 * This code assumes that myID is not the predecessor of x; in this
 * case, testrange should be "inrange" and not doing this stuff.
 */
ptr<location>
proxroute::closestproxpred (const chordID &x, const vec<float> &n,
			     const vec<chordID> &failed)
{
  ptr<location> p = me_;
  
  float mindist = -1.0;

  vec<ptr<location> > ts = toes->get_toes (toes->filled_level ());
  for (size_t i = 0; i < ts.size (); i++) {
    if (candidate_is_closer (ts[i], myID, x, failed, n, mindist))
      p = ts[i];
  }
  // We have a toe that makes progress and is acceptable to the
  // querier, let's go for it.
  if (mindist >= 0.0)
    return p;
    
  // No good toes?  Either we are too close, or they were all rejected.
  // If we happen to span the key in our successor _list_, then
  // attempt to send to some close node in the last half of the
  // successor list, so that for fetches, you will almost definitely win.
  vec<ptr<location> > sl = succs ();
  size_t sz = sl.size ();
  if (sz > 1 && between (sl[0]->id (), sl[sz - 1]->id (), x)) {
    for (u_int i = 0; i < sz; i++) {
      if (candidate_is_closer (sl[i], myID, x, failed, n, mindist))
	p = sl[i];
    }
  }
  if (mindist >= 0.0)
    return p;

  // Okay, we are just too far away, let's just go as far as we can
  // with the fingers.
  ptr<location> f = fingers->closestpred (x, failed);
  ptr<location> u = successors->closestpred (x, failed);
  if (between (myID, f->id (), u->id ())) 
    p = f;
  else
    p = u;
  
  return p;
}


static inline chordID
greedy_speed (const vec<float> &qc, // querier coordinates
	      const ptr<location> c,     // candidate
	      const chordID &myID)
{
  vec<float> them = c->coords ();
  if (!them.size ()) {
    modlogger ("greedy_speed", modlogger::TRACE) << "No coordinates for "
						 << c->id () << "\n";
    return 0; // XXX weird
  }
  chordID id_dist = distance (myID, c->id ());
  float f = Coord::distance_f (qc, them);
  if (f < 1.0) f = 1.0;
  u_int32_t coord_dist = (u_int32_t) f;
  if (coord_dist == 0) {
    char buf[32];
    sprintf (buf, "%f", f);
    modlogger ("greedy_speed", modlogger::TRACE) << "dist "
						 << buf << " gave cd = 0\n";
    return 0;
  }
  
  return id_dist / coord_dist;
}

ptr<location>
proxroute::closestgreedpred (const chordID &x, const vec<float> &n,
			     const vec<chordID> &failed)
{
  ptr<location> p = vnode_impl::closestpred (x, failed); // fallback
  chordID bestspeed = 0;
  
  // the real "location table"
  vec<ptr<location> > candidates;
  {
    vec<ptr<location> > x;
    x = toes->get_toes (toes->filled_level ());
    while (x.size ()) 
      candidates.push_back (x.pop_front ());
    x = fingers->get_fingers ();
    while (x.size ()) 
      candidates.push_back (x.pop_front ());
    x = successors->succs ();
    while (x.size ()) 
      candidates.push_back (x.pop_front ());
  }

  // XXX filter out duplicate locations...
  for (size_t i = 0; i < candidates.size (); i++) {
    ptr<location> c = candidates[i];
      
    // Don't give back nodes that querier doesn't want.
    if (in_vector (failed, c->id ())) continue;
    // Do not overshoot, do not go backwards.
    if (!between (myID, x, c->id ())) continue;
    
    chordID speed = greedy_speed (n, c, myID);
    if (speed > bestspeed) {
      p = c;
      bestspeed = speed;
    }
  }
  
  return p;
}

ptr<location> 
proxroute::closestpred (const chordID &x, const vec<chordID> &failed)
{
  static bool initialized = false;
  static bool greedy = false;
  if (!initialized) {
    int x = 0;
    assert (Configurator::only ().get_int ("chord.greedy_lookup", x));
    greedy = (x == 1);
    initialized = true;
  }

  ptr<location> s;
  if (greedy)
    s = closestgreedpred (x, me_->coords (), failed);
  else
    s = closestproxpred (x, me_->coords (), failed);
    
  return s;
}
