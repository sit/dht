#include <math.h>
#include "chord.h"
#include "toe_table.h"

#include <coord.h>
#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <modlogger.h>
#define trace modlogger ("toes")

toe_table::toe_table ()
  : in_progress (0)
{
  for (int i=0; i < MAX_LEVELS; i++) {
    target_size[i] = 5; //must be less than nsucc to bootstrap
    toes[i] = New vec<ptr<location> >;
  }
  
  last_level = MAX_LEVELS; // will start at 0

  stable_toes = false;
}

void toe_table::init (ptr<vnode> v, ptr<locationtable> locs)
{
  locations = locs;
  myID = v->my_ID ();
  myvnode = v;
  
}

void
toe_table::prune_toes (int level)
{
  int removeindex = -1;
  //look for stale entries and remove one
  vec<float> us = myvnode->my_location ()->coords ();
  for(unsigned int i = 0 ; i < toes[level]->size() && removeindex < 0 ; i++){
    ptr<location> id = (*toes[level])[i];
    //do based on coords instead
    vec<float> them = id->coords ();
    if(!id->alive () || Coord::distance_f(us, them) >= level_to_delay (level))
      removeindex = i;
  }
  
  if(removeindex >= 0){
    for(unsigned int i = removeindex ; i < toes[level]->size() - 1 ; i++){
      (*toes[level])[i] = (*toes[level])[i+1];
    }
    toes[level]->pop_back();
  }
  
}

//the nodes that will likely fill level are in our level-2 row
void
toe_table::get_toes_rmt (int level) 
{
  vec<ptr<location> > donors = get_toes (max(level - 1, 0));
  for (unsigned int i = 0; i < donors.size (); i++) {
    in_progress++;
    ptr<chord_findtoes_arg> arg = New refcounted<chord_findtoes_arg> ();
    arg->level = level;
    myvnode->my_location ()->fill_node (arg->n);

    chord_nodelistres *res = New chord_nodelistres ();
    myvnode->doRPC (donors[i], chord_program_1,
		    CHORDPROC_FINDTOES,
		    arg, res, 
		    wrap (this, &toe_table::get_toes_rmt_cb, res, level));
  }
}

void
toe_table::get_toes_rmt_cb (chord_nodelistres *res, int level, clnt_stat err)
{
  in_progress--;
  if (!(err || res->status)){
    for (unsigned int i=0; i < res->resok->nlist.size (); i++) 
      add_toe (make_chord_node (res->resok->nlist[i]), level);
  }
  
  delete res;
}

int
toe_table::level_to_delay (int level)
{
  return (max_delay >> level)*1000;
}


bool
toe_table::present (chordID id) 
{
  for (unsigned int level = 0; level < MAX_LEVELS; level++)
    for (unsigned int i = 0; i < toes[level]->size (); i++) 
      if ((*toes[level])[i]->id () == id) return true;
  return false;
}

bool
toe_table::present (chordID id, int level) 
{
  for (unsigned int i = 0; i < toes[level]->size (); i++) 
    if ((*toes[level])[i]->id () == id) return true;
  return false;
}

void
toe_table::add_toe (const chord_node &n, int level)
{
  assert (level >= 0);
  // Assumes that the node is not already in the toe table.

  if (n.x == myID) return;
  if (present (n.x, level)) return;
 
  chordID id = n.x;

  float dist = -1.0;

  vec<float> them;
  for (u_int i = 0; i < n.coords.size (); i++)
    them.push_back ((float)n.coords[i]);
  vec<float> us = myvnode->my_location ()->coords ();
  dist = Coord::distance_f (them, us);

  //verify donors distance agrees with ours
  if (dist >= level_to_delay (level) || dist <= 0)
    return;

  ptr<location> nl = locations->insert (n);
  
  //bubble through the list looking for out of date and where to place
  //the new id.  alternative would be to use a llist instead
  unsigned int newindex = 0;
  bool newset = false;
  unsigned int i = 0;

  //stick at beginning?
  if (toes[level]->size () == 0){
    newindex = 0;
    newset = true;
    toes[level]->push_back (nl);
    //warn << "stuck to empty front\n";
  }
    
  //stick in middle?
  while(!newset && i < toes[level]->size ()){
    if(between(myID, (*toes[level])[i]->id (), id)){
      newindex = i;
      newset = true;
      //warn << "stick in middle\n";
    }
    i++;
  }

  //stick at end?
  if(!newset && (int)toes[level]->size () < target_size[level]){
    newindex = toes[level]->size ();
    newset = true;
    //warn << "adding to end\n";
  }

  if(newset && id != toes[level]->front()->id ()){
    //need to expand?
    if((int) toes[level]->size() < target_size[level]){
      toes[level]->push_back();
    }
    for(i = toes[level]->size() - 1; i > newindex ; i--)
      (*toes[level])[i] = (*toes[level])[i-1];
    //warn << "done shifting\n";
      
    (*toes[level])[newindex] = nl;
  }

	  
  if(newset){
    trace << "added " << id << " to level " << level
	  << " holding "
	  << toes[level]->size () << "/" << count_unique() << "\n";
    
    //try to promote the new one right away
    if(level+1 < MAX_LEVELS){
      add_toe(n, level+1);
    }
      
  }

}
  
vec<ptr<location> > 
toe_table::get_toes (int level)
{
  //int up = level_to_delay (level);
  vec<ptr<location> > res;
  if (level < 0 || level >= MAX_LEVELS)
    return res;
  for (unsigned int i = 0; i < toes[level]->size (); i++) {
    ptr<location> l =  (*toes[level])[i];
    if (l->alive ())
      res.push_back ((*toes[level])[i]);
  }

  return res;
}

//returns the first filled level, not first nonempty level
int
toe_table::filled_level () 
{
  for (int level = MAX_LEVELS - 1; level >= 0; level--) {
    if (toes[level]->size() == (unsigned short)target_size[level]) {
      return level;
    }
  }
  return 0;
}

//probably useful for stats
int
toe_table::count_unique ()
{
  vec<chordID> nodes;
  for (int level=0; level < MAX_LEVELS; level++) {
    vec<ptr<location> > vl = get_toes (level);
    for (unsigned int i=0; i < vl.size (); i++) {
      if(!in_vector(nodes, vl[i]->id ()))
	nodes.push_back(vl[i]->id ());
    }
  }
  return nodes.size();

}

void
toe_table::print ()
{
  for (int level=0; level < MAX_LEVELS; level++) {
    vec<ptr<location> > vl = get_toes (level);
    warn << "Toes at level " << level << ":\n";
    for (unsigned int i=0; i < vl.size (); i++) {
      warn << "     " << vl[i]->id () << " latency: "
	   << (int)vl[i]->distance ()
	   << " max " << level_to_delay(level) << "\n";
    }
  }
  
  warn << "Unique toe entries: " <<  count_unique() << "\n";

}

void
toe_table::stabilize_toes ()
{
  stable_toes = true;

  int level = get_last_level () + 1;

  if(level >= MAX_LEVELS)
    level = 0;

  //warn << "toe stabilize level " << level << "\n";
  
#if 0
  if ((level < MAX_LEVELS) 
      && (level == get_last_level ())
      && (level > 0)) {
    //we failed to find enough nodes to fill the last level we tried
    //go back and get more donors and try again
    
    bump_target (level - 1);
    warn << "bumped " << level - 1 << " and retrying\n";
    level = filled_level ();
  }
#endif 

  set_last_level (level);
  prune_toes(level);
  if (level == 0) { 
    // grab the succlist and stick it in the toe table
    vec<ptr<location> > succs = myvnode->succs ();
    for (u_int i = 0; i < succs.size (); i++) {
      chord_node n;
      succs[i]->fill_node (n);
      add_toe (n, 0);
    }
  } else {
    get_toes_rmt (level);
  } 
  
  //warnx << "stabilize done\n";
  return;
}

#if 0
bool
toe_table::betterpred1 (chordID current, chordID target, chordID candidate)
{
  return between (current, target, candidate);
}

// assumes some of form of the triangle equality!
char
toe_table::betterpred2 (chordID myID, chordID current, chordID target, 
			    chordID newpred)
{ 
  #define HIST 0
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  char r = 0;
  if (between (myID, target, newpred)) { // is newpred a possible pred?

    unsigned int cur_nrpc = locations->get_nrpc(current);
    unsigned int proposed_nrpc = locations->get_nrpc(newpred);
    u_long nnodes = locations->estimate_nodes();

    if ((current == myID) && (newpred != myID)) {
      r = 1;
    } else if ((cur_nrpc == 0) || (proposed_nrpc == 0)) {
      if (between (current, target, newpred)) r = 2;
    } else {
      u_long nbit;
      if (nnodes <= 1) nbit = 0;
      //else nbit = log2 (nnodes / log2 (nnodes));
      else {
	float n = nnodes;
	float log2_nnodes = logf(n)/logf(2.0);
	float div = n / log2_nnodes;
	nbit = (u_long)ceilf ( logf(div)/logf(2.0));
      };

      u_long target_bits = topbits (nbit, target);
      u_long current_bits = topbits (nbit, current);
      u_long prop_bits = topbits (nbit, newpred);

      u_long cur_diff = (target_bits > current_bits) ? 
	target_bits - current_bits :
	current_bits - target_bits;
      u_long prop_diff = (target_bits > prop_bits) ? 
	target_bits - prop_bits : 
	prop_bits - target_bits;
      if (n1bits (cur_diff) > n1bits (prop_diff)) { 
	r = 3;
      } else if (n1bits (cur_diff) == n1bits (prop_diff)) {
	float cur_delay = locations->get_a_lat(current);
	float proposed_delay = locations->get_a_lat(newpred);
	if ((proposed_delay + HIST) < cur_delay) r = 4;

      }
    }
  }
  

  return r;
}

// assumes some of form of the triangle equality!
bool
toe_table::betterpred3 (chordID myID, chordID current, chordID target, 
			    chordID newpred)
{ 
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  bool r = false;
  if (between (myID, target, newpred)) { // is newpred a possible pred?
    u_long nnodes = locations->estimate_nodes();

    if ((current == myID) && (newpred != myID)) {
      r = true;
    } else if ((locations->get_nrpc(current) == 0) || (locations->get_nrpc(newpred) == 0)) {
      r = between (current, target, newpred);
    } else {
      u_long nbit;
      if (nnodes <= 1) nbit = 0;
      else {
	float n = nnodes;
	float log2_nnodes = logf(n)/logf(2.0);
	float div = n / log2_nnodes;
	nbit = (int)ceilf ( logf(div)/logf(2.0));
      };
      u_long target_bits = topbits (nbit, target);
      u_long current_bits = topbits (nbit, current);
      u_long new_bits = topbits (nbit, newpred);
      u_long cur_diff = (target_bits > current_bits) ? 
	target_bits - current_bits :
	current_bits - target_bits;
      u_long new_diff = (target_bits > new_bits) ? 
	target_bits - new_bits : 
	new_bits - target_bits;

      float avg_delay = locations->get_avg_lat();
      float cur_delay = locations->get_a_lat(current);
      float new_delay = locations->get_a_lat(newpred);

      float cur_est = n1bits (cur_diff)*avg_delay + cur_delay;
      float new_est = n1bits (new_diff)*avg_delay + new_delay;

      float diff = cur_est - new_est;
      if (::fabs(diff) < 10000) 
	return between (current, target, newpred);

      r = (cur_est > new_est);
#if 0
      if (r) {
	char b[1024];
	sprintf (b, "cur_est=%f*%ld + %f=%f > new_est=%f*%ld + %f=%f\n", 
		 avg_delay, n1bits(cur_diff), cur_delay, cur_est,
		 avg_delay, n1bits(new_diff), new_delay, new_est);
	warn << "chose " << newpred << " since " << b;
      }
#endif      
     
    }
  }
  
  
  return r;
}


// assumes some of form of the triangle equality!
char
toe_table::betterpred_distest (chordID myID, chordID current, 
				   chordID target, 
				   chordID newpred)
{ 
  // #avg hop latency
  // #estimate the number of nodes to figure how many bits to compare
  char r = false;
  if (between (myID, target, newpred)) { // is newpred a possible pred?
    if ((current == myID) && (newpred != myID)) {
      r = 1;
    } else if ((locations->get_nrpc(current) == 0) || (locations->get_nrpc(newpred) == 0)) {
      if (between (current, target, newpred)) r = 2;
    } else {
      double D = locations->get_avg_lat();
      double cur_delay = locations->get_a_lat(current);
      double new_delay = locations->get_a_lat(newpred);
      double log2 = log(2);
      int N = locations->estimate_nodes();

      bigint dist_c = distance (current, target)*N;
      assert (NBIT > 32);
      int dist_size = dist_c.nbits ();
      int shift = (dist_size - 32 > 0) ? dist_size - 32 : 0;
      bigint high_bits = dist_c >> shift;
      double dist_c_exp = (double)high_bits.getui ();
      double fdist_c = ldexp (dist_c_exp, shift);
      double logdist_c = log (fdist_c)/log2;
      if (logdist_c < 0.0) logdist_c = 0.0;
      double d_current = (logdist_c - 160.0)*D + cur_delay;
      
      bigint dist_p = distance (newpred, target)*N;
      assert (NBIT > 32);
      dist_size = dist_p.nbits ();
      shift = (dist_size - 32 > 0) ? dist_size - 32 : 0;
      high_bits = dist_p >> shift;
      double dist_p_exp = (double)high_bits.getui ();
      double fdist_p = ldexp (dist_p_exp, shift);
      double logdist_p = log (fdist_p)/log2;
      if (logdist_p < 0.0) logdist_p = 0.0;
      double d_proposed = (logdist_p - 160.0)*D + new_delay;

      if (d_proposed < d_current) 
	r = 3;
      if (1) {
	char b[1024];
	sprintf (b, "d_cur = %f = %f*%f + %f; d_proposed = %f = %f*%f + %f", 
		 d_current, logdist_c - 160.0, D, cur_delay,
		 d_proposed, logdist_p - 160.0, D, new_delay);
	if (r) 
	  warn << "choosing " << newpred << " over " << current << " since " << b << "\n";
	else
	  warn << "choosing " << current << " over " << newpred << " since " << b << "\n";
      }
    }
  }
  
  
  return r;
}

bool
toe_table::betterpred_greedy (chordID myID, chordID current, 
				  chordID target, chordID newpred) 
{
  bool r = false;
  if ((current == myID) && (newpred != myID)) return true;
  if (between (myID, target, newpred)) { 

    if (locations->get_nrpc(current) == 0) return true;
    float cur_delay = locations->get_a_lat(current);
    if (locations->get_nrpc(newpred) == 0) return false;
    float new_delay = locations->get_a_lat(newpred);
    r = (new_delay < cur_delay);
  }
  return r;
}
#endif /* 0 */

void toe_table::fill_nodelistresext (chord_nodelistextres *res)
{
  vec<ptr<location> > t = get_toes (last_level);      
  res->resok->nlist.setsize (t.size ());
  for (unsigned int i = 0; i < t.size (); i++) {
    t[i]->fill_node_ext (res->resok->nlist[i]);
  }
}

void 
toe_table::fill_nodelistres (chord_nodelistres *res)
{
  fatal << "toe_table::fill_nodelistres not implemented.\n";
}


ptr<location>
toe_table::closestsucc (const chordID &x)
{
  //warnx << "doing a toe table lookup\n";
  return locations->closestsuccloc(x);

}

ptr<location>
toe_table::closestpred (const chordID &x, vec<chordID> failed)
{

  //warnx << "doing a toe table closestpred (with failures)\n";
  return locations->closestpredloc(x, failed);
}


ptr<location>
toe_table::closestpred (const chordID &x)
{

  //warnx << "doing a toe table closestpred\n";
  return locations->closestpredloc(x);
}

class toeiter : public fingerlike_iter {
  friend class toe_table;
public:
  toeiter () : fingerlike_iter () {};
};

ref<fingerlike_iter>
toe_table::get_iter ()
{
  ref<toeiter> iter = New refcounted<toeiter> ();
  iter->nodes = get_toes (filled_level ());
  return iter;
}
