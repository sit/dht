#include <math.h>
#include "chord.h"

bool present (vec<chordID> toes, chordID id);

toe_table::toe_table (ptr<locationtable> locs, chordID ID)
  : locations (locs), myID (ID), in_progress (0)
{
  for (int i=0; i < MAX_LEVELS; i++) 
    target_size[i] = 2; //must be less than nsucc to bootstrap
  
  last_level = -2;
  //warnx << "toe table created\n";

  stable_toes = false;

}

void
toe_table::get_toes_rmt (int level) 
{
  in_progress++;

  vec<chordID> donors = get_toes (level - 1);
  for (unsigned int i = 0; i < donors.size (); i++) {
    ptr<chord_gettoes_arg> arg = New refcounted<chord_gettoes_arg> ();
    arg->v = donors[i];
    arg->level = level - 1;
    
    chord_nodelistextres *res = New chord_nodelistextres ();
    locations->doRPC (donors[i], chord_program_1,
		      CHORDPROC_GETTOES,
		      arg, res, 
		      wrap (this, &toe_table::get_toes_rmt_cb, res, level));
  }

}

void
toe_table::get_toes_rmt_cb (chord_nodelistextres *res, int level, clnt_stat err)
{
  if (!(err || res->status)){
    for (unsigned int i=0; i < res->resok->nlist.size (); i++) 
      add_toe (res->resok->nlist[i].x, res->resok->nlist[i].r, level);
  }
  in_progress--;
  delete res;
}

int
toe_table::level_to_delay (int level)
{
  return (max_delay >> level)*1000;
}


bool
present (vec<chordID> toes, chordID id) 
{
  for (unsigned int i = 0; i < toes.size (); i++) 
    if (toes[i] == id) return true;
  return false;
}

void
toe_table::add_toe (chordID id, net_address r, int level) 
{
  if (present (toes, id)) return;
  
  in_progress++;
  locations->cacheloc (id, r, cbchall_null); // XXX
  locations->ping (id, wrap (this, &toe_table::add_toe_ping_cb, id, level));
}

void
toe_table::add_toe_ping_cb (chordID id, int level, chordstat err)
{

  if (!err && locations->get_a_lat (id) < level_to_delay (level)) {
    warn << "added " << id << " to level " << level << "\n";
    net_address r = locations->getaddress (id);
    // what was this supposed to do??
    // locations->updateloc (id, r, cbchall_null); // XXX
    toes.push_back (id);
  }
  in_progress--;
}

vec<chordID> 
toe_table::get_toes (int level)
{
  int up = level_to_delay (level);
  vec<chordID> res;
  for (unsigned int i = 0; i < toes.size (); i++) {
    if (locations->get_a_lat (toes[i]) < up)
      res.push_back (toes[i]);
  }
  return res;
}

int
toe_table::filled_level () 
{
  for (int level = 0; level < MAX_LEVELS; level++) {
    vec<chordID> res = get_toes (level);
    if (res.size () < (unsigned short)target_size[level]) {
      warn << res.size () << " of " << target_size[level] << " at " 
	   << level << "\n";
      return level - 1;
    }
  }
  return MAX_LEVELS;
}

void
toe_table::dump ()
{
  for (int level=0; level < MAX_LEVELS; level++) {
    vec<chordID> vl = get_toes (level);
    warn << "Toes at level " << level << ":\n";
    for (unsigned int i=0; i < vl.size (); i++) {
      warn << "     " << vl[i] << " latency: "
	   << (int)locations->get_a_lat (vl[i]) << "\n";
	
    }
  }

}


void
toe_table::stabilize_toes ()
{
  stable_toes = true;

  int level = filled_level ();
  //warn << "stabilizing toes at level " << level << "\n";
  if (backoff_stabilizing () || continuous_stabilizing ()) return;
  
  if ((level < MAX_LEVELS) 
      && (level == get_last_level ())
      && (level > 0)) {
    //we failed to find enough nodes to fill the last level we tried
    //go back and get more donors and try again
    bump_target (level - 1);
    warn << "bumped " << level - 1 << " and retrying\n";
    level = filled_level ();
  }

  set_last_level (level);
  if (level < 0) { //bootstrap off succ list
    // grab the succlist and stick it in the toe table

    chordID ith_succ = myID;
    int goodnodes = locations->usablenodes () - 1;
    int numnodes = (NSUCC > goodnodes) ? goodnodes : NSUCC;
    //warnx << "toe bootstrapping " << goodnodes << " " << numnodes << "\n";
    for (int i = 0; i < numnodes; i++) {
      ith_succ = locations->closestsuccloc (ith_succ + 1); //XXX ith_succ + 1?
      add_toe (ith_succ, locations->getaddress (ith_succ), 0);
      //warnx << "add_toe called with " << ith_succ << "\n";
      stable_toes = false;
    }
  } else if (level < MAX_LEVELS) { //building table
    //contact level (level) nodes and get their level (level) toes
    get_toes_rmt (level + 1);
    stable_toes = false;
    //warnx << "toes unstable! " << stable_toes << "\n";
  } else { //steady state
    
  }
  //warnx << "stabilize done\n";
  return;
}

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
      int nbit;
      if (nnodes <= 1) nbit = 0;
      //else nbit = log2 (nnodes / log2 (nnodes));
      else {
	float n = nnodes;
	float log2_nnodes = logf(n)/logf(2.0);
	float div = n / log2_nnodes;
	nbit = (int)ceilf ( logf(div)/logf(2.0));
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
      int nbit;
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

