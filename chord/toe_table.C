#include "chord.h"

bool present (vec<chordID> toes, chordID id);

toe_table::toe_table (ptr<locationtable> locs, chordID ID)
  : locations (locs), myID (ID), in_progress (0)
{
  for (int i=0; i < MAX_LEVELS; i++) 
    target_size[i] = 2; //must be less than nsucc to bootstrap
  
  last_level = -2;
}

void
toe_table::get_toes_rmt (int level) 
{
  in_progress++;

  vec<chordID> donors = get_toes (level - 1);
  for (unsigned int i = 0; i < donors.size (); i++) {
    ptr<chord_gettoes_arg> arg = New refcounted<chord_gettoes_arg> ();
    arg->v.n = donors[i];
    arg->level = level - 1;
    
    chord_gettoes_res *res = New chord_gettoes_res ();
    locations->doRPC (donors[i], chord_program_1,
		      CHORDPROC_GETTOES,
		      arg, res, 
		      wrap (this, &toe_table::get_toes_rmt_cb, res, level));
  }

}

void
toe_table::get_toes_rmt_cb (chord_gettoes_res *res, int level, clnt_stat err)
{
  if (err || res->status) return;
  for (unsigned int i=0; i < res->resok->toes.size (); i++) 
    add_toe (res->resok->toes[i].x, res->resok->toes[i].r, level);
  
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
  // xxx should check err?
  if (locations->get_a_lat (id) < level_to_delay (level)) {
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
	   << (int)locations->get_a_lat (vl[i]);
    }
  }

}


void
toe_table::stabilize_toes ()
{
  return;

  int level = filled_level ();
  warn << "stabilizing toes at level " << level << "\n";
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
    for (int i = 1; i < numnodes; i++) {
      ith_succ = locations->closestsuccloc (myID);
      add_toe (ith_succ, locations->getaddress (ith_succ), 0);
    }
  } else if (level < MAX_LEVELS) { //building table
    //contact level (level) nodes and get their level (level) toes
    get_toes_rmt (level + 1);
  } else { //steady state
    
  }
  return;
}
