#include "chord.h"

bool present (vec<chordID> toes, chordID id);

toe_table::toe_table (ptr<locationtable> locs,
		      ptr<succ_list> succ) 
  : locations (locs), successors (succ), in_progress (0) {
  
  for (int i=0; i < MAX_LEVELS; i++) 
    target_size[i] = 2; //must be less than nsucc to bootstrap
  
  last_level = -2;
};

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
  locations->cacheloc (id, r);
  locations->ping (id, wrap (this, &toe_table::add_toe_ping_cb, id, level));
}

void
toe_table::add_toe_ping_cb (chordID id, int level)
{
  location *l = locations->getlocation (id);
  if (l->a_lat < level_to_delay (level)) {
    warn << "added " << id << " to level " << level << "\n";
    net_address r = locations->getaddress (id);
    locations->updateloc (id, r);
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
    location *l = locations->getlocation (toes[i]);
    if (l->a_lat < up)
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
      location *l = locations->getlocation (vl[i]);
      warn << "     " << vl[i] << " latency: " << (int)l->a_lat;
    }
  }

}
