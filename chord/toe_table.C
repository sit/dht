#include "chord.h"

#define NTOES 3
#define MAX_LEVELS 5

bool present (vec<chordID> toes, chordID id);

void
vnode::stabilize_toes ()
{

  if (toes->stabilizing ()) return;


  int level = toes->filled_level ();

  if (level < 0) {
    //grab the succlist and stick it in the toe table
    for (unsigned int i = 1; i < NSUCC + 1; i++) 
      if (succlist[i].alive) {
	toes->add_toe (succlist[i].n,
		       locations->getaddress (succlist[i].n), 0);
      }
  } else if (level < MAX_LEVELS) {
    //contact level (level) nodes and get their level (level) toes
    toes->get_toes_rmt (level + 1);
  } 

  return;
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
  locations->cacheloc (id, r);
  locations->ping (id, wrap (this, &toe_table::add_toe_ping_cb, id, level));
}

void
toe_table::add_toe_ping_cb (chordID id, int level)
{
  location *l = locations->getlocation (id);
  if (l->a_lat < level_to_delay (level)) {
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
      res.push_back ();
  }
  return res;
}

int
toe_table::filled_level () 
{
  for (int level = 0; ; level++) {
    vec<chordID> res = get_toes (level);
    if (res.size () < NTOES) return level - 1;
  }
  
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
