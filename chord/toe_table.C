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

  if(removeindex >= 0) {
    locations->unpin ((*toes[level])[removeindex]->id ());
  

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
  locations->pin (n.x, 0); //PIN this location


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


void toe_table::fill_nodelistresext (chord_nodelistextres *res)
{
  vec<ptr<location> > t = get_toes (filled_level ());      
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
