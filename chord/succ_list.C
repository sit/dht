#include "chord.h"

succ_list::succ_list (ptr<locationtable> locs,
		      chordID ID) : myID (ID), locations (locs) {

  for (int i = 1; i <= NSUCC; i++) {
    succlist[i].alive = false;
  }

  locations->increfcnt (myID);
  succlist[0].n = myID;
  succlist[0].alive = true;
  nsucc = 0;
}

chordID 
succ_list::first_succ ()
{
  for (int i = 1; i <= nsucc; i++) {
    if (succlist[i].alive) return succlist[i].n;
  }
  return myID;
}

int
succ_list::countrefs (chordID &x)
{
  int n = 0;
  for (int i = 0; i <= nsucc; i++) 
    if (succlist[i].alive && (x == succlist[i].n))
      n++;
  return n;
}

void
succ_list::print ()
{
  for (int i = 1; i <= nsucc; i++) {
    if (!succlist[i].alive) continue;
    warnx << "succ " << i << " : " << succlist[i].n << "\n";
  }
}

void
succ_list::replace_succ (int j)
{
#ifdef PNODE
  succlist[j].n = closestsuccfinger (succlist[j].n);
#else
  succlist[j].n = locations->closestsuccloc (succlist[j].n);
#endif
  succlist[j].alive = true;  
  locations->increfcnt (succlist[j].n);
}

void
succ_list::remove_succ (int j)
{
  if (succlist[j].alive) {
    locations->deleteloc (succlist[j].n);
    succlist[j].alive = false;
  }
}

void 
succ_list::new_succ (int i, chordID s) 
{
  locations->changenode(&succlist[i], s, locations->getaddress(s));
  if (i > nsucc) nsucc++;
}

void
succ_list::delete_succ (chordID &x)
{
  for (int i = 1; i <= NSUCC; i++) {
    if (succlist[i].alive && (x == succlist[i].n)) {
      locations->deleteloc (succlist[i].n);
      succlist[i].alive = false;
    }
  }
}

chordID
succ_list::closest_succ (chordID &x)
{
  chordID s = succlist[0].n;

  for (int i = 1; i <= nsucc; i++) {
    if ((succlist[i].alive) && between (x, s, succlist[i].n)) {
      s = succlist[i].n;
    }
  }
  return s;
}

chordID
succ_list::closest_pred (chordID &x)
{

  chordID p = succlist[0].n;
  for (int i = nsucc - 1; i >= 1; i--) {
    if ((succlist[i].alive) && between (p, x, succlist[i].n)) 
      return succlist[i].n;
  }
  return p;
}

u_long
succ_list::estimate_nnodes ()
{
  u_long n;
  chordID d = diff (myID, succlist[nsucc].n);
  if ((d > 0) && (nsucc > 0)) {
    chordID s = d / nsucc;
    chordID c = bigint (1) << NBIT;
    chordID q = c / s;
    n = q.getui ();
  } else 
    n = 1;
  return n;
}

void
succ_list::fill_getsuccres (chord_getsucc_ext_res *res)
{
  int n = 1;
  for (int i = 1; i <= nsucc; i++) {
    if (succlist[i].alive) n++;
  }
  res->resok->succ.setsize (n);
  res->resok->succ[0].x = succlist[0].n;
  location *l = locations->getlocation (succlist[0].n);
  res->resok->succ[0].r = locations->getaddress (succlist[0].n);
  res->resok->succ[0].a_lat = (long)(l->a_lat * 100);
  res->resok->succ[0].a_var = (long)(l->a_var * 100);
  res->resok->succ[0].nrpc = l->nrpc;
  n = 1;
  for (int i = 1; i <= nsucc; i++) {
    if (!succlist[i].alive) continue;
    l = locations->getlocation (succlist[i].n);
    res->resok->succ[n].x = succlist[i].n;
    res->resok->succ[n].r = locations->getaddress (succlist[i].n);
    res->resok->succ[n].a_lat = (long)(l->a_lat * 100);
    res->resok->succ[n].a_var = (long)(l->a_var * 100);
    res->resok->succ[n].nrpc = l->nrpc;
    n++;
  }
}

chordID
succ_list::operator[] (int n) 
{ 
  if (n > nsucc) 
    return succlist[0].n;
  else
    return succlist[n].n; 
};
