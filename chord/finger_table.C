#include "chord.h"

finger_table::finger_table (ptr<locationtable> locs,
			    chordID ID) : locations (locs), myID (ID) 

{
  fingers[0].start = fingers[0].first.n = myID;
  fingers[0].first.alive = true;
  locations->increfcnt (myID);

  for (int i = 1; i <= NBIT; i++) {
    locations->increfcnt (myID);
    fingers[i].start = successorID(myID, i-1);
    fingers[i].first.n = myID;
    fingers[i].first.alive = true;
  }
}

chordID
finger_table::operator[] (int i)
{
  return fingers[i].first.n;
}

bool
finger_table::better_ith_finger (int i, chordID s)
{
  return betweenleftincl (fingers[i].start, fingers[i].first.n, s);
}

bool
finger_table::succ_alive () {
  return fingers[1].first.alive;
}

chordID 
finger_table::succ ()
{
  return fingers[1].first.n;
}

void
finger_table::updatefinger (chordID &x)
{
  net_address r = locations->getaddress (x);
  updatefinger (x, r);
}

void 
finger_table::updatefinger (chordID &x, net_address &r)
{
  check ();
  for (int i = 1; i <= NBIT; i++) {
    if (betweenleftincl (fingers[i].start, fingers[i].first.n, x)) {
      locations->changenode (&fingers[i].first, x, r);
    }
  }
  check ();
}

void
finger_table::replacefinger (int i)
{  
  check ();
#ifdef PNODE
  fingers[i].first.n = closestsuccfinger (fingers[i].start);
#else
  fingers[i].first.n = locations->closestsuccloc (fingers[i].start);
#endif
  fingers[i].first.alive = true;
  locations->increfcnt (fingers[i].first.n);
  check ();
}

void 
finger_table::deletefinger (chordID &x)
{
  if (x == myID) return;

  for (int i = 1; i <= NBIT; i++) {
    if (fingers[i].first.alive && (x == fingers[i].first.n)) {
      locations->deleteloc (fingers[i].first.n);
      fingers[i].first.alive = false;
    }
  }

 
}


chordID
finger_table::closestsuccfinger (chordID &x)
{
  chordID s = x;
  for (int i = 0; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if ((s == x) || between (x, s, fingers[i].first.n)) {
      s = fingers[i].first.n;
    }
  }

  return s;
}

chordID 
finger_table::closestpredfinger (chordID &x)
{
  chordID p = myID;
  for (int i = NBIT; i >= 1; i--) {
    if ((fingers[i].first.alive) && 
	between (myID, x, fingers[i].first.n)) {
      p = fingers[i].first.n;
      return p;
    }
  }

  return p;
}

int
finger_table::countrefs (chordID &x)
{
  int n = 0;
  for (int i = 0; i <= NBIT; i++) 
    if (fingers[i].first.alive && (x == fingers[i].first.n))
      n++;

  return n;
}
void
finger_table::check ()
{
  int j;
  int i;
  for (i = 1; i <= NBIT; i++) {
    if (fingers[i].first.n == myID) continue;
    else break;
  }
  if (i > NBIT) return;

  for (i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    j = i+1;
    while ((j <= NBIT) && !fingers[j].first.alive) j++;
    if (j > NBIT) {
      if (!betweenrightincl (fingers[i].start, myID, 
                             fingers[i].first.n)) {
        warnx << "table " << i << " bad\n";
        warnx << "start " << fingers[i].start << "\n";
        warnx << "first " << fingers[i].first.n << "\n";
        print ();
        assert (0);
      }
    } else {
      if (fingers[j].first.n == myID) {
        return;
      }
      if (!betweenrightincl (fingers[i].start, fingers[j].first.n,
                 fingers[i].first.n)) {
        warnx << "table " << i << " bad\n";
        print ();
        assert (0);
      }
    }
  }
}

void
finger_table::print ()
{
  for (int i = 1; i <= NBIT; i++) {
    if (!succ_alive ()) continue;
    warnx << "finger: " << i << " : " << fingers[i].start << " : succ " 
	  << fingers[i].first.n << "\n";
    
  }
}

void
finger_table::fill_getfingersres (chord_getfingersres *res)
{

  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  res->resok->fingers[0].x = fingers[0].first.n;
  res->resok->fingers[0].r = locations->getaddress (fingers[0].first.n);
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      res->resok->fingers[n].x = fingers[i].first.n;
      res->resok->fingers[n].r = locations->getaddress (fingers[i].first.n);
      n++;
    }
  }
}

void
finger_table::fill_getfingersresext (chord_getfingers_ext_res *res)
{

  int n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      n++;
    }
  }
  res->resok->fingers.setsize (n);
  res->resok->fingers[0].x = fingers[0].first.n;
  location *l = locations->getlocation (fingers[0].first.n);
  res->resok->fingers[0].r = locations->getaddress (fingers[0].first.n);
  res->resok->fingers[0].a_lat = (long)(l->a_lat * 100);
  res->resok->fingers[0].a_var = (long)(l->a_var * 100);
  res->resok->fingers[0].nrpc = l->nrpc;
  n = 1;
  for (int i = 1; i <= NBIT; i++) {
    if (!fingers[i].first.alive) continue;
    if (fingers[i].first.n != fingers[i-1].first.n) {
      l = locations->getlocation (fingers[i].first.n);
      res->resok->fingers[n].x = fingers[i].first.n;
      res->resok->fingers[n].r = locations->getaddress (fingers[i].first.n);
      res->resok->fingers[n].a_lat = (long)(l->a_lat * 100);
      res->resok->fingers[n].a_var = (long)(l->a_var * 100);
      res->resok->fingers[n].nrpc = l->nrpc;
      n++;
    }
  }
}
