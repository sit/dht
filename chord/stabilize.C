/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <assert.h>
#include <chord.h>
#include <qhash.h>

bool
vnode::isstable (void) 
{
  return stable_fingers && stable_fingers2 && stable_succlist && 
    stable_succlist2;
}

bool
vnode::hasbecomeunstable (void)
{
  return ((!stable_fingers && stable_fingers2) ||
	  (!stable_succlist && stable_succlist2));
}


void
vnode::stabilize (void)
{
  stabilize_continuous (stabilize_timer);
  stabilize_backoff (0, 0, stabilize_timer);
}

void
vnode::stabilize_continuous (u_int32_t t)
{
  stabilize_continuous_tmo = NULL;
  if (nout_continuous > 0) {
    // stabilizing too fast
    t = 2 * t;
  } else {
    stabilize_succ ();
    stabilize_pred ();
  }
  if (hasbecomeunstable () && stabilize_backoff_tmo) {
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = 0;
    stabilize_backoff (0, 0, stabilize_timer);
  }
  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  continuous_timer = t;
  stabilize_continuous_tmo = delaycb (sec, nsec, 
				      wrap (this, 
					    &vnode::stabilize_continuous, t));
}

void
vnode::stabilize_succ ()
{
  if (!fingers->succ_alive ()) {   // notify() may result in failure
    fingers->replacefinger (1);
    chordID s = fingers->succ ();
    notify (s, myID); 
  }
  nout_continuous++;
  get_predecessor (fingers->succ (), 
		   wrap (this, &vnode::stabilize_getpred_cb, fingers->succ ()));
}

void
vnode::stabilize_getpred_cb (chordID sd,
			     chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " 
	  << sd << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      deletefingers (sd);
    nout_continuous--;
  } else {
    if (fingers->better_ith_finger (1, p)) {
      locations->cacheloc (p, r);
      challenge (p, wrap (this, &vnode::stabilize_getpred_cb_ok, sd));
    } else {
      nout_continuous--;
      notify (sd, myID);
    }
  }
}


void
vnode::stabilize_getpred_cb_ok (chordID sd,
				chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if ((status == CHORD_OK) && ok) {
    if (fingers->better_ith_finger (1, p)) {
      fingers->updatefinger (p);
      stable_fingers = false;
      notify (sd, myID);
    }
  }
}

void
vnode::stabilize_pred ()
{
  if (predecessor.alive) {
    nout_continuous++;
    get_successor (predecessor.n,
		   wrap (this, &vnode::stabilize_getsucc_cb,
			 predecessor.n));
  } else 
    stable_fingers = false;
}

void
vnode::stabilize_getsucc_cb (chordID pred, 
			     chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " << pred 
	  << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      deletefingers (pred);
  } else {
    //XXX do something to fix situation?
    if (s != myID) 
      stable_fingers = false;
  }
}

/*
 * RSC: I think that we should be using some sort of AI/MD so that
 * if things start changing we can adapt.  Right now once we get slow
 * we stay slow forever.  At the least, we need some sort of increase
 * (right now we're just MD).  However, if I turn on aimd, then the
 * Chord finger tables start dying, presumably because the increased
 * update frequency tickles a bug elsewhere.  That's a project for
 * another day.
 */
#define aimd 0
void
vnode::stabilize_backoff (int f, int s, u_int32_t t)
{
  stabilize_backoff_tmo = 0;
  if (!stable && isstable ()) {
    stable = true;
    warnx << gettime () << " stabilize: " << myID 
	  << " stable! with estimate # nodes " << nnodes << "\n";
  } else if (!isstable ()) {
    stable = false;
  }
  if (nout_backoff > 0) {
    if(aimd)
      t = (int)(1.2 * t);
    else
     t *= 2;
    // warnx << "stabilize_backoff: " << myID << " " << nout_backoff 
    //  << " slow down " << t << "\n";
  } else {
    f = stabilize_finger (f);
    s = stabilize_succlist (s);
    stabilize_toes ();
    if (isstable () && (t <= stabilize_timer_max * 1000))
      if(aimd)
        t = (int)(1.2 * t);
      else
        t *= 2;
    else if (aimd && t > 100)
      t -= 100;
  }
  u_int32_t t1 = uniform_random (0.5 * t, 1.5 * t);
  u_int32_t sec = t1 / 1000;
  u_int32_t nsec =  (t1 % 1000) * 1000000;
  backoff_timer = t;
  stabilize_backoff_tmo = delaycb (sec, nsec, wrap (this, 
						    &vnode::stabilize_backoff,
						    f, s, t));
}

int
vnode::stabilize_finger (int f)
{
  int i = f % (NBIT+1);

  if (i == 0) {
    if (stable_fingers) stable_fingers2 = true;
    else stable_fingers2 = false;
    stable_fingers = true;
  }

  if (i <= 1) i = 2;		// skip myself and immediate successor

  if (!fingers->alive (i)) {
    //  warnx << "stabilize: replace finger " << i << "\n" ;
    fingers->replacefinger (i);
    stable_fingers = false;
  }
  if (i > 1) {
    for (; i <= NBIT; i++) {
      if (!fingers->alive (i-1)) break;
      //FED - don't understand this check
      if (fingers->better_ith_finger (i-1, fingers->start(i))) {
	chordID s = (*fingers)[i-1];
	if ((*fingers)[i] != s) {
	  fingers->updatefinger (s);
	  stable_fingers = false;
	}
      } else break;
    }
    if (i <= NBIT) {
      // warnx << "stabilize: " << myID << " findsucc of finger " << i << "\n";
      nout_backoff++;
      chordID n = fingers->start (i);
      find_successor (n, wrap (this, &vnode::stabilize_findsucc_cb, n, i));
      i++;
    }
  }
  return i;
}

void
vnode::stabilize_findsucc_cb (chordID dn, int i, chordID s, route search_path, 
			      chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << "stabilize_findsucc_cb: " << myID << " " 
	  << dn << " failure " << status << "\n";
    stable_fingers = false;
    if (status == CHORD_RPCFAILURE)
      deletefingers (dn);
  } else {
    if (fingers->better_ith_finger (i, s)) {
      challenge (s, wrap (this, &vnode::stabilize_findsucc_ok, i));
    }
  }
}

void
vnode::stabilize_findsucc_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (fingers->better_ith_finger (i, s)) {
      fingers->updatefinger (s);
      stable_fingers = false;
    }
  }
}

int
vnode::stabilize_succlist (int s)
{
  int j = s % (successors->num_succ () + 1);
  
  if (j == 0) {
    if (stable_succlist) stable_succlist2 = true;
    else stable_succlist2 = false;
    stable_succlist = true;
  }
  if (!successors->nth_alive (j)) {
    stable_succlist = false;
    successors->replace_succ(j);
  }
  nout_backoff++;
  chordID jid = (*successors)[j];
  get_successor (jid,
		 wrap (this, &vnode::stabilize_getsucclist_cb, jid, j));
  return j+1;
}


void
vnode::stabilize_getsucclist_cb (chordID jid, int i, chordID s, net_address r, 
				 chordstat status)
{
  int nsucc = successors->num_succ ();
  nout_backoff--;
  if (status) {
    warnx << "stabilize_getsucclist_cb: " << myID << " " << i << " : " 
	  << jid << " failure " << status << "\n";
    stable_succlist = false;
    if (status == CHORD_RPCFAILURE)
      deletefingers (jid);
  } else if (s == myID) {  // did we go full circle?
    if (nsucc > i) {  // remove old entries?
      stable_succlist = false;
      for (int j = nsucc+1; j <= NSUCC; j++) 
	successors->remove_succ (j);
    }
    nsucc = i;
  } else if (i < NSUCC) {
    if ((*successors)[i+1] != s) {
      locations->cacheloc (s, r);
      challenge (s, wrap (this, &vnode::stabilize_getsucclist_ok, 
			  i+1));
    }
  }
  u_long n = successors->estimate_nnodes ();
  locations->replace_estimate (nnodes, n);
  nnodes = n;
  
}

void
vnode::stabilize_getsucclist_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if ((*successors)[i] != s) {
	stable_succlist = false;
	successors->new_succ (i, s);
    }
  }
}


void
vnode::stop ()
{
  if (stabilize_continuous_tmo) {
    warnx << "stop " << myID << " switch off cont stabilize timer\n";
    timecb_remove (stabilize_continuous_tmo);
    stabilize_continuous_tmo = NULL;
  }
  if (stabilize_backoff_tmo) {
    warnx << "stop " << myID << " switch off backoff stabilize timer\n";
    timecb_remove (stabilize_backoff_tmo);
    stabilize_backoff_tmo = NULL;
  }
}


void
vnode::stabilize_toes ()
{
  return;

  int level = toes->filled_level ();
  warn << "stabilizing toes at level " << level << "\n";
  if (toes->stabilizing ()) return;
  
  if ((level < MAX_LEVELS) 
      && (level == toes->get_last_level ())
      && (level > 0)) {
    //we failed to find enough nodes to fill the last level we tried
    //go back and get more donors and try again
    toes->bump_target (level - 1);
    warn << "bumped " << level - 1 << " and retrying\n";
    level = toes->filled_level ();
  }

  toes->set_last_level (level);
  if (level < 0) { //bootstrap off succ list
    //grab the succlist and stick it in the toe table
    for (int i = 1; i < successors->num_succ (); i++) 
      if (successors->nth_alive(i)) {
	chordID ith_succ = (*successors)[i];
    	toes->add_toe (ith_succ, locations->getaddress (ith_succ), 0);
      }
  } else if (level < MAX_LEVELS) { //building table
    //contact level (level) nodes and get their level (level) toes
    toes->get_toes_rmt (level + 1);
  } else { //steady state
    
  }

  return;
}
