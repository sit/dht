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
				      wrap (mkref (this), 
					    &vnode::stabilize_continuous, t));
}

void
vnode::stabilize_succ ()
{
  while (!finger_table[1].first.alive) {   // notify() may result in failure
    replacefinger (finger_table[1].start, &finger_table[1].first);
    notify (finger_table[1].first.n, myID); 
  }
  nout_continuous++;
  get_predecessor (finger_table[1].first.n, 
		   wrap (mkref (this), &vnode::stabilize_getpred_cb));
}

void
vnode::stabilize_getpred_cb (chordID p, net_address r, chordstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " 
	  << finger_table[1].first.n << " failure " << status << "\n";
    stable_fingers = false;
    nout_continuous--;
  } else {
    if (betweenleftincl (finger_table[1].start, finger_table[1].first.n, p)) {
      locations->cacheloc (p, r);
      challenge (p, wrap (mkref (this), &vnode::stabilize_getpred_cb_ok));
    } else {
      nout_continuous--;
      notify (finger_table[1].first.n, myID);
    }
  }
}


void
vnode::stabilize_getpred_cb_ok (chordID p, bool ok, chordstat status)
{
  nout_continuous--;
  if ((status == CHORD_OK) && ok) {
    if (betweenleftincl (finger_table[1].start, finger_table[1].first.n, p)) {
      net_address r = locations->getaddress (p);
      locations->changenode (&finger_table[1].first, p, r);
      updatefingers (p, r);
      stable_fingers = false;
      notify (finger_table[1].first.n, myID);
    }
  }
}

void
vnode::stabilize_pred ()
{
  if (predecessor.alive) {
    nout_continuous++;
    get_successor (predecessor.n,
		   wrap (mkref (this), &vnode::stabilize_getsucc_cb));
  } else 
    stable_fingers = false;
}

void
vnode::stabilize_getsucc_cb (chordID s, net_address r, chordstat status)
{
  // receive successor from my predecessor; in stable case it is me
  nout_continuous--;
  if (status) {
    warnx << "stabilize_getpred_cb: " << myID << " " << predecessor.n 
	  << " failure " << status << "\n";
    stable_fingers = false;
  } else {
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
#ifdef TOES
    stabilize_toes ();
#endif /*TOES*/
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
  stabilize_backoff_tmo = delaycb (sec, nsec, wrap (mkref (this), 
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

  if (!finger_table[i].first.alive) {
    //  warnx << "stabilize: replace finger " << i << "\n" ;
    replacefinger (finger_table[i].start, &finger_table[i].first);
    stable_fingers = false;
  }
  if (i > 1) {
    for (; i <= NBIT; i++) {
      if (!finger_table[i-1].first.alive) break;
      if (between (finger_table[i-1].start, finger_table[i-1].first.n,
		   finger_table[i].start)) {
	chordID s = finger_table[i-1].first.n;
	if (finger_table[i].first.n != s) {
	  locations->changenode (&finger_table[i].first, s, 
				 locations->getaddress(s));
	  updatefingers (s, locations->getaddress(s));
	  stable_fingers = false;
	}
      } else break;
    }
    if (i <= NBIT) {
      // warnx << "stabilize: " << myID << " findsucc of finger " << i << "\n";
      nout_backoff++;
      find_successor (finger_table[i].start, wrap (mkref (this), 
				&vnode::stabilize_findsucc_cb, i));
      i++;
    }
  }
  return i;
}

void
vnode::stabilize_findsucc_cb (int i, chordID s, route search_path, 
			    chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << "stabilize_findsucc_cb: " << myID << " " 
	  << finger_table[i].first.n << " failure " << status << "\n";
    stable_fingers = false;
  } else {
    if (betweenleftincl (finger_table[i].start, finger_table[i].first.n, s)) {
      challenge (s, wrap (mkref (this), &vnode::stabilize_findsucc_ok, i));
    }
  }
}

void
vnode::stabilize_findsucc_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (betweenleftincl (finger_table[i].start, finger_table[i].first.n, s)) {
      // warnx << "stabilize_findsucc_ok: " << myID << " " 
      //   << "new successor of " << finger_table[i].start 
      //    << " is " << s << "\n";
      locations->changenode (&finger_table[i].first, s, 
			     locations->getaddress(s));
      updatefingers (s, locations->getaddress(s));
      stable_fingers = false;
    }
  }
}

int
vnode::stabilize_succlist (int s)
{
  int j = s % (nsucc+1);

  if (j == 0) {
    if (stable_succlist) stable_succlist2 = true;
    else stable_succlist2 = false;
    stable_succlist = true;
  }
  if (!succlist[j].alive) {
    //  warnx << "stabilize: replace succ " << j << "\n";
    stable_succlist = false;
    replacefinger (succlist[j].n, &succlist[j]);
  }
  nout_backoff++;
  get_successor (succlist[j].n,
		 wrap (mkref (this), &vnode::stabilize_getsucclist_cb, j));
  return j+1;
}


void
vnode::stabilize_getsucclist_cb (int i, chordID s, net_address r, 
			       chordstat status)
{
  nout_backoff--;
  if (status) {
    warnx << "stabilize_getsucclist_cb: " << myID << " " << i << " : " 
	  << succlist[i].n << " failure " << status << "\n";
    stable_succlist = false;
  } else {
    //    warnx << "stabilize_getsucclist_cb: " << myID << " " << i 
    //	  << " : successor of " 
    //	  << succlist[i].n << " is " << s << "\n";
    if (s == myID) {  // did we go full circle?
      if (nsucc > i) {  // remove old entries?
	stable_succlist = false;
	for (int j = nsucc+1; j <= NSUCC; j++) {
	  if (succlist[j].alive) {
	    locations->deleteloc (succlist[j].n);
	    succlist[j].alive = false;
	  }
	}
      }
      nsucc = i;
    } else if (i < NSUCC) {
      if (succlist[i+1].n != s) {
	locations->cacheloc (s, r);
	challenge (s, wrap (mkref (this), &vnode::stabilize_getsucclist_ok, 
			    i+1));
      }
      if ((i+1) > nsucc) {
	stable_succlist = false;
	nsucc = i+1;
      }
    }
    u_long n = estimate_nnodes ();
    locations->replace_estimate (nnodes, n);
    nnodes = n;
  }
}

void
vnode::stabilize_getsucclist_ok (int i, chordID s, bool ok, chordstat status)
{
  if ((status == CHORD_OK) && ok) {
    if (succlist[i].n != s) {
	stable_succlist = false;
	locations->changenode (&succlist[i], s, locations->getaddress (s));
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
