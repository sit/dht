#include <assert.h>
#include <chord.h>
#include <qhash.h>
#include <chord_util.h>

/*
 *
 * Chord.C
 *
 * This file implements the "core" of the chord system
 */


bool
p2p::updatepred (wedge &w, sfs_ID &x)
{
  if (w.first == x)
    return false;

  doActionCallbacks(x, ACT_NODE_JOIN);

  if (!between (w.start, w.end, w.first)) {
    if (between (w.start, w.end, x)) {
      w.first = x;
      return true;
    } else if (between (w.first, w.start, x)) {
      w.first = x;
      return true;
    }
  } else if (between (w.start, w.end, x) && gt (x, w.first)) {
    w.first = x;
    return true;
  }
  return false;
}

bool
p2p::updatesucc (wedge &w, sfs_ID &x)
{
  if (w.first == x)
    return false;

  doActionCallbacks(x, ACT_NODE_JOIN);
  if (!between (w.start, w.end, w.first)) {
    if (between (w.start, w.end, x)) {
      w.first = x;
      return true;
    } else if (between (w.end, w.first, x)) {
      w.first = x;
      return true;
    }
  } else if (between (w.start, w.end, x) && gt (w.first, x)) {
    w.first = x;
    return true;
  }
  return false;
}

bool
p2p::noticesucc (int k, sfs_ID &x, net_address &r)
{
  bool t = false;
  for (int i = k; i <= NBIT; i++) {
    if (updatesucc (finger_table[i], x))
      t = true;
  }
  return t;
}

bool
p2p::notice (int k, sfs_ID &x, net_address &r)
{
  bool t = false;
  if (noticesucc (k, x, r))
    t = true;
  if (updatepred (predecessor, x))
    t = true;
  return t;
}


int
p2p::successor_wedge (sfs_ID &n)
{
  for (int i = 0; i <= NBIT; i++) {
    if (between (finger_table[i].start, finger_table[i].end, n))
      return i;
  }
  assert (0);
  return -1;
}

bool
p2p::lookup_anyloc (sfs_ID &n, sfs_ID *r)
{
  for (location *l = locations.first (); l != NULL; l = locations.next (l)) {
    if (l->n != n) {
      *r = l->n;
      return true;
    }
  }
  return false;
}

bool
p2p::lookup_closeloc (sfs_ID &n, sfs_ID *r)
{
  // warnx << "lookup_closeloc:\n";

  if (locations.first () == NULL) {
    warnx << "no nodes left\n";
    return false;
  }
  sfs_ID x = locations.first ()->n;
  for (location *l = locations.first (); l != NULL; l = locations.next (l)) {
    //    warnx << l->n << "\n";
    if ((n - l->n) < (n - x))
      x = l->n;
  }
  // warnx << "lookup_closeloc: return " << x << "\n";
  *r = x;
  return true;
}

void
p2p::set_closeloc (wedge &w)
{
  sfs_ID n;
  if (!lookup_closeloc (w.first, &n)) {
    fatal ("No nodes left; cannot recover");
  }
  // warnx << "set_closeloc: replace " << w.first << " with " << n << "\n";
  w.first = n;
  w.alive = true;
  doActionCallbacks(n, ACT_NODE_JOIN);
}


void
p2p::updateloc (sfs_ID &x, net_address &r, sfs_ID &source)
{
  if (locations[x] == NULL) {
     warnx << "add: " << x << " at port " << r.port << " source: " 
    	  << source << "\n";
    location *loc = New location (x, r, source);
    locations.insert (loc);
    doActionCallbacks(x, ACT_NODE_JOIN);
  } else {
    // warnx << "update: " << x << " at port " << r.port << " source "
    //	  << source << "\n";
    doActionCallbacks(x, ACT_NODE_UPDATE);
    locations[x]->addr = r;
    locations[x]->source = source;
  }
}

void
p2p::deleteloc (sfs_ID &n)
{
  // warnx << "deleteloc: " << n << "\n";
  assert (n != myID);
  for (int i = 0; i <= NBIT; i++) {
    if (predecessor.first == n)
      predecessor.alive = false;
    if (finger_table[i].first == n)
      finger_table[i].alive = false;
  }
  location *l = locations[n];
  if (l) {
    if (l->alive && (l->source != n))
      alert (l->source, n);
    l->alive = false;
    locations.remove (l);
    doActionCallbacks(n, ACT_NODE_LEAVE);
  }
}


static void
wedge_print (wedge &w)
{
  warnx << w.start << " " << w.end << " first: " << w.first << " alive? " 
	<< w.alive << "\n";
}

void
p2p::print ()
{
  for (int i = 0; i <= NBIT; i++) {
    warnx << "succ " << i << ": ";
    wedge_print (finger_table[i]);
  }
  warnx << "pred : ";
  wedge_print (predecessor);
}

p2p::~p2p()
{
#ifdef _SIM_
  // free memory used by initialize graph when finish
  free(edges);
#endif
}

p2p::p2p (str host, int hostport, const sfs_ID &hostID,
	  int port, const sfs_ID &ID) :
  wellknownID (hostID),
  myID (ID)
{
  // used for calculating num hops
  lookup_ops = 0;
  lookups_outstanding = 0;
  lookup_RPCs = 0;
  insert_or_lookup = 0;
  rpcdelay = 0;

  lsd_location_lookup = (getenv("LSD_LOCATION_LOOKUP"));

#ifdef _SIM_
  // used to help simulate
  initialize_graph();
#endif


  bzero(&stats, sizeof(chord_stats));
  stats.balance = New qhash<sfs_ID, int, hashID> ();

  sigcb(SIGUSR1, wrap (this, &p2p::stats_cb));

  
  wellknownhost.hostname = host;
  wellknownhost.port = hostport;
  myaddress.port = port;
  myaddress.hostname = myname ();

  finger_table[0].start = finger_table[0].end = finger_table[0].first = myID;
  finger_table[0].alive = true;
  warnx << "namemyID " << myaddress.hostname << "\n";
  warnx << "myID is " << myID << "\n";
  warnx << "wellknowID is " << wellknownID << "\n";
  warnx << "myport is " << myaddress.port << "\n";
  for (int i = 1; i <= NBIT; i++) {
    finger_table[i].start = successorID(myID, i-1);
    finger_table[i].end = successorID(myID, i);
    finger_table[i].end = decID (finger_table[i].end);
    finger_table[i].first = myID;
    finger_table[i].alive = true;
    warnx << "succ " << i << ": ";
    wedge_print (finger_table[i]);
  }
  predecessor.start = predecessorID (myID, 0);
  predecessor.end = predecessorID (myID, 0);
  predecessor.first = myID;
  predecessor.alive = true;

  location *l = New location (wellknownID, wellknownhost.hostname, wellknownhost.port,
			      myID);
  locations.insert (l);
  if (myID == wellknownID) {
    warnx << "bootstrap server\n";
  } else {
    warnx << myID << " collect from " << wellknownhost.hostname << "\n";
    l = New location (myID, myaddress.hostname, myaddress.port, myID);
    locations.insert (l);
    join ();
  }
  stabilize_tmo = delaycb (stabilize_timer, 
			   wrap (mkref (this), &p2p::stabilize, 2));
}

void
p2p::stabilize (int c)
{
  int i = c % (NBIT+1);
  bool stable = true;

  // warnx << "stabilize " << i << "\n";

  if (!predecessor.alive) stable = false;
  else get_successor (predecessor.first,
		   wrap (mkref (this), &p2p::stabilize_getsucc_cb));
  if (!finger_table[1].alive) stable = false;
  else get_predecessor (finger_table[1].first,
		     wrap (mkref (this), &p2p::stabilize_getpred_cb));
  if (i > 1) {
    if (!finger_table[i].alive) stable = false;
    else find_successor (finger_table[i].first, finger_table[i].start,
			  wrap (mkref (this), &p2p::stabilize_findsucc_cb, i));
  }
  int time = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
  //warnx << "stabilize in " << time << " seconds\n";
  stabilize_tmo = delaycb (time, 
			   wrap (mkref (this), &p2p::stabilize, i+1));
  if (!stable)
    bootstrap ();
}

void
p2p::stabilize_getsucc_cb (sfs_ID s, net_address r, sfsp2pstat status)
{
  // receive first successor from my predecessor; in stable case it is me
  if (status) {
    warnx << "stabilize_getsucc_cb: " << predecessor.first << " failure " 
    	  << status << "\n";
    bootstrap ();
  } else {
    if (updatepred (predecessor, s)) {
      //      print ();
      bootstrap ();
    }
  }
}

void
p2p::stabilize_getpred_cb (sfs_ID p, net_address r, sfsp2pstat status)
{
  // receive first predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << finger_table[1].first << " failure " 
	  << status << "\n";
    bootstrap ();
  } else {
    if (updatesucc (finger_table[1], p)) {
      // print ();
      bootstrap ();
    }
  }
}

void
p2p::stabilize_findsucc_cb (int i, sfs_ID s, route search_path, 
			    sfsp2pstat status)
{
  if (status) {
    warnx << "stabilize_findsucc_cb: " << finger_table[i].first << " failure " 
	  << status << "\n";
    bootstrap ();
  } else {
    if (updatesucc (finger_table[i], s)) {
      bootstrap ();
    }
  }
}

void
p2p::join ()
{
  sfs_ID n;

  if (!lookup_anyloc(myID, &n))
    fatal ("No nodes left to join\n");
  
  find_predecessor (n, myID, wrap (mkref (this), 
					     &p2p::join_findpred_cb));
}

void
p2p::join_findpred_cb (sfs_ID p, route search_path, sfsp2pstat status)
{
  if (status) {
    warnx << "join_findpred_cb: failed with " << status << "\n";
    join ();  // try again
  } else {
    updatepred (predecessor, p);
    //    warnx << "join_findpred_cb: pred is " << p << "\n";
    get_successor (p, wrap (mkref (this), &p2p::join_getsucc_cb, p));
  }
}

void 
p2p::join_getsucc_cb (sfs_ID p, sfs_ID s, net_address r, sfsp2pstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << status << "\n";
    join ();  // try again
  } else {
    //    warnx << "join_getsucc_cb: " << p << " " << s << "\n";
    if (between (p, s, myID) || (p == s)) {
      // we found the first successor of myID: s. if p == s, then there is 
      // only one other node in the system; that node is my successor and 
      // predecessor.
      //      warnx << "join_getsucc_cb: succ is " << s << "\n";
      notice (1, s, r);
      if (finger_table[1].alive) notify (finger_table[1].first, myID);
      if (predecessor.alive) notify (predecessor.first, myID);
      bootstrap ();
    } else {
      get_successor (s, wrap (mkref (this), &p2p::join_getsucc_cb, s));
    }
  }
}

void
p2p::bootstrap ()
{
    warnx << "bootstrap\n";
  if (nbootstrap > 0) {
    //warnx << "bootstrap: we are busy bootstrapping\n";
    return;
  }
  //  print ();
  nbootstrap = NBIT + 1;
  bootstrap_failure = false;
  stable = true;
  for (int i = 1; i <= NBIT; i++) {
    if (!finger_table[i].alive) {
      set_closeloc (finger_table[i]);
    }
    find_successor (finger_table[i].first, finger_table[i].start, 
		    wrap (mkref (this), &p2p::bootstrap_succ_cb, i, 
			  finger_table[i].first));
  }
  if (!predecessor.alive) {
    set_closeloc (predecessor);
  }
  find_predecessor (predecessor.first, predecessor.end, 
    wrap (mkref (this), &p2p::bootstrap_pred_cb, predecessor.first));
}

void
p2p::bootstrap_done ()
{
  warnx << "bootstrap_done: stable? " << stable << " at " << gettime ()
  	<< " failures? " << 
  bootstrap_failure << "\n";

  if (!finger_table[1].alive) stable = false;
  else notify (finger_table[1].first, myID);
  if (!predecessor.alive) stable = false;
  else notify (predecessor.first, myID);

  if (!stable) 
    bootstrap ();
  //  else
  //  print ();

}

void
p2p::bootstrap_succ_cb (int i, sfs_ID n, sfs_ID s, 
			route path, sfsp2pstat status)
{
  nbootstrap--;
  if (status) {
    //  warnx << "bootstrap_succ_cb: " << status << ": dead : " << n << "\n";
    bootstrap_failure = true;
  } else {
    if (updatesucc (finger_table[i], s)) {
      //      warnx << "bootstrap_succ_cb: updated\n";
      stable = false;
    }
    if (nbootstrap <= 0)
      bootstrap_done ();
  }
}

void
p2p::bootstrap_pred_cb (sfs_ID n, sfs_ID p, route search_path, sfsp2pstat status)
{
  nbootstrap--;
  if (status) {
    // warnx << "bootstrap_pred_cb: " << status << ": dead " << n << "\n";
    bootstrap_failure = true;
  } else {
    if (updatepred (predecessor, p)) {
      // warnx << "bootstrap_pred_cb: updated\n";
      stable = false;
    }
    if (nbootstrap <= 0)
      bootstrap_done ();
  }
}

void
p2p::notify (sfs_ID &n, sfs_ID &x)
{
  sfsp2p_notifyarg *na = New sfsp2p_notifyarg;
  sfsp2pstat *res = New sfsp2pstat;

  location *l = locations[x];
  assert (l);
  na->x = x;
  na->r = l->addr;
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_NOTIFY, na, res, wrap (mkref (this), 
								&p2p::notify_cb, res));
}

void
p2p::notify_cb (sfsp2pstat *res, clnt_stat err)
{
  if (err) {
    warnx << "notify_cb: RPC failure " << err << "\n";
  } else if (*res != SFSP2P_OK) {
    warnx << "notify_cb: RPC error" << *res << "\n";
  }
}

void
p2p::alert (sfs_ID &n, sfs_ID &x)
{
  sfsp2p_notifyarg *na = New sfsp2p_notifyarg;
  sfsp2pstat *res = New sfsp2pstat;

  location *l = locations[x];
  assert (l);
  na->x = x;
  na->r = l->addr;
  //warnx << "RPC CALL: SFSP2PPROC_ALERT" << "\n";
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_ALERT, na, res, wrap (mkref (this), 
					      &p2p::notify_cb, res));
}

void
p2p::alert_cb (sfsp2pstat *res, clnt_stat err)
{
  if (err) {
    warnx << "alert_cb: RPC failure " << err << "\n";
  } else if (*res != SFSP2P_OK) {
    warnx << "alert_cb: RPC error" << *res << "\n";
  }
}

void
p2p::doget_successor (svccb *sbp)
{
  if (finger_table[1].alive) {
    sfs_ID s = finger_table[1].first;
    sfsp2p_findres res(SFSP2P_OK);
    location *l = locations[s];
    assert (l);
    res.resok->node = s;
    res.resok->r = l->addr;
    sbp->reply (&res);
  } else {
    sbp->replyref (sfsp2pstat (SFSP2P_ERRNOENT));
  }
}

void
p2p::doget_predecessor (svccb *sbp)
{
  if (predecessor.alive) {
    sfs_ID p = predecessor.first;
    sfsp2p_findres res(SFSP2P_OK);
    location *l = locations[p];
    assert (l);
    res.resok->node = p;
    res.resok->r = l->addr;
    sbp->reply (&res);
  } else {
    sbp->replyref (sfsp2pstat (SFSP2P_ERRNOENT));
  }
}

void
p2p::dofindclosestpred (svccb *sbp, sfsp2p_findarg *fa)
{
  sfsp2p_findres res(SFSP2P_OK);
  sfs_ID p = myID;
  sfs_ID s = myID + 1;

  //  print ();
  for (int i = NBIT; i >= 0; i--) {
    if ((finger_table[i].alive) && 
	between (s, fa->x, finger_table[i].first)) {
      p = finger_table[i].first;
      break;
    }
  }
  //  warnx << "dofindclosestpred of " << fa->x << " is " << p << "\n";
  location *l = locations[p];
  assert (l);
  res.resok->x = fa->x;
  res.resok->node = p;
  res.resok->r = l->addr;
  sbp->reply (&res);
}

void
p2p::donotify (svccb *sbp, sfsp2p_notifyarg *na)
{
  // warnx << "donotify: " << na->x << "\n";
  updateloc (na->x, na->r, na->x);
  if (notice (1, na->x, na->r)) {
    bootstrap ();
  }
  sbp->replyref (sfsp2pstat (SFSP2P_OK));
}

void
p2p::doalert (svccb *sbp, sfsp2p_notifyarg *na)
{
  // warnx << "doalert: " << na->x << "\n";
  // perhaps less aggressive and check status of x first
  deleteloc (na->x);
  bootstrap ();
  sbp->replyref (sfsp2pstat (SFSP2P_OK));
}

void
p2p::dofindsucc (sfs_ID &n, cbroute_t cb)
{
  
  //  warn << "do find succ " << n << "\n";
  //int i = successor_wedge (n);
  if (!predecessor.alive) {
    set_closeloc (predecessor);
  }


  //  warn << "calling f_s " << predecessor.first << " " << n << "\n";
  find_successor (myID, n, wrap (mkref (this), &p2p::dofindsucc_cb, cb, n));

}

void
p2p::dofindsucc_cb (cbroute_t cb, sfs_ID n, sfs_ID x,
		    route search_path, sfsp2pstat status) 
{
  if (status) {
    warnx << "lookup_findsucc_cb for " << n << " returned " <<
      status << "\n";
    if (status == SFSP2P_RPCFAILURE)
      dofindsucc (n, cb);		// try again; it should terminate
    else  
      cb (x, search_path, SFSP2P_ERRNOENT);
  } else {
    //    warn << "succ was " << x << "\n";
    cb (x, search_path, SFSP2P_OK);
  }
}
