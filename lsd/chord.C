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

void p2p::updateall (sfs_ID &x)
{
  for (int i = 1; i <= NBIT; i++) {
    if (between (finger_table[i].start, finger_table[i].first, x)) {
      finger_table[i].first = x;
    }
  }
  if (between (predecessor, myID, x)) {
    predecessor = x;
  }
}

sfs_ID p2p::findclosestpred (sfs_ID &x)
{
  sfs_ID p = myID;
  for (int i = NBIT; i >= 0; i--) {
    if ((finger_table[i].alive) && 
	between (myID, x, finger_table[i].first)) {
      p = finger_table[i].first;
      break;
    }
  }
  // warnx << "findclosestpred of " << fa->x << " is " << p << "\n";
  return p;
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

sfs_ID
p2p::query_location_table (sfs_ID x) {
  location *l = locations.first ();
  sfs_ID min = bigint(1) << 160;
  sfs_ID ret = -1;
  while (l) {
    sfs_ID d = diff(l->n, x);
    if (d < min) { min = d; ret = l->n; }
    l = locations.next (l);
  }
  return ret;
}

void
p2p::set_closeloc (finger &w)
{
  sfs_ID n;
  if (!lookup_closeloc (w.first, &n)) {
    fatal ("No nodes left; cannot recover");
  }
  // warnx << "set_closeloc: replace " << w.first << " with " << n << "\n";
  w.first = n;
  w.alive = true;
}


void
p2p::updateloc (sfs_ID &x, net_address &r, sfs_ID &source)
{
  if (locations[x] == NULL) {
     warnx << "updateloc: add " << x << " at port " << r.port << " source: " 
    	  << source << "\n";
    location *loc = New location (x, r, source);
    locations.insert (loc);
    doActionCallbacks(x, ACT_NODE_JOIN);
  } else {
    // warnx << "update: " << x << " at port " << r.port << " source "
    //	  << source << "\n";
    locations[x]->addr = r;
    locations[x]->source = source;
  }
}

void
p2p::deleteloc (sfs_ID &n)
{
  warnx << "deleteloc: " << n << "\n";
  assert (n != myID);
  location *l = locations[n];
  if (l) {
    // if (l->alive && (l->source != n))
    //    alert (l->source, n);
    l->alive = false;
    locations.remove (l);
  }
  doActionCallbacks(n, ACT_NODE_LEAVE);
    
}


static void
finger_print (finger &w)
{
  warnx << w.start << " alive? " << w.alive << " finger :";
  warnx << "  " << w.first << "\n";
}

void
p2p::print ()
{
  for (int i = 1; i <= NBIT; i++) {
#if 0
    finger_print (finger_table[i]);
#else
    if (finger_table[i].first != finger_table[i-1].first) {
      warnx << "succ " << i << ": ";
      finger_print (finger_table[i]);
    }
#endif
  }
  warnx << "pred : " << predecessor << "\n";
#if 0
  for (int i = 1; i <= NBIT; i++) {
    if (immediate[i] != immediate[i-1]) {
      warnx << "imm succ " << i << ": " << immediate[i];
    }
  }
  warnx << "\n";
#endif
}

p2p::~p2p()
{
}

p2p::p2p (str host, int hostport, const sfs_ID &hostID,
	  int port, str myhost, const sfs_ID &ID) :
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

  bzero(&stats, sizeof(chord_stats));
  stats.balance = New qhash<sfs_ID, int, hashID> ();

  sigcb(SIGUSR1, wrap (this, &p2p::stats_cb));

  
  wellknownhost.hostname = host;
  wellknownhost.port = hostport;
  myaddress.port = port;
  myaddress.hostname = myhost;

  finger_table[0].start = finger_table[0].first = myID;
  finger_table[0].alive = true;
  warnx << "myname is " << myaddress.hostname << "\n";
  warnx << "myID is " << myID << "\n";
  warnx << "myport is " << myaddress.port << "\n";
  warnx << "wellknowID is " << wellknownID << "\n";
  for (int i = 1; i <= NBIT; i++) {
    finger_table[i].start = successorID(myID, i-1);
    finger_table[i].first = myID;
    finger_table[i].alive = true;
  }
  predecessor = myID;

  location *l = New location (wellknownID, wellknownhost.hostname, 
			      wellknownhost.port, myID);
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

  warnt("CHORD: stabilize");
  warnx << "stabilize " << i << "\n";
  get_predecessor (finger_table[1].first, 
		   wrap (mkref (this), &p2p::stabilize_getpred_cb));
  if (i > 1) {
    find_successor (myID, finger_table[i].start,
			  wrap (mkref (this), &p2p::stabilize_findsucc_cb, i));
  }
  int time = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
  stabilize_tmo = delaycb (time, wrap (mkref (this), &p2p::stabilize, i+1));
}

void
p2p::stabilize_getpred_cb (sfs_ID p, net_address r, sfsp2pstat status)
{
  // receive predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << finger_table[1].first << " failure " 
	  << status << "\n";
  } else {
    if ((finger_table[1].first == myID) ||
	between (myID, finger_table[1].first, p)) {
      warnx << "stabilize_pred_cb: new successor is " << p << "\n";
      finger_table[1].first = p;
      updateall (p);
      print ();
    }
    notify (finger_table[1].first, myID);
  }
}

void
p2p::stabilize_findsucc_cb (int i, sfs_ID s, route search_path, 
			    sfsp2pstat status)
{
  if (status) {
    warnx << "stabilize_findsucc_cb: " << finger_table[i].first << " failure " 
	  << status << "\n";
  } else {
    if (finger_table[i].first != s) {
      warnx << "stabilize_findsucc_cb: new successor of " << 
	finger_table[i].start << " is " << s << "\n";
      finger_table[i].first = s;
      updateall (s);
      print ();
    }
  }
}

void
p2p::join ()
{
  sfs_ID n;

  if (!lookup_anyloc(myID, &n))
    fatal ("No nodes left to join\n");
  
  find_successor (n, myID, wrap (mkref (this), &p2p::join_getsucc_cb));
}

void 
p2p::join_getsucc_cb (sfs_ID s, route r, sfsp2pstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << status << "\n";
    join ();  // try again
  } else {
    warnx << "join_getsucc_cb: " << s << "\n";   
    finger_table[1].first = s;
    updateall (s);
    print ();
  }
}

void
p2p::notify (sfs_ID &n, sfs_ID &x)
{
  ptr<sfsp2p_notifyarg> na = New refcounted<sfsp2p_notifyarg>;
  sfsp2pstat *res = New sfsp2pstat;

  // warnx << "notify " << n << " about " << x << "\n";
  location *l = locations[x];
  assert (l);
  na->x = x;
  na->r = l->addr;
  doRPC (n, sfsp2p_program_1, SFSP2PPROC_NOTIFY, na, res, 
	 wrap (mkref (this), &p2p::notify_cb, res));
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
  ptr<sfsp2p_notifyarg> na = New refcounted<sfsp2p_notifyarg>;
  sfsp2pstat *res = New sfsp2pstat;

  warnx << "alert: " << x << " died; notify " << n << "\n";
  //  location *l = locations[x];
  // assert (l);
  na->x = x;
  na->r = myaddress;
  // na->r = l->addr;
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
  warnt("CHORD: doget_successor_reply");
}

void
p2p::doget_predecessor (svccb *sbp)
{
  sfs_ID p = predecessor;
  sfsp2p_findres res(SFSP2P_OK);
  location *l = locations[p];
  assert (l);
  res.resok->node = p;
  res.resok->r = l->addr;
  sbp->reply (&res);
}

void
p2p::dotestandfind (svccb *sbp, sfsp2p_testandfindarg *fa) 
{
  sfs_ID x = fa->x;

  sfsp2p_testandfindres *res;
  

  if (betweenrightincl(myID, finger_table[1].first, x) ) {

    res = New sfsp2p_testandfindres (SFSP2P_INRANGE);
    warnt("CHORD: testandfind_inrangereply");
    res->inres->succ = finger_table[1].first;
    location *l = locations[finger_table[1].first];
    assert (l);
    res->inres->r = l->addr;
    sbp->reply(res);
    delete res;
  } else {
    res = New sfsp2p_testandfindres (SFSP2P_NOTINRANGE);
    sfs_ID p = findclosestpred (fa->x);
    location *l = locations[p];
    assert (l);
    res->findres->x = fa->x;
    res->findres->node = p;
    res->findres->r = l->addr;
    warnt("CHORD: testandfind_notinrangereply");
    sbp->reply(res);
    delete res;
  }
  
}

void
p2p::dofindclosestpred (svccb *sbp, sfsp2p_findarg *fa)
{
  sfsp2p_findres res(SFSP2P_OK);
  sfs_ID p = findclosestpred (fa->x);
  location *l = locations[p];
  assert (l);
  res.resok->x = fa->x;
  res.resok->node = p;
  res.resok->r = l->addr;
  
  warnt("CHORD: dofindclosestpred_reply");
  sbp->reply (&res);
}

void
p2p::donotify (svccb *sbp, sfsp2p_notifyarg *na)
{
  warnt("CHORD: donotify");
  updateloc (na->x, na->r, na->x);
  if ((predecessor == myID) || between (predecessor, myID, na->x)) {
    warnx << "donotify: updated predecessor: new pred is " << na->x << "\n";
    predecessor = na->x;
    updateall (predecessor);
    print ();
  }
  sbp->replyref (sfsp2pstat (SFSP2P_OK));
}

void
p2p::doalert (svccb *sbp, sfsp2p_notifyarg *na)
{
  warnt("CHORD: doalert");

  // perhaps less aggressive and check status of x first
  deleteloc (na->x);
  // bootstrap ();
  sbp->replyref (sfsp2pstat (SFSP2P_OK));
}

void
p2p::dofindsucc (sfs_ID &n, cbroute_t cb)
{
  find_successor (myID, n, wrap (mkref (this), &p2p::dofindsucc_cb, cb, n));
}

void
p2p::dofindsucc_cb (cbroute_t cb, sfs_ID n, sfs_ID x,
		    route search_path, sfsp2pstat status) 
{
  if (status) {
    warnx << "dofindsucc_cb for " << n << " returned " <<  status << "\n";
    if (status == SFSP2P_RPCFAILURE) {
      warnx << "dofindsucc_cb: try to recover\n";
      sfs_ID last = search_path.pop_back ();
      sfs_ID lastOK = search_path.back ();
      warnx << "dofindsucc_cb: last node " << last << " contacted failed\n";
      alert (lastOK, last);
      find_successor_restart (lastOK, x, search_path,
			      wrap (mkref (this), &p2p::dofindsucc_cb, cb, n));
    } else {
      cb (x, search_path, SFSP2P_ERRNOENT);
    }
  } else {
    // warnx << "dofindsucc_cb: " << x << "\n";
    cb (x, search_path, SFSP2P_OK);
  }
}


