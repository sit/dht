#include <assert.h>
#include "sfsp2p.h"
#include <qhash.h>
#include <sys/time.h>

#define MAX_INT 0x7fffffff

static
str gettime()
{
  str buf ("");
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  buf = strbuf (" %d.%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
  return buf;
}

static
int uniform_random(double a, double b)
{
  double f;
  int c = random();

  if (c == MAX_INT) c--;
  f = (b - a)*((double)c)/((double)MAX_INT);

  return (int)(a + f);
}


static sfs_ID
incID (sfs_ID &n)
{
  sfs_ID s = n + 1;
  sfs_ID b (1);
  b = b << NBIT;
  if (s >= b)
    return s - b;
  else
    return s;
}

static sfs_ID
decID (sfs_ID &n)
{
  sfs_ID p = n - 1;
  sfs_ID b (1);
  b = b << NBIT;
  if (p < 0)
    return p + b;
  else
    return p;
}

static sfs_ID
successorID (sfs_ID &n, int p)
{
  sfs_ID s;
  sfs_ID t (1);
  sfs_ID b (1);
  
  b = b << NBIT;
  s = n;
  sfs_ID t1 = t << p;
  s = s + t1;
  if (s >= b)
    s = s - b;
  return s;
}

static sfs_ID
predecessorID (sfs_ID &n, int p)
{
  sfs_ID s;
  sfs_ID t (1);
  sfs_ID b (1);
  
  b = b << NBIT;
  s = n;
  sfs_ID t1 = t << p;
  s = s - t1;
  if (s < 0)
    s = s + b;
  return s;
}

// XXX use operator overloading?
static bool
gt_or_eq (sfs_ID &n, sfs_ID &n1)
{
  sfs_ID b (1);
  b = (b << NBIT);
  if (((n - n1) >= 0) && ((n - n1) < b))
    return true;
  if (((n - n1) <= 0) && ((n1 - n) > b))
    return true;
  return false;
}

static bool
gt (sfs_ID &n, sfs_ID &n1)
{
  sfs_ID b (1);
  b = (b << NBIT);
  if (((n - n1) > 0) && ((n - n1) < b))
    return true;
  if (((n - n1) < 0) && ((n1 - n) > b))
    return true;
  return false;
}

static bool
between (sfs_ID &a, sfs_ID &b, sfs_ID &n)
{
  bool r;
  bool f = gt_or_eq (b, a);
  if ((!f && (gt_or_eq (b, n) || gt_or_eq (n, a)))
      || (f && gt_or_eq (b, n) && gt_or_eq (n, a)))
    r = true;
  else
    r = false;
  // warnx << n << " between( " << a << ", " <<  b << "): " <<  r << "\n";
  return r;
}

bool
p2p::updatepred (wedge &w, sfs_ID &x, route &r)
{
  if (w.first == x)
    return false;

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
p2p::updatesucc (wedge &w, sfs_ID &x, route &r)
{
  if (w.first == x)
    return false;

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
p2p::noticesucc (int k, sfs_ID &x, route &r)
{
  bool t = false;
  for (int i = k; i <= NBIT; i++) {
    if (updatesucc (successor[i], x, r))
      t = true;
  }
  return t;
}

bool
p2p::noticepred (int k, sfs_ID &x, route &r)
{
  bool t = false;
  for (int i = k; i <= NBIT; i++) {
    if (updatepred (predecessor[i], x, r))
      t = true;
  }
  return t;
}

bool
p2p::notice (int k, sfs_ID &x, route &r)
{
  bool t = false;
  if (noticesucc (k, x, r))
    t = true;
  if (noticepred (k, x, r))
    t = true;
  return t;
}


int
p2p::successor_wedge (sfs_ID &n)
{
  for (int i = 0; i <= NBIT; i++) {
    if (between (successor[i].start, successor[i].end, n))
      return i;
  }
  assert (0);
  return -1;
}

int
p2p::predecessor_wedge (sfs_ID &n)
{
  for (int i = 0; i <= NBIT; i++) {
    if (between (predecessor[i].start, predecessor[i].end, n))
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
    warnx << l->n << "\n";
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
}


void
p2p::updateloc (sfs_ID &x, route &r, sfs_ID &source)
{
  if (locations[x] == NULL) {
    // warnx << "add: " << x << " at port " << r.port << " source: " 
    //	  << source << "\n";
    location *loc = New location (x, r, source);
    locations.insert (loc);
  } else {
    // warnx << "update: " << x << " at port " << r.port << " source "
    //	  << source << "\n";
    locations[x]->r = r;
    locations[x]->source = source;
  }
}

void
p2p::deleteloc (sfs_ID &n)
{
  // warnx << "deleteloc: " << n << "\n";
  assert (n != myID);
  for (int i = 0; i <= NBIT; i++) {
    if (predecessor[i].first == n)
      predecessor[i].alive = false;
    if (successor[i].first == n)
      successor[i].alive = false;
  }
  location *l = locations[n];
  if (l) {
    if (l->alive && (l->source != n))
      alert (l->source, n);
    l->alive = false;
    locations.remove (l);
  }
}

void
p2p::connect_cb (location *l, int fd)
{
  if (fd < 0) {
    warnx << "connect_cb: connect failed\n";
    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      aclnt_cb cb = st->cb;
      (*cb) (RPC_FAILED);
      delete st;
    }
    l->connecting = false;
  } else {
    // warnx << "connect_cb: succeeded\n";
    assert (l->alive);
    ptr<aclnt> c = aclnt::alloc (axprt_stream::alloc (fd), sfsp2p_program_1);
    l->c = c;
    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      c->call (st->procno, st->in, st->out, st->cb);
    }
    l->connecting = false;
  }
}

void
p2p::timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, clnt_stat err) 
{
  struct timeval now;
  gettimeofday(&now, NULL);
  l->total_latency += (now.tv_sec - start->tv_sec)*1000000 + (now.tv_usec - start->tv_usec);
  l->num_latencies++;
  (*cb)(err);
}

void
p2p::doRPC (sfs_ID &ID, int procno, const void *in, void *out,
		      aclnt_cb cb)
{

  location *l = locations[ID];
  assert (l);
  assert (l->alive);
  if (l->c) {
    ptr<struct timeval> start = new refcounted<struct timeval>();
    gettimeofday(start, NULL);
    l->c->call (procno, in, out, wrap(this, &p2p::timing_cb, cb, l, start));
  } else {
    // If we are in the process of connecting; we should wait
    doRPC_cbstate *st = New doRPC_cbstate (procno, in, out, cb);
    l->connectlist.insert_tail (st);
    if (!l->connecting) {
      l->connecting = true;
      // warnx << "connect to " << l->r.server << " port " << l->r.port << "\n";
      tcpconnect (l->r.server, l->r.port, wrap (mkref (this), &p2p::connect_cb,
						l));
    }
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
    wedge_print (successor[i]);
  }
  for (int i = 0; i <= NBIT; i++) {
    warnx << "pred " << i << ": ";
    wedge_print (predecessor[i]);
  }
}

p2p::p2p (str host, int hostport, const sfs_ID &hostID,
	  int port, const sfs_ID &ID) :
  wellknownhost (host), wellknownport (hostport), wellknownID (hostID),
  myport (port), myID (ID)
{
  namemyID = myname ();
  successor[0].start = successor[0].end = successor[0].first = myID;
  successor[0].alive = true;
  warnx << "namemyID " << namemyID << "\n";
  warnx << "myID is " << myID << "\n";
  warnx << "myport is " << myport << "\n";
  for (int i = 1; i <= NBIT; i++) {
    successor[i].start = successorID(myID, i-1);
    successor[i].end = successorID(myID, i);
    successor[i].end = decID (successor[i].end);
    successor[i].first = myID;
    successor[i].alive = true;
    warnx << "succ " << i << ": ";
    wedge_print (successor[i]);
  }
  predecessor[0].start = predecessor[0].end = predecessor[0].first = myID;
  predecessor[0].alive = true;
  for (int i = 1; i <= NBIT; i++) {
    predecessor[i].end = predecessorID (myID, i-1);
    predecessor[i].start = predecessorID (myID, i);
    predecessor[i].start = incID (predecessor[i].start);
    predecessor[i].first = myID;
    predecessor[i].alive = true;
    warnx << "pred " << i << ": ";
    wedge_print (predecessor[i]);
  }
  location *l = New location (wellknownID, wellknownhost, wellknownport,
			      myID);
  locations.insert (l);
  if (myID == wellknownID) {
    warnx << "bootstrap server\n";
  } else {
    warnx << namemyID << " collect from " << wellknownhost << "\n";
    l = New location (myID, namemyID, myport, myID);
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

  warnx << "stabilize " << i << "\n";

  if (!predecessor[1].alive) stable = false;
  else get_predecessor (predecessor[1].first, 
		   wrap (mkref (this), &p2p::stabilize_getsucc_cb));
  if (!successor[1].alive) stable = false;
  else get_predecessor (successor[1].first,
		     wrap (mkref (this), &p2p::stabilize_getpred_cb));

  if (i > 1) {
    if (!successor[i].alive) stable = false;
    else lookup_closestsucc (successor[i].first, successor[i].start,
			  wrap (mkref (this), &p2p::stabilize_findsucc_cb, i));

    if (!predecessor[i].alive) stable = false;
    else lookup_closestpred (predecessor[i].first, predecessor[i].end,
			  wrap (mkref (this), &p2p::stabilize_findpred_cb, i));
  }
  int time = uniform_random (0.5 * stabilize_timer, 1.5 * stabilize_timer);
  warnx << "stabilize in " << time << " seconds\n";
  stabilize_tmo = delaycb (time, 
			   wrap (mkref (this), &p2p::stabilize, i+1));
  if (!stable)
    bootstrap ();
}

void
p2p::stabilize_getsucc_cb (sfs_ID s, route r, sfsp2pstat status)
{
  // receive first successor from my predecessor; in stable case it is me
  if (status) {
    warnx << "stabilize_getsucc_cb: " << predecessor[1].first << " failure " 
	  << status << "\n";
    bootstrap ();
  } else {
    // warnx << "stabilize_getsucc_cb from " << predecessor[1].first 
    //  << " : " << s << "\n";
    if (updatepred (predecessor[1], s, r)) {
      // print ();
      bootstrap ();
    }
  }
}

void
p2p::stabilize_getpred_cb (sfs_ID p, route r, sfsp2pstat status)
{
  // receive first predecessor from my successor; in stable case it is me
  if (status) {
    warnx << "stabilize_getpred_cb: " << successor[1].first << " failure " 
	  << status << "\n";
    bootstrap ();
  } else {
    // warnx << "stabilize_getpred_cb from " << successor[1].first << " : " 
    //  << p << "\n";
    if (updatesucc (successor[1], p, r)) {
      // print ();
      bootstrap ();
    }
  }
}

void
p2p::stabilize_findsucc_cb (int i, sfs_ID s, route r, sfsp2pstat status)
{
  if (status) {
    warnx << "stabilize_findsucc_cb: " << successor[i].first << " failure " 
	  << status << "\n";
    bootstrap ();
  } else {
    // warnx << "stabilize_findsucc_cb from " << successor[i].first 
    //  << " : " << s << "\n";
    if (updatesucc (successor[i], s, r)) {
      // print ();
      bootstrap ();
    }
  }
}

void
p2p::stabilize_findpred_cb (int i, sfs_ID p, route r, sfsp2pstat status)
{
  if (status) {
    warnx << "stabilize_findpred_cb: " << status << "\n";
    bootstrap ();
  } else {
    // warnx << "stabilize_findpred_cb from " << predecessor[i].first 
    //  << " : " << p << "\n";
    if (updatepred (predecessor[i], p, r)) {
      // print ();
      bootstrap ();
    }
  }
}

void
p2p::join ()
{
  sfs_ID n;

  warnx << "join at " << gettime () << "\n";

  if (!lookup_anyloc(myID, &n))
    fatal ("No nodes left to join\n");
  
  find_predecessor (n, myID, wrap (mkref (this), 
					     &p2p::join_findpred_cb));
}

void
p2p::join_findpred_cb (sfs_ID p, route r, sfsp2pstat status)
{
  if (status) {
    warnx << "join_findpred_cb: failed with " << status << "\n";
    join ();  // try again
  } else {
    warnx << "join_findpred_cb: " << p << "\n";
    notice (1, p, r);
    get_successor (p, wrap (mkref (this), &p2p::join_getsucc_cb, p));
  }
}

void 
p2p::join_getsucc_cb (sfs_ID p, sfs_ID s, route r, sfsp2pstat status)
{
  if (status) {
    warnx << "join_getsucc_cb: " << status << "\n";
    join ();  // try again
  } else {
    // warnx << "join_getsucc_cb: " << p << " " << s << "\n";
    if (between (p, s, myID) || (p == s)) {
      // we found the first successor of myID: s. if p == s, then there is 
      // only one other node in the system; that node is my successor and 
      // predecessor.
      warnx << "join_getsucc_cb: succ is " << s << "\n";
      notice (1, s, r);
      move ();
      if (successor[1].alive) notify (successor[1].first, myID);
      if (predecessor[1].alive) notify (predecessor[1].first, myID);
      bootstrap ();
    } else {
      get_successor (s, wrap (mkref (this), &p2p::join_getsucc_cb, s));
    }
  }
}

void
p2p::move ()
{
  sfsp2p_movearg *ma = New sfsp2p_movearg;
  sfsp2p_moveres *res = New sfsp2p_moveres (SFSP2P_OK);

  ma->x = myID;
  doRPC (successor[1].first, SFSP2PPROC_MOVE, ma, res, 
	 wrap (mkref (this), &p2p::move_cb, res));
}

void
p2p::move_cb (sfsp2p_moveres *res, clnt_stat err)
{
  if (err) {
    warnx << "move_cb: RPC failure " << err << "\n";
  } else if (res->status != SFSP2P_OK) {
    warnx << "move_cb: RPC error" << res->status << "\n";
  } else {
    for (unsigned int i = 0; i < res->resok->mappings.size (); i++) {
      attribute *a = New attribute (res->resok->mappings[i].x, 
			 res->resok->mappings[i].r);
      warnx << "move_cb: insert " << res->resok->mappings[i].x << "\n";
      attributes.insert (a);
    }
  }
}

void
p2p::bootstrap ()
{
  warnx << "bootstrap\n";
  if (nbootstrap > 0) {
    warnx << "bootstrap: we are busy bootstrapping\n";
    return;
  }
  // print ();
  nbootstrap = NBIT * 2;
  bootstrap_failure = false;
  stable = true;
  for (int i = 1; i <= NBIT; i++) {
    if (!successor[i].alive) {
      set_closeloc (successor[i]);
    }
    find_successor (successor[i].first, successor[i].start, 
		    wrap (mkref (this), &p2p::bootstrap_succ_cb, i, 
			  successor[i].first));
    if (!predecessor[i].alive) {
      set_closeloc (predecessor[i]);
    }
    find_predecessor (predecessor[i].first, predecessor[i].end, 
		      wrap (mkref (this), &p2p::bootstrap_pred_cb, i,
			    predecessor[i].first));
  }
}

void
p2p::bootstrap_done ()
{
  warnx << "bootstrap_done: stable? " << stable << " at " << gettime ()
	<< " failures? " << 
    bootstrap_failure << "\n";

  if (!successor[1].alive) stable = false;
  else notify (successor[1].first, myID);
  if (!predecessor[1].alive) stable = false;
  else notify (predecessor[1].first, myID);

  if (!stable) 
    bootstrap ();
  else
    print ();
}

void
p2p::bootstrap_succ_cb (int i, sfs_ID n, sfs_ID s, route r, sfsp2pstat status)
{
  nbootstrap--;
  if (status) {
    warnx << "bootstrap_succ_cb: " << status << ": dead : " << n << "\n";
    bootstrap_failure = true;
  } else {
    // warnx << "bootstrap_succ_cb: " << i << " is " << n 
    //  << " propose: " << s << "\n";
    if (updatesucc (successor[i], s, r)) {
      // warnx << "bootstrap_succ_cb: updated\n";
      stable = false;
    }
    if (nbootstrap <= 0)
      bootstrap_done ();
  }
}

void
p2p::bootstrap_pred_cb (int i, sfs_ID n, sfs_ID p, route r, sfsp2pstat status)
{
  nbootstrap--;
  if (status) {
    warnx << "bootstrap_pred_cb: " << status << ": dead " << n << "\n";
    bootstrap_failure = true;
  } else {
    //warnx << "bootstrap_pred_cb: " << i << " is " << n << " propose: " 
    //	  << p << "\n";
    if (updatepred (predecessor[i], p, r)) {
      warnx << "bootstrap_pred_cb: updated\n";
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
  na->r = l->r;
  doRPC (n, SFSP2PPROC_NOTIFY, na, res, wrap (mkref (this), 
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
  na->r = l->r;
  doRPC (n, SFSP2PPROC_ALERT, na, res, wrap (mkref (this), 
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
p2p::insert (svccb *sbp,  sfsp2p_insertarg *ia)
{
  int i = successor_wedge (ia->d);
  find_successor (successor[i].first, ia->d,
		  wrap (mkref (this), &p2p::insert_findsucc_cb, sbp, ia));
}

void
p2p::insert_findsucc_cb (svccb *sbp, sfsp2p_insertarg *ia, sfs_ID x, route r,
			 sfsp2pstat status) 
{
  if (status) {
    warnx << "insert_findsucc_cb for " << ia->d << " returned " <<
      status << "\n";
    if (status == SFSP2P_RPCFAILURE)
      insert (sbp, ia);		// try again; it should terminate
    else 
      sbp->replyref (sfsp2pstat (status));
  } else {
    // warnx << "insert_findsucc_cb: do insert of " << ia->d << " at " << x 
    //  << "\n";
    sfsp2pstat *res = New sfsp2pstat;
    doRPC (x, SFSP2PPROC_INSERT, ia, res, wrap (mkref (this), 
					      &p2p::insert_cb, sbp, res));
  }
}

void
p2p::insert_cb (svccb *sbp, sfsp2pstat *res, clnt_stat err)
{
  if (err) {
    sbp->replyref (sfsp2pstat (SFSP2P_RPCFAILURE));
  } else if (*res) {
    sbp->replyref (*res);
  } else {
    sbp->replyref (sfsp2pstat (SFSP2P_OK));
  }
}

void
p2p::lookup (svccb *sbp, sfs_ID &n)
{
  int i = successor_wedge (n);
  if (!predecessor[i].alive) {
    set_closeloc (predecessor[i]);
  }
  find_successor (successor[i].first, n,
		  wrap (mkref (this), &p2p::lookup_findsucc_cb, sbp, n));
}

void
p2p::lookup_findsucc_cb (svccb *sbp, sfs_ID n, sfs_ID x, route r,
			 sfsp2pstat status) 
{
  if (status) {
    warnx << "lookup_findsucc_cb for " << n << " returned " <<
      status << "\n";
    if (status == SFSP2P_RPCFAILURE)
      lookup (sbp, n);		// try again; it should terminate
    else 
      sbp->replyref (sfsp2pstat (status));
  } else {
    // warnx << "lookup_findsucc_cb: do lookup at " << x << "\n";
    sfsp2p_lookupres *res = New sfsp2p_lookupres (SFSP2P_OK);
    doRPC (x, SFSP2PPROC_LOOKUP, &n, res, wrap (mkref (this), 
					      &p2p::lookup_cb, sbp, res));
  }
}

void
p2p::lookup_cb (svccb *sbp, sfsp2p_lookupres *res, clnt_stat err)
{
  if (err) {
    sbp->replyref (sfsp2pstat (SFSP2P_RPCFAILURE));
  } else if (res->status) {
    sbp->replyref (res->status);
  } else {
    sbp->reply (res);
  }
}


void 
p2p::get_successor (sfs_ID n, cbsfsID_t cb)
{
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  doRPC (n, SFSP2PPROC_GETSUCCESSOR, NULL, res,
	 wrap (mkref (this), &p2p::get_successor_cb, n, cb, res));
}

void
p2p::get_successor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
		       clnt_stat err) 
{
  if (err) {
    route dr;
    warnx << "get_successor_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    route dr;
    warnx << "get_successor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

void 
p2p::get_predecessor (sfs_ID n, cbsfsID_t cb)
{
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  doRPC (n, SFSP2PPROC_GETPREDECESSOR, NULL, res,
	 wrap (mkref (this), &p2p::get_predecessor_cb, n, cb, res));
}

void
p2p::get_predecessor_cb (sfs_ID n, cbsfsID_t cb, sfsp2p_findres *res, 
		       clnt_stat err) 
{
  if (err) {
    route dr;
    warnx << "get_predecessor_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    route dr;
    warnx << "get_predecessor_cb: RPC error " << res->status << "\n";
    cb (n, dr, res->status);
  } else {
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

void
p2p::find_successor (sfs_ID &n, sfs_ID &x, cbsfsID_t cb)
{
  lookup_closestsucc (n, x, cb);
}

void 
p2p::lookup_closestsucc (sfs_ID &n, sfs_ID &x, cbsfsID_t cb)
{
  sfsp2p_findarg *fap = New sfsp2p_findarg;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;
  doRPC (n, SFSP2PPROC_FINDCLOSESTSUCC, fap, res,
	 wrap (mkref (this), &p2p::lookup_closestsucc_cb, n, cb, res));
}

void
p2p::lookup_closestsucc_cb (sfs_ID n, cbsfsID_t cb, 
			   sfsp2p_findres *res, clnt_stat err)
{
  if (err) {
    route dr;
    warnx << "find_closestsucc_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    route dr;
    warnx << "find_closestsucc_cb: RPC error" << res->status << "\n";
    cb (n, dr, res->status);
  } else if (n != res->resok->node) {
    //warnx << "find_closestsucc_cb of " << res->resok->x
    //  << " from " << n << " returns " << res->resok->node << "\n";
    updateloc (res->resok->node, res->resok->r, n);
    lookup_closestsucc (res->resok->node, res->resok->x, cb);
  } else {
    //warnx << "find_closestsucc_cb of " << res->resok->x 
    //  << " from " << n << " returns " << res->resok->node << "\n";
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

void
p2p::find_predecessor (sfs_ID &n, sfs_ID &x, cbsfsID_t cb)
{
  lookup_closestpred (n, x, cb);
}

void
p2p::lookup_closestpred (sfs_ID &n, sfs_ID &x, cbsfsID_t cb)
{
  sfsp2p_findarg *fap = New sfsp2p_findarg;
  sfsp2p_findres *res = New sfsp2p_findres (SFSP2P_OK);
  fap->x = x;
  doRPC (n, SFSP2PPROC_FINDCLOSESTPRED, fap, res,
	 wrap (mkref (this), &p2p::lookup_closestpred_cb, n, cb, res));
}

void
p2p::lookup_closestpred_cb (sfs_ID n, cbsfsID_t cb, 
			   sfsp2p_findres *res, clnt_stat err)
{
  if (err) {
    route dr;
    warnx << "find_closestpred_cb: RPC failure " << err << "\n";
    deleteloc (n);
    cb (n, dr, SFSP2P_RPCFAILURE);
  } else if (res->status) {
    route dr;
    warnx << "find_closestpred_cb: RPC error" << res->status << "\n";
    cb (n, dr, res->status);
  } else if (n != res->resok->node) {
    // warnx << "find_closestpred_cb of " << res->resok->x 
    //  << " from " << n << " returns " << res->resok->node << "\n";
    updateloc (res->resok->node, res->resok->r, n);
    lookup_closestpred (res->resok->node, res->resok->x, cb);
  } else {
    // warnx << "find_closestpred_cb of " << res->resok->x 
    //  << " from " << n << " returns " << res->resok->node << "\n";
    updateloc (res->resok->node, res->resok->r, n);
    cb (res->resok->node, res->resok->r, SFSP2P_OK);
  }
}

void
p2p::doget_successor (svccb *sbp)
{
  if (successor[1].alive) {
    sfs_ID s = successor[1].first;
    sfsp2p_findres res(SFSP2P_OK);
    location *l = locations[s];
    assert (l);
    res.resok->node = s;
    res.resok->r = l->r;
    sbp->reply (&res);
  } else {
    sbp->replyref (sfsp2pstat (SFSP2P_ERRNOENT));
  }
}

void
p2p::doget_predecessor (svccb *sbp)
{
  if (predecessor[1].alive) {
    sfs_ID p = predecessor[1].first;
    sfsp2p_findres res(SFSP2P_OK);
    location *l = locations[p];
    assert (l);
    res.resok->node = p;
    res.resok->r = l->r;
    sbp->reply (&res);
  } else {
    sbp->replyref (sfsp2pstat (SFSP2P_ERRNOENT));
  }
}

void
p2p::dofindclosestsucc (svccb *sbp, sfsp2p_findarg *fa)
{
  sfsp2p_findres res(SFSP2P_OK);
  sfs_ID s = myID;

  for (int i = 0; i <= NBIT; i++) {
    if ((predecessor[i].alive) && between (fa->x, s, predecessor[i].first)) {
      s = predecessor[i].first;
    }
  }
  // warnx << "dofindclosestsucc of " << fa->x << " is " << s << "\n";
  location *l = locations[s];
  assert (l);
  res.resok->x = fa->x;
  res.resok->node = s;
  res.resok->r = l->r;
  sbp->reply (&res);
}

void
p2p::dofindclosestpred (svccb *sbp, sfsp2p_findarg *fa)
{
  sfsp2p_findres res(SFSP2P_OK);
  sfs_ID p = myID;

  for (int i = 0; i <= NBIT; i++) {
    if ((successor[i].alive) && between (p, fa->x, successor[i].first)) {
      p = successor[i].first;
    }
  }
  // warnx << "dofindclosestpred of " << fa->x << " is " << p << "\n";
  location *l = locations[p];
  assert (l);
  res.resok->x = fa->x;
  res.resok->node = p;
  res.resok->r = l->r;
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
p2p::domove (svccb *sbp, sfsp2p_movearg *ma)
{
  sfsp2p_moveres res (SFSP2P_OK);

  int n = 0;
  for (attribute *a = attributes.first (); a != NULL; a = attributes.next (a)) {
    if (between (myID, ma->x, a->n)) {
      n++;
    }
  }
  res.resok->mappings.setsize (n);
  int i = 0;
  for (attribute *a = attributes.first (); a != NULL; a = attributes.next (a)) {
    if (between (myID, ma->x, a->n)) {
      res.resok->mappings[i].x = a->n;
      res.resok->mappings[i].r = a->r;
      i++;
      attributes.remove (a);
    }
  }
  warnx << "moving " << n << " key/values to " << ma->x << "\n";
  sbp->reply (&res);
}

void
p2p::doinsert (svccb *sbp, sfsp2p_insertarg *ia)
{
  warnx << "doinsert: " << ia->d << " with " << ia->r.server << "\n";

  attribute *a = attributes[ia->d];
  if (a) {
    sbp->replyref (sfsp2pstat (SFSP2P_ERREXIST));
  } else {
    a = New attribute (ia->d, ia->r);
    attributes.insert (a);
    sbp->replyref (sfsp2pstat (SFSP2P_OK));
  }
}

void
p2p::dolookup (svccb *sbp, sfs_ID *n)
{
  // warnx << "dolookup for " << *n << ": ";
  attribute *a = attributes[*n];
  if (a) {
    // warnx << a->r.server << "\n";
    sfsp2p_lookupres res = (SFSP2P_OK);
    res.resok->r = a->r;
    sbp->reply (&res);
  } else {
    warnx << "no entry\n";
    sbp->replyref (sfsp2pstat (SFSP2P_ERRNOENT));
  }
}

client::client (ptr<axprt_stream> x)
{
  // we are calling this too often
  p2psrv = asrv::alloc (x, sfsp2p_program_1,  wrap (this, &client::dispatch));

}

void
client::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case SFSP2PPROC_NULL:
    sbp->reply (NULL);
    break;
  case SFSP2PPROC_GETSUCCESSOR:
    defp2p->doget_successor (sbp);
    break;
  case SFSP2PPROC_GETPREDECESSOR:
    defp2p->doget_predecessor (sbp);
    break;
  case SFSP2PPROC_FINDCLOSESTSUCC:
    {
      sfsp2p_findarg *fa = sbp->template getarg<sfsp2p_findarg> ();
      defp2p->dofindclosestsucc (sbp, fa);
    }
    break;
  case SFSP2PPROC_FINDCLOSESTPRED:
    {
      sfsp2p_findarg *fa = sbp->template getarg<sfsp2p_findarg> ();
      defp2p->dofindclosestpred (sbp, fa);
    }
    break;
  case SFSP2PPROC_NOTIFY:
    {
      sfsp2p_notifyarg *na = sbp->template getarg<sfsp2p_notifyarg> ();
      defp2p->donotify (sbp, na);
    }
    break;
  case SFSP2PPROC_ALERT:
    {
      sfsp2p_notifyarg *na = sbp->template getarg<sfsp2p_notifyarg> ();
      defp2p->doalert (sbp, na);
    }
    break;
  case SFSP2PPROC_MOVE:
    {
      sfsp2p_movearg *ma = sbp->template getarg<sfsp2p_movearg> ();
      defp2p->domove (sbp, ma);
    }
    break;
  case SFSP2PPROC_INSERT:
    {
      sfsp2p_insertarg *ia = sbp->template getarg<sfsp2p_insertarg> ();
      defp2p->doinsert (sbp, ia);
    }
    break;
  case SFSP2PPROC_LOOKUP:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      defp2p->dolookup (sbp, n);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

sfsp2pclient::sfsp2pclient (ptr<axprt_stream> _x)
  : x (_x)
{
  p2pclntsrv = asrv::alloc (x, sfsp2pclnt_program_1,
			 wrap (this, &sfsp2pclient::dispatch));
}

void
sfsp2pclient::dispatch (svccb *sbp)
{
  if (!sbp) {
    delete this;
    return;
  }
  assert (defp2p);
  switch (sbp->proc ()) {
  case SFSP2PCLNTPROC_NULL:
    sbp->reply (NULL);
    return;
  case SFSP2PCLNTPROC_LOOKUP:
    {
      sfs_ID *n = sbp->template getarg<sfs_ID> ();
      defp2p->lookup (sbp, *n);
    } 
    break;
  case SFSP2PCLNTPROC_INSERT:
    {
      sfsp2p_insertarg *ia = sbp->template getarg<sfsp2p_insertarg> ();
      defp2p->insert (sbp, ia);
    }
    break;
  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

