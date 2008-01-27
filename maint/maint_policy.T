// -*-c++-*-
// vim: filetype=cpp  foldmethod=marker
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <db.h>
#include <arpc.h>

#include <chord_prot.h>
#include <comm.h>
#include <merkle.h>
#include <merkle_tree_bdb.h>

#include <id_utils.h>
#include <location.h>
#include <libadb.h>
#include <rpclib.h>

#include <maint_prot.h>
#include "maint_policy.h"

// {{{ Bandwidth tracker
// See comm.[Ch]
static inline rpcstats *
getstats (int progno, int procno)
{
  str key = strbuf ("%d:%d", progno, procno);
  rpcstats *stats = rpc_stats_tab[key];
  if (!stats) {
    stats = New rpcstats (key);
    rpc_stats_tab.insert (stats);
  }
  return stats;
}

static void
track_aclnt (aclnt_acct_t a)
{
  rpcstats *stats = getstats (a.progno, a.procno);
  switch (a.dir) {
    case ACCT_SEND:
      stats->ncall++;
      stats->call_bytes += a.len;
      break;
    case ACCT_RECV:
      stats->nreply++;
      stats->reply_bytes += a.len;
      break;
    default:
      fatal ("Unknown RPC direction: %d", a.dir);
      break;
  }
}
// }}}

// {{{ locationcc - TCP connection management
vec<locationcc *> locationcc::allocated;

locationcc::locationcc (const chord_node &n) :
    n (n),
    tcpc (NULL),
    x (NULL),
    reapcaller (NULL)
{
  allocated.push_back (this);
}

ptr<locationcc>
locationcc::alloc (const chord_node &n)
{
  for (size_t i = 0; i < allocated.size (); i++) {
    if (allocated[i]->n.x == n.x)
      return mkref (allocated[i]);
  }
  return New refcounted<locationcc> (n);
}

void
locationcc::aclntmaker (const rpc_program *p, locationcc::aclntcb cb)
{
  ptr<aclnt> c (NULL);
  if (x && !x->ateof ()) {
    c = aclnt::alloc (x, *p);
    c->set_acct_hook (wrap (&track_aclnt));
  }
  cb (c);
}

TAMED void
locationcc::get_stream_aclnt (const rpc_program &p,
    callback<void, ptr<aclnt> >::ref cb)
{
  VARS {
    int fd (-1);
  }

  if (x && x->ateof ()) {
    if (reapcaller)
      timecb_remove (reapcaller);
    reapcaller = NULL;
    x = NULL;
  }

  if (x) {
    assert (!x->ateof ());
    assert (reapcaller);
    timecb_remove (reapcaller);
    reapcaller = NULL;
    ptr<aclnt> c = aclnt::alloc (x, p);
    if (c)
      c->set_acct_hook (wrap (&track_aclnt));
    cb (c);
  } else {
    assert (!reapcaller);
    aclntcbs.push_back (wrap 
	(this, &locationcc::aclntmaker, &p, cb));
    if (tcpc) return; // Our cb will be called when tcpc finishes.
    BLOCK {
      tcpc = tcpconnect (n.r.hostname,
		  n.r.port-1, // LAME CONVENTION
		  @(fd));
    }
    tcpc = NULL;
    if (fd < 0) {
      warn << "locationcc: connect to " << n << " failed: " <<
	strerror (errno) << "\n";
      assert (x == NULL);
    } else {
      x = axprt_stream::alloc (fd);
    }
    while (aclntcbs.size ()) {
      cbv cb = aclntcbs.pop_back ();
      cb ();
    }
  }
  if (x) {
    // XXX Should dynamically calculate delay to be just
    // longer than the time between the start of two rounds.
    reapcaller = delaycb (1200, wrap (this, &locationcc::reaper));
  }
}

void
locationcc::fill_ipportvn (u_int32_t &a, u_int32_t &b)
{
  sockaddr_in saddr;
  bzero (&saddr, sizeof(sockaddr_in));
  // saddr.sin_family = AF_INET;
  inet_aton (n.r.hostname.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (n.r.port);
  /* saddr fields are in network byte order */
  a = ntohl (saddr.sin_addr.s_addr);
  b = (ntohs (saddr.sin_port) << 16) | n.vnode_num;
}

void
locationcc::reaper () {
  reapcaller = NULL;
  // Forget about the axprt.  This will cause the connection
  // to close as soon as any outstanding aclnt's lose their last ref.
  x = NULL;
}

locationcc::~locationcc () {
  if (reapcaller) {
    timecb_remove (reapcaller);
    reapcaller = NULL;
  }
  for (size_t i = 0; i < allocated.size (); i++) {
    if (allocated[i] == this) {
      allocated[i] = allocated.back ();
      allocated.pop_back ();
      break;
    }
  }
}
// }}}

// {{{ maintainer - base maintainer class
static str ctype2ext (dhash_ctype c) {
  switch (c) {
  case DHASH_CONTENTHASH:
    return "c";
    break;
  case DHASH_KEYHASH:
    return "k";
    break;
  case DHASH_NOAUTH:
    return "n";
    break;
  default:
    fatal << "bad ctype\n";
  }
}

const u_int32_t maintainer::default_delay = 300;
maintainer::maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s) :
  host (hostinfo->host),
  ctype (hostinfo->ctype),
  sync (s),
  efrags (hostinfo->efrags),
  dfrags (hostinfo->dfrags),
  db (New refcounted<adb> (hostinfo->dbsock, hostinfo->dbname, hostinfo->hasaux)),
  private_path (path),
  gm (New refcounted<maint_global> (this)),
  running (false),
  in_progress (false),
  delay (default_delay),
  mainttimer (NULL),
  ltree (NULL)
{
}

maintainer::~maintainer () 
{
  stop ();
}

void
maintainer::start (u_int32_t d, bool randomize)
{
  int jitter = 0;

  delay = d;
  if (mainttimer || in_progress)
    return;
  running = true;
  if (randomize)
    jitter = random_getword () % delay;
  mainttimer = delaycb (delay + jitter,
      wrap (this, &maintainer::start_helper));
}
void maintainer::start_helper ()
{
  mainttimer = NULL;
  // Helper is needed for delaycb since run_cycle is TAMED.
  run_cycle (wrap (this, &maintainer::restart, delay));
}

void
maintainer::restart (u_int32_t d)
{
  if (running)
    start (d);
}

void
maintainer::stop ()
{
  if (mainttimer) {
    timecb_remove (mainttimer);
    mainttimer = NULL;
  }
  // If we are in-progress, make sure we don't restart later.
  // We do not have control to stop an in-process sync.
  running = false;
}

TAMED void
maintainer::run_cycle (cbv cb)
{
  in_progress = true;

  // Run local and global cycle in parallel
  BLOCK {
    local_maint_cycle (@());
    gm->next (@());
  }

  in_progress = false;
  delaycb (0, cb);
}

TAMED void
maintainer::local_maint_cycle (cbv cb)
{
  BLOCK {
    update_neighbors (@());
  }
  if (preds.size () > 0) {
    BLOCK { 
      process_neighbors (preds, succs, @());
    }
  }
  (cb) ();
}

TAMED void
maintainer::update_neighbors (cbv cb)
{
  VARS {
    ptr<chordID> id;
    chord_nodelistres *slist;
    chord_nodelistres *plist;
    clnt_stat e1, e2;
    bool changed (false);
  }
  id = New refcounted<chordID> (host.x);

  slist = New chord_nodelistres ();
  plist = New chord_nodelistres ();
  BLOCK {
    doRPC (host, chord_program_1, CHORDPROC_GETPREDLIST, 
	   id, plist, @(e1));
    doRPC (host, chord_program_1, CHORDPROC_GETSUCCLIST,
	   id, slist, @(e2));
  }
  if (e1 || e2) {
    warn << "my local node is down.\n";
    changed = true;
    goto updateOUT;
  }

  // Predecessor and predecessor list (for range).
  {
    size_t sz = plist->resok->nlist.size ();
    vec<ptr<locationcc> > npreds;
    for (size_t i = 1; i < sz; i++) {
      chord_node n = make_chord_node (plist->resok->nlist[i]);
      ptr<locationcc> s = locationcc::alloc (n);
      npreds.push_back (s);
    }
    if (npreds.size () != preds.size ())
      changed = true;
    else {
      for (size_t i = 0; i < preds.size (); i++) {
	if (npreds[i]->id () != preds[i]->id ()) {
	  changed = true;
	  break;
	}
      }
    }
    preds = npreds;
  }

  // Successor
  {
    size_t sz = slist->resok->nlist.size ();
    vec<ptr<locationcc> > nsuccs;
    for (size_t i = 1; i < sz; i++) {
      chord_node n = make_chord_node (slist->resok->nlist[i]);
      ptr<locationcc> s = locationcc::alloc (n);
      nsuccs.push_back (s);
    }
    if (nsuccs.size () != succs.size ())
      changed = true;
    else {
      for (size_t i = 0; i < succs.size (); i++) {
	if (nsuccs[i]->id () != succs[i]->id ()) {
	  changed = true;
	  break;
	}
      }
    }
    succs = nsuccs;
  }

updateOUT:
  stable = !changed;
  if (slist)
    delete slist;
  if (plist)
    delete plist;
  cb ();
}

TAMED void
maintainer::process_neighbors (
    const vec<ptr<locationcc> > &preds,
    const vec<ptr<locationcc> > &succs, cbv cb)
{
  // See Carbonite, Passing Tone or other implementations.
  cb ();
}

size_t
maintainer::get_global_repairs (size_t max,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs)
{
  size_t added = 0;
  ptr<locationcc> hostcc = locationcc::alloc (host);
  while (gm->maintqueue.size () && repairs.size () < max)
  {
    maint_repair_t repair;
    repair.responsible = false;
    repair.id = gm->maintqueue.pop_front ();
    hostcc->fill_ipportvn (repair.src_ipv4_addr,
	repair.src_port_vnnum);
    ptr<locationcc> dst = gm->maintdest.pop_front ();
    dst->fill_ipportvn (repair.dst_ipv4_addr,
	repair.dst_port_vnnum);
    repairs.push_back (repair);
    added++;
  }
  if (added || gm->maintqueue.size ())
    warn << host << " added " << added << " gm repairs; " << gm->maintqueue.size () << " remaining.\n";
  return added;
}

// }}}
// {{{ Carbonite
ref<maintainer> 
carbonite::produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb)
{
  return New refcounted<carbonite> (path, hostinfo, s, cb);
}

carbonite::carbonite (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb) :
    maintainer (path, hostinfo, s)
{
  db->getspaceinfo (wrap (this, &carbonite::init_ltree, cb));
}

void 
carbonite::init_ltree (cbv cb, adb_status err, str path, bool hasaux)
{
  if (err) {
    warn << "carbonite::init_ltree: Unexpected adbd error: " << err << "\n";
    return;
  }
  ltree = New refcounted<merkle_tree_bdb> (path.cstr (),
      /* join = */ true,
      /* ro = */ true);
  cb ();
}

carbonite::~carbonite ()
{
}

void 
carbonite::handle_missing (ptr<locationcc> from,
    ptr<merkle_tree> t,
    chordID key, bool missing_local)
{
  // Make the local tree look identical to the remote tree.
  // For now, also keep the adbd register up to date.
  ptr<location> rfrom = New refcounted<location> (from->chordnode ());
  if (db->hasaux ()) {
    chordID aux = (key & 0xFFFFFFFF);
    chordID dbkey = (key >> 32) << 32;
    if (missing_local) {
      t->insert (dbkey, aux.getui ());
    } else {
      t->remove (dbkey, aux.getui ());
    }
  } else {
    if (missing_local) {
      t->insert (key);
    } else {
      t->remove (key);
    }
  }
}

TAMED void
carbonite::process_neighbors (
    const vec<ptr<locationcc> > &preds,
    const vec<ptr<locationcc> > &osuccs,
    cbv cb)
{
  VARS {
    chordID rngmin, rngmax;
  }
  rngmin = preds[0]->id ();
  rngmax = host.x;
  if (db->hasaux ()) {
    rngmin = ((rngmin >> 32) << 32) | 0xFFFFFFFF;
    rngmax = ((rngmax >> 32) << 32) | 0xFFFFFFFF;
  }
  BLOCK {
    // Merkle data structures for remote nodes are private to maintd.
    // We want to sync with self, but just once.  This is to avoid
    // having newer information about self than successors, which
    // can cause spurious repairs of objects written between
    // sync and getrepairs.
    vec<ptr<locationcc> > succs = osuccs;
    if (succs.back ()->id () != host.x)
      succs.push_back (locationcc::alloc (host));
    // We only want to read each tree off the disk once
    // per sync period.  We initialize treedone here and it
    // gets flipped to true in getrepairs.
    treedone.clear ();
    for (unsigned int i=0; i < succs.size(); i++) {
      str succtreepath = strbuf () << private_path << "/" 
	<< host.vnode_num << "-"
	<< succs[i]->id () << "." << ctype2ext (ctype);
      treedone.push_back (false);
      ptr<merkle_tree> t = New refcounted<merkle_tree_bdb> 
	(succtreepath.cstr (),
	 /* join = */ false, /* ro = */ false);
      sync->sync_with (succs[i], rngmin, rngmax,
	  t, 
	  wrap (this, &carbonite::handle_missing, succs[i], t),
	  @());
    }
  }
  cb ();
}

struct keycounter {
  chordID id;
  unsigned int presence;
  unsigned int count;
  itree_entry<keycounter> ik;
  static const unsigned int psz = 8*sizeof (unsigned int);
  keycounter (chordID id) : id (id), presence (0), count (0) {}
  void add (unsigned int i) {
    assert (i < psz);
    unsigned int x = 1 << i;
    if ((presence & x) == x) return;
    presence |= x;
    count++;
  }
  int firstvoid () {
    unsigned int t = presence;
    unsigned int i = 0;
    while (i < psz) {
      if (!(t & 1))
	return i;
      i++; t >>= 1;
    }
    return psz;
  }
  int firstpresent () {
    unsigned int t = presence;
    unsigned int i = 0;
    while (i < psz) {
      if (t & 1)
	return i;
      i++; t >>= 1;
    }
    return psz;
  }
};
// Find the first leaf page that includes 'start'.
// For each tree, read in the next leaf page and place the keys
// in that page into a tree that counts replicas.
// Maintain a last key read for each tree (lastread).
// Find the min key in highcount.
// Iterate over the rbcounttree up until min(lastread) to find
// possible things to repair.
// If we have found at least 'count' objects, return!
// Otherwise, loop.
void
carbonite::getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs)
{
  repairs.setsize (0);
  get_global_repairs (count/2, repairs);
  if (in_progress) {
    if (!get_global_repairs (count, repairs))
      warn << host << ": Not returning repairs while sync is active.\n";
    return;
  }

  warn << host << ": Starting getrepairs " << start << " " << count << "\n";

  // No point in doing any work if there are no successors.
  if (!succs.size ())
    return;

  vec<ptr<merkle_tree> > trees;
  vec<chordID> lastread;
  chordID stop = host.x;
  if (db->hasaux ())
    stop = ((stop << 32) >> 32) | 0xFFFFFFFF;
  itree<chordID, keycounter, &keycounter::id, &keycounter::ik> keymap;

  // 0 is self, 1 and on are actual successors.
  // Make sure to use private copy of tree to avoid spurious repairs.
  vec<ptr<locationcc> > nsuccs;
  nsuccs.push_back (locationcc::alloc (host));
  nsuccs += succs;
  // Exclude dup self if self on successor list (small ring case).
  if (succs.back ()->id () == host.x)
    nsuccs.pop_back ();

  // Because all vnodes share a database, we only need to
  // consider one database per physical host.
  bhash<str> hosts;
  vec<bool> treeskip;
  for (unsigned int i = 0; i < nsuccs.size (); i++) {
    str succtreepath = strbuf () << private_path << "/" 
      << host.vnode_num << "-"
      << nsuccs[i]->id () << "." << ctype2ext (ctype);
    if (!merkle_tree_bdb::tree_exists (succtreepath.cstr ())) {
      warn << host << ": Not returning repairs due to missing tree "
	   << succtreepath << "\n";
      return;
    }
    ptr<merkle_tree> t = New refcounted<merkle_tree_bdb> 
      (succtreepath.cstr (), /* join = */ false, /* ro = */ true);
    trees.push_back (t);
    lastread.push_back (start);

    str host = strbuf () << nsuccs[i]->chordnode ().r;
    treeskip.push_back (hosts[host]);
    hosts.insert (host);
  }
  assert (treedone.size () == nsuccs.size ());
  
  while ((int) repairs.size () < count) {
    bool addedany = false;
    for (unsigned int i = 0; i < trees.size (); i++) {
      if (treedone[i] || treeskip[i]) continue;
      addedany = true;
      // Aim to read about one leaf page at a time.
      vec<chordID> keys = trees[i]->get_keyrange (lastread[i], stop, 64);
      if (keys.size () < 64)
	treedone[i] = true;
      for (unsigned int j = 0; j < keys.size (); j++) {
	keycounter *k = keymap[keys[j]];
	if (!k) {
	  k = New keycounter (keys[j]);
	  k->add (i);
	  keymap.insert (k);
	} else {
	  k->add (i);
	}
      }
      if (keys.size ())
	lastread[i] = incID (keys.back ());
    }
    // if we've read all the keys in the responsible range
    // in all trees, it's time to quit.
    if (!addedany)
      break;
    // Find the smallest range for which we have read everything
    chordID minimax = stop;
    for (unsigned int i = 0; i < trees.size (); i++)
      if (betweenleftincl (start, minimax, lastread[i]))
	minimax = lastread[i];

    keycounter *k = NULL; 
    for (k = keymap.first ();
	 (k && (int) repairs.size () < count);
	 k = keymap.next (k))
    {
      if (!between (start, minimax, k->id)) {
	// warn << "carbonite " << host << " rejects " << k->id << " not between\n";
	continue;
      }
      if (k->count < (unsigned) thresh) {
	maint_repair_t repair;
	repair.responsible = true;
	repair.id = k->id;

	repair.src_ipv4_addr = 0;
	repair.src_port_vnnum = 0;
	// aux is an indicator of mutability
	if (db->hasaux ()) {
	  unsigned int src = k->firstpresent ();
	  assert (src < nsuccs.size ());
	  nsuccs[src]->fill_ipportvn (repair.src_ipv4_addr,
	      repair.src_port_vnnum);
	}

	unsigned int dst = k->firstvoid ();
	if (dst < nsuccs.size ()) {
	  warn << "carbonite " << host << " adds " << k->id
	       << " to successor " << dst << "\n";
	  nsuccs[dst]->fill_ipportvn (repair.dst_ipv4_addr,
	      repair.dst_port_vnnum);
	  repairs.push_back (repair);
	} else {
	  // warn << "carbonite " << host << " rejects " << k->id
	  //      << " no dst " << dst << " >= " << nsuccs.size () << "\n";
	  continue;
	}
      } else {
	// warn << "carbonite " << host << " rejects " << k->id
	//      << " enough copies " << k->count << "\n";
      }
    }
  }
  keymap.deleteall_correct ();
}
// How to figure out what the ip.port.vnnum is for a given tree?
// Build up trees from the recorded successor list which includes
// ip addresses.
// }}}
// {{{ Passing Tone
ref<maintainer> 
passingtone::produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb)
{
  return New refcounted<passingtone> (path, hostinfo, s, cb);
}

passingtone::passingtone (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s, cbv cb) :
  maintainer (path, hostinfo, s)
{
  db->getspaceinfo (wrap (this, &passingtone::init_ltree, cb));
}

void 
passingtone::init_ltree (cbv cb, adb_status err, str path, bool hasaux)
{
  if (err) {
    warn << "passingtone::init_ltree: Unexpected adbd error: " << err << "\n";
    return;
  }
  ltree = New refcounted<merkle_tree_bdb> (path.cstr (),
      /* join = */ true,
      /* ro = */ true);
  cb ();
}

passingtone::~passingtone ()
{
}

void 
passingtone::handle_missing (ptr<locationcc> from,
    ptr<merkle_tree> t,
    chordID key, bool missing_local)
{
  // for key on succ/pred:
  //   if key in responsible range and not locally present:
  //     replicate key locally
  if (!missing_local)
    return;
  // XXX Should do better than linear scan here...
  for (size_t i = 0; i < repairqueue.size (); i++) {
    if (repairqueue[i].key == key)
      return;
  }
  warn << "passingtone " << host << " needs " << key << "\n";
  struct pt_repair_t r (key, from, timenow);
  repairqueue.push_back (r);
}

TAMED void
passingtone::process_neighbors (
    const vec<ptr<locationcc> > &preds,
    const vec<ptr<locationcc> > &succs,
    cbv cb)
{
  VARS {
    chordID rngmin, rngmax;
  }
  
  if (!stable) {
    warn << "passingtone " << host << " neighbors changed; flushing queue.\n";
    repairqueue.clear ();
  }

  // If there are repairs queued, don't bother re-syncing.
  if (repairqueue.size ()) {
    cb ();
    return;
  }

  // Our range starts before our efrag-th predecessor.
  // Unless there are so few nodes that all objects are fully replicated.
  if (preds.size() > efrags) 
    rngmin = preds[efrags-1]->id ();
  else
    rngmin = incID (host.x);
  rngmax = host.x;
  if (db->hasaux ()) {
    rngmin = ((rngmin >> 32) << 32) | 0xFFFFFFFF;
    rngmax = ((rngmax >> 32) << 32) | 0xFFFFFFFF;
  }

  // merkle_sync with first non-self succ and pred.
  // If any keys missing locally, fetch them.
  BLOCK {
    // Sync the entire tree range using the current live tree.
    size_t i = 0;
    for (i = 0; i < succs.size (); i++) 
      if (succs[i]->chordnode ().r.hostname != host.r.hostname ||
	  succs[i]->chordnode ().r.port     != host.r.port)
	break;
    if (i != succs.size ())
      sync->sync_with (succs[i], rngmin, rngmax,
	ltree, 
	wrap (this, &passingtone::handle_missing, succs[i], ltree),
	@());

    for (i = 0; i < preds.size (); i++) 
      if (preds[i]->chordnode ().r.hostname != host.r.hostname ||
	  preds[i]->chordnode ().r.port     != host.r.port)
	break;
    if (i != preds.size ())
      sync->sync_with (preds[i], rngmin, rngmax,
	ltree,
	wrap (this, &passingtone::handle_missing, preds[i], ltree),
	@());
  }
  cb ();
}

void
passingtone::getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs)
{
  get_global_repairs (count/2, repairs);

  // ignore thresh.
  // ignore start.
  sockaddr_in saddr;
  bzero (&saddr, sizeof(sockaddr_in));
  // saddr.sin_family = AF_INET;
  inet_aton (host.r.hostname.cstr (), &saddr.sin_addr);
  saddr.sin_port = htons (host.r.port);
  /* saddr fields are in network byte order */
  u_int32_t a = ntohl (saddr.sin_addr.s_addr);
  u_int32_t b = (ntohs (saddr.sin_port) << 16) | host.vnode_num;
  while (repairqueue.size () && (int) repairs.size () < count) {
    pt_repair_t rq = repairqueue.pop_front ();
    maint_repair_t r;
    r.responsible = true;
    r.id = rq.key;
    rq.from->fill_ipportvn (r.src_ipv4_addr, r.src_port_vnnum);
    r.dst_ipv4_addr = a;
    r.dst_port_vnnum = b;
    repairs.push_back (r);
    warn << "passingtone " << host << " unqueued " << rq.key
         << " after " << timenow - rq.add_time << "s.\n";
  }
}
// }}}
