// -*-c++-*-
// vim: filetype=cpp  foldmethod=marker
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpc.h>

#include <chord_prot.h>
#include <comm.h>
#include <merkle.h>
#include <merkle_tree_disk.h>

#include <id_utils.h>
#include <location.h>
#include <libadb.h>
#include <rpclib.h>

#include <maint_prot.h>
#include "maint_policy.h"

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
  if (x)
    c = aclnt::alloc (x, *p);
  cb (c);
}

TAMED void
locationcc::get_stream_aclnt (const rpc_program &p,
    callback<void, ptr<aclnt> >::ref cb)
{
  VARS {
    int fd (-1);
  }

  if (x) {
    assert (reapcaller);
    timecb_remove (reapcaller);
    reapcaller = NULL;
    ptr<aclnt> c = aclnt::alloc (x, p);
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
      allocated[i] = allocated.pop_back ();
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
  efrags (hostinfo->dfrags),
  dfrags (hostinfo->efrags),
  db (New refcounted<adb> (hostinfo->dbsock, hostinfo->dbname, hostinfo->hasaux)),
  private_path (path),
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
maintainer::start (u_int32_t d)
{
  delay = d;
  if (mainttimer || in_progress)
    return;
  running = true;
  mainttimer = delaycb (delay,
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

  // Now seems a good time to pick up any local changes.
  ltree->sync ();

  BLOCK {
    update_neighbors (@());
  }
  if (preds.size () >= 2) {
    BLOCK { 
      process_neighbors (preds, succs, @());
    }
  }
  in_progress = false;
  delaycb (0, cb);
}

TAMED void
maintainer::update_neighbors (cbv cb)
{
  VARS {
    ptr<chordID> id;
    chord_nodelistres *slist;
    chord_nodelistres *plist;
    clnt_stat e1, e2;
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
    goto updateOUT;
  }

  // Predecessor and predecessor list (for range).
  {
    size_t sz = plist->resok->nlist.size ();
    preds.clear ();
    for (size_t i = 1; i < sz; i++) {
      chord_node n = make_chord_node (plist->resok->nlist[i]);
      ptr<locationcc> s = locationcc::alloc (n);
      preds.push_back (s);
    }
  }

  // Successor
  {
    size_t sz = slist->resok->nlist.size ();
    succs.clear ();
    for (size_t i = 1; i < sz; i++) {
      chord_node n = make_chord_node (slist->resok->nlist[i]);
      ptr<locationcc> s = locationcc::alloc (n);
      succs.push_back (s);
    }
  }

updateOUT:
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
// }}}
// {{{ Carbonite
ref<maintainer> 
carbonite::produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s)
{
  return New refcounted<carbonite> (path, hostinfo, s);
}

carbonite::carbonite (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s) :
    maintainer (path, hostinfo, s)
{
  db->getspaceinfo (wrap (this, &carbonite::init_ltree));
}

void 
carbonite::init_ltree (adb_status err, str path, bool hasaux)
{
  if (err) {
    warn << "carbonite::init_ltree: Unexpected adbd error: " << err << "\n";
    return;
  }
  ltree = New refcounted<merkle_tree_disk> (path, /* rw = */ false);
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
    db->update (dbkey, rfrom, aux.getui (), missing_local, true);
    if (missing_local) {
      t->insert (dbkey, aux.getui ());
    } else {
      t->remove (dbkey, aux.getui ());
    }
  } else {
    db->update (key, rfrom, missing_local, true);
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
    const vec<ptr<locationcc> > &succs,
    cbv cb)
{
  BLOCK {
    // Merkle data structures for remote nodes are private to maintd.
    for (unsigned int i=0; i < succs.size(); i++) {
      strbuf succtreepath; succtreepath << private_path << "/" 
	<< host.vnode_num << "-"
	<< succs[i]->id () << "." << ctype2ext (ctype);
      ptr<merkle_tree> t = New refcounted<merkle_tree_disk> 
	(succtreepath, /* rw = */ true);
      sync->sync_with (succs[i], preds[0]->id (), host.x,
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
  if (in_progress) {
    warn << host << ": Not returning repairs while sync is active.\n";
    return;
  }

  vec<ptr<merkle_tree> > trees;
  vec<chordID> lastread;
  vec<bool> treedone;
  chordID stop = host.x;
  itree<chordID, keycounter, &keycounter::id, &keycounter::ik> keymap;

  // 0 is self, 1 and on are actual successors.
  trees.push_back (ltree);
  lastread.push_back (start);
  treedone.push_back (false);

  for (unsigned int i = 0; i < succs.size (); i++) {
    strbuf succtreepath; succtreepath << private_path << "/" 
      << host.vnode_num << "-"
      << succs[i]->id () << "." << ctype2ext (ctype);
    ptr<merkle_tree> t = New refcounted<merkle_tree_disk> 
      (succtreepath, /* rw = */ false);
    trees.push_back (t);
    lastread.push_back (start);
    treedone.push_back (false);
  }
  
  repairs.setsize (0);
  while ((int) repairs.size () < count) {
    bool addedany = false;
    for (unsigned int i = 0; i < trees.size (); i++) {
      if (treedone[i]) continue;
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
	repair.id = k->id;
	unsigned int dst = k->firstvoid ();
	if (dst > succs.size ()) {
	  // warn << "carbonite " << host << " rejects " << k->id
	  //      << " no dst " << dst << " >= " << succs.size () << "\n";
	  continue;
	}
	// warn << "carbonite " << host << " adds " << k->id
	//      << " to successor " << dst << "\n";
	if (dst == 0) {
	  ptr<locationcc> hostcc = locationcc::alloc (host);
	  hostcc->fill_ipportvn (repair.machine_order_ipv4_addr,
	      repair.machine_order_port_vnnum);
	} else {
	  succs[dst-1]->fill_ipportvn (repair.machine_order_ipv4_addr,
	      repair.machine_order_port_vnnum);
	}
	repairs.push_back (repair);
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
passingtone::produce_maintainer (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s)
{
  return New refcounted<passingtone> (path, hostinfo, s);
}

passingtone::passingtone (str path, maint_dhashinfo_t *hostinfo, ptr<syncer> s) :
  maintainer (path, hostinfo, s)
{
  db->getspaceinfo (wrap (this, &passingtone::init_ltree));
}

void 
passingtone::init_ltree (adb_status err, str path, bool hasaux)
{
  if (err) {
    warn << "passingtone::init_ltree: Unexpected adbd error: " << err << "\n";
    return;
  }
  ltree = New refcounted<merkle_tree_disk> (path, /* rw = */ false);
}

passingtone::~passingtone ()
{
}

void 
passingtone::handle_missing (ptr<locationcc> from,
    ptr<merkle_tree> t,
    chordID key, bool missing_local)
{
  // For completeness, keep adbd up to date.
  ptr<location> rfrom = New refcounted<location> (from->chordnode ());
  if (db->hasaux ()) {
    chordID aux = (key & 0xFFFFFFFF);
    chordID dbkey = (key >> 32) << 32;
    db->update (dbkey, rfrom, aux.getui (), missing_local, true);
  } else {
    db->update (key, rfrom, missing_local, true);
  }

  // for key on succ/pred:
  //   if key in responsible range and not locally present:
  //     replicate key locally
  if (!missing_local)
    return;
  // XXX Should do better than linear scan here...
  for (size_t i = 0; i < repairqueue.size (); i++) {
    if (repairqueue[i] == key)
      return;
  }
  warn << "passingtone " << host << " needs " << key << "\n";
  repairqueue.push_back (key);
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

  // If there are repairs queued, don't bother re-syncing.
  if (repairqueue.size ())
    return;

  // Our range starts before our efrag-th predecessor.
  // Unless there are so few nodes that all objects are fully replicated.
  if (preds.size() > efrags) 
    rngmin = preds[efrags]->id ();
  else
    rngmin = incID (host.x);
  rngmax = host.x;

  // merkle_sync with succ and pred.  If any keys missing
  // locally, fetch them.
  BLOCK {
    // Sync the entire tree range using the current live tree.
    sync->sync_with (succs[0], rngmin, rngmax,
	ltree, 
	wrap (this, &passingtone::handle_missing, succs[0], ltree),
	@());
    sync->sync_with (preds[0], rngmin, rngmax,
	ltree, 
	wrap (this, &passingtone::handle_missing, preds[0], ltree),
	@());
  }
  cb ();
}

void
passingtone::getrepairs (chordID start, int thresh, int count,
      rpc_vec<maint_repair_t, RPC_INFINITY> &repairs)
{
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
  while (repairqueue.size () && count > 0) {
    chordID k = repairqueue.pop_front ();
    maint_repair_t r;
    r.id = k;
    r.machine_order_ipv4_addr = a;
    r.machine_order_port_vnnum = b;
    repairs.push_back (r);
    count--;
  }
}
// }}}
