#include <arpc.h>
#include <rpclib.h>
#include <comm.h>

#include <dhash_prot.h>
#include <locationtable.h>
#include <location.h>
#include <merkle_tree.h>
#include <merkle_tree_disk.h>
#include <merkle_syncer.h>

#include <syncer.h>

static int sync_trace (getenv ("SYNC_TRACE") ? atoi (getenv ("SYNC_TRACE")) : 0);

syncer::syncer (ptr<locationtable> locations,
		ptr<location> h,
		str dbdir,
		str dbpath,
		str dbname,
		dhash_ctype ctype,
		u_int dfrags, u_int efrags)
  : locations (locations), ctype (ctype), dfrags (dfrags), efrags (efrags),
    tmptree (NULL), host_loc (h),
    db (New refcounted<adb> (dbpath, dbname)),
    db_prefix (strbuf () << dbdir << "/" << dbname),
    cur_succ (0),
    replica_timer (300)
{ 
  
  warn << "new syncer: \n" 
       << "   dbpath: " << dbpath << "\n"
       << "    dbext: " << dbname << "\n"
       << "    ctype: " << ctype << "\n"
       << " d/efrags: " << dfrags << "/" << efrags << "\n";

  locations->insert (h);
  locations->pin (h->id ());
  
  if (sync_trace >= 10)
    replica_timer = sync_trace;
  
  // Initially randomize a little.
  int delay = random_getword () % replica_timer;
  delaycb (delay, wrap (this, &syncer::sync_replicas)); 
}

syncer::~syncer ()
{
  tmptree = NULL;
  replica_syncer = NULL;
  db = NULL;
}

void
syncer::doRPC (const rpc_program &prog,
		int procno, const void *in, void *out, aclnt_cb cb)
{
  chord_node dst;
  host_loc->fill_node (dst);
  ::doRPC (dst, prog, procno, in, out, cb);
}

void
syncer::update_pred (cb_location cb)
{
  ptr<chordID> id = New refcounted<chordID> (host_loc->id ());

  chord_noderes *res = New chord_noderes ();
  doRPC (chord_program_1, CHORDPROC_GETPREDECESSOR,
	 id, res,
	 wrap (this, &syncer::update_pred_cb, cb, res) );
}

void
syncer::update_pred_cb (cb_location cb, chord_noderes *res, clnt_stat err)
{
  if (err) {
    warn << "my local node is down?\n";
    (*cb) (NULL);
  } else {
    chord_node n = make_chord_node (*res->resok);
    ptr<location> x = locations->lookup_or_create (n);
    locations->insert (x);
    cb (x);
  }
  delete res;
}


void
syncer::get_succlist (cb_locationlist cb)
{
  ptr<chordID> ga = New refcounted<chordID> (host_loc->id ());
  chord_nodelistres *lst = New chord_nodelistres ();
  doRPC (chord_program_1,
	 CHORDPROC_GETSUCCLIST, 
	 ga, lst, wrap (this, &syncer::get_succlist_cb, lst, cb));
}

void
syncer::get_succlist_cb (chord_nodelistres *res,
		 cb_locationlist cb,
		 clnt_stat status)
{
  vec<ptr<location> > ret;
  if (!status) {
    size_t sz = res->resok->nlist.size ();
    for (size_t i = 0; i < sz; i++) {
      chord_node n = make_chord_node (res->resok->nlist[i]);
      ptr<location> s = locations->lookup_or_create (n);
      locations->insert (s);
      ret.push_back (s);
    }
  }

  cb (ret);
  delete res;
}


void
syncer::sync_replicas ()
{
  if (replica_syncer && !replica_syncer->done ()) {
    // still working on the last sync
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
  } else {
    warn << "sync_replicas: starting (ctype = " << ctype << ")\n";
    update_pred (wrap (this, &syncer::sync_replicas_predupdated)); 
  } 
}

void
syncer::sync_replicas_predupdated (ptr<location> pred)
{
  if (!pred) {
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
    return;
  }
  warn << "sync_replicas: my pred is " << pred << "\n";
  get_succlist (wrap (this, &syncer::sync_replicas_gotsucclist, pred));
}


void
syncer::sync_replicas_gotsucclist (ptr<location> pred,
				   vec<ptr<location> > succs) 
{
  if (succs.size () < 2) {
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
    return;
  }
    
  // succs[0] is the vnode we are working for
  // pred = locations->closestpredloc (succs[0]);
  assert (pred);
  assert (succs[0]);
  assert (host_loc);
 
  cur_succ++; // start at 1 (0 is me)
  if (efrags > 0 && cur_succ >= efrags) cur_succ = 1;
  else if (cur_succ >= succs.size ()) cur_succ = 1;

  assert (succs[cur_succ]);

  // first thing to do is establish a TCP connection with the neighbor
  ptr<location> neighbor = succs[cur_succ];
  curr_dst = neighbor;
  tcpconnect (neighbor->address ().hostname,
	      neighbor->address ().port-1,
	      wrap (this, &syncer::tcp_connected, pred, succs));
}

void
syncer::tcp_connected (ptr<location> pred,
		       vec<ptr<location> > succs, int fd)
{
  if (fd < 0) {
    warn << "couldn't connect to neighbor " << curr_dst << ", giving up\n";
    delaycb (replica_timer, wrap (this, &syncer::sync_replicas));
    return;
  }

  ptr<axprt_stream> xprt = axprt_stream::alloc (fd);
  curr_client = aclnt::alloc (xprt, merklesync_program_1);

  str ext;
  switch (ctype) {
  case DHASH_CONTENTHASH:
    ext = "c";
    break;
  case DHASH_KEYHASH:
    ext = "k";
    break;
  case DHASH_NOAUTH:
    ext = "n";
    break;
  default:
    fatal << "bad ctype\n";
  }

  str merkle_file = strbuf () << db_prefix << "/" << succs[cur_succ]->id ()
			     << "." << ext << "/";

  tmptree = New refcounted<merkle_tree_disk>
     (strbuf () << merkle_file << "index.mrk",
      strbuf () << merkle_file << "internal.mrk",
      strbuf () << merkle_file << "leaf.mrk", true);

  replica_syncer = New refcounted<merkle_syncer> 
    (curr_dst->vnode (), ctype, tmptree, 
     wrap (this, &syncer::doRPC_unbundler),
     wrap (this, &syncer::missing, succs[cur_succ], tmptree));
  
  bigint rngmin = pred->id ();
  bigint rngmax = succs[0]->id ();

  warn << host_loc->id () << " syncing with " << succs[cur_succ] 
       << " (succ #" << cur_succ << ")"
       << " for range [ " << rngmin << ", " << rngmax << " ]\n";
  
  replica_syncer->sync (rngmin, rngmax);
  
  delaycb (replica_timer, wrap (this, &syncer::sync_replicas)); 
}

void
syncer::doRPC_unbundler (RPC_delay_args *args)
{
  curr_client->call (args->procno, args->in, args->out, args->cb);

  //chord_node n;
  //dst->fill_node (n);
  //::doRPC (n, args->prog, args->procno, args->in, args->out, args->cb);
}

void
syncer::missing (ptr<location> from, ptr<merkle_tree> tmptree,
		 bigint key, bool missing_local)
{
  // if he tells us that we're missing it, then he has it.
  // otherwise, we found out he doesn't have it.
  // XXX this switch business is kinda gross.
  switch (ctype) {
  case DHASH_CONTENTHASH:
    db->update (key, from, missing_local, true);
    // if they have it, but we don't yet, add it to our tree
    // otherwise remove it from our tree
    if (missing_local) {
      tmptree->insert (key);
    } else {
      tmptree->remove (key);
    }
    break;
  case DHASH_KEYHASH:
  case DHASH_NOAUTH:
    {
      chordID aux = (key & 0xFFFFFFFF);
      chordID dbkey = (key >> 32) << 32;
      db->update (dbkey, from, aux.getui (), missing_local, true);
      // if they have it, but we don't yet, add it to our tree
      // otherwise remove it from our tree
      if (missing_local) {
	tmptree->insert (dbkey, aux.getui ());
      } else {
	tmptree->remove (dbkey, aux.getui ());
      }
    }
    break;
  default:
    fatal << "syncer::missing: unexpected ctype " << ctype << "\n";
    break;
  }
}
