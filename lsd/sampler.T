// -*-c++-*-
// vim: filetype=cpp 

#include <arpc.h>
#include <rpclib.h>
#include <comm.h>
#include "libadb.h"

#include <dhash_prot.h>
#include <locationtable.h>
#include <location.h>

#include "sampler.h"
#include <sample_prot.h>

static int sample_trace (getenv ("SAMPLE_TRACE") ? atoi (getenv ("SAMPLE_TRACE")) : 0);

sampler::sampler (ptr<locationtable> locations,
		  ptr<location> h,
		  str dbpath,
		  str dbname,
		  dhash_ctype ctype,
		  u_int dfrags, u_int efrags)
  : locations (locations), ctype (ctype), dfrags (dfrags), efrags (efrags),
    host_loc (h),
    db (New refcounted<adb> (dbpath, dbname)),
    replica_timer (60),
    in_progress (false),
    _pred_client (NULL), _last_pred (NULL), 
    _succ_client (NULL), _last_succ (NULL)
{ 
  warn << "new sampler: \n" 
       << "   dbpath: " << dbpath << "\n"
       << "    dbext: " << dbname << "\n"
       << "    ctype: " << ctype << "\n"
       << " d/efrags: " << dfrags << "/" << efrags << "\n";

  locations->insert (h);
  locations->pin (h->id ());
  
  if (sample_trace >= 10)
    replica_timer = sample_trace;
  
  // Initially randomize a little.
  int delay = random_getword () % replica_timer;
  delaycb (delay, wrap (this, &sampler::sample_replicas)); 
}

sampler::~sampler ()
{
  db = NULL;
  _pred_client = NULL;
  _succ_client = NULL;
}

void
sampler::doRPC (const rpc_program &prog,
		int procno, const void *in, void *out, aclnt_cb cb)
{
  chord_node dst;
  host_loc->fill_node (dst);
  ::doRPC (dst, prog, procno, in, out, cb);
}

void
sampler::resched ()
{
  delaycb (replica_timer, wrap (this, &sampler::sample_replicas));
}

void
sampler::sample_replicas ()
{
  assert (!in_progress);
  warn << "sample_replicas: starting (ctype = " << ctype << ")\n";
  update_neighbors (); 
}

TAMED void
sampler::update_neighbors ()
{
  VARS {
    ptr<chordID> id;
    chord_noderes *res;
    chord_nodelistres *lst;
    clnt_stat e1, e2;

    chordID rngmin;
    ptr<location> pred;
    ptr<location> succ;
    vec<ptr<location> > predlist;
  }
  in_progress = true;
  id = New refcounted<chordID> (host_loc->id ());

  res = New chord_noderes ();
  lst = New chord_nodelistres ();
  BLOCK {
    doRPC (chord_program_1, CHORDPROC_GETPREDLIST, 
	   id, lst, @(e1));
    doRPC (chord_program_1, CHORDPROC_GETSUCCESSOR,
	   id, res, @(e2));
  }
  if (e1 || e2) {
    warn << "my local node is down.\n";
    goto OUT;
  }

  // Predecessor and predecessor list (for range).
  {
    size_t sz = lst->resok->nlist.size ();
    for (size_t i = 1; i < sz; i++) {
      chord_node n = make_chord_node (lst->resok->nlist[i]);
      ptr<location> s = locations->lookup_or_create (n);
      locations->insert (s);
      predlist.push_back (s);
    }
    if (sz)
      pred = predlist[0];
  }

  // Successor
  {
    chord_node n = make_chord_node (*res->resok);
    succ = locations->lookup_or_create (n);
    locations->insert (succ);
  }

  if (predlist.size () < 2)
    goto OUT;

  // Our range starts before our efrag-th predecessor.
  // Unless there are so few nodes that all objects are fully replicated.
  if (predlist.size() > efrags) 
    rngmin = predlist[efrags]->id ();
  else
    rngmin = incID (*id);

  BLOCK { 
    call_getkeys (succ, _last_succ, _succ_client, rngmin, @(_succ_client));
    call_getkeys (pred, _last_pred, _pred_client, rngmin, @(_pred_client));
  }
  _last_succ = succ;
  _last_pred = pred;

OUT:
  if (lst)
    delete lst;
  if (res)
    delete res;
  in_progress = false;
  resched ();
}

TAMED void
sampler::call_getkeys (ptr<location> neighbor,
		       ptr<location> last_neighbor,
		       ptr<aclnt> client,
		       chordID rngmin,
		       callback<void, ptr<aclnt> >::ref cb)
{
  VARS {
    vec<chordID> to_fetch;
    uint i;
    adb_status stat;
    chordID key;
    str data;
    ptr<getkeys_sample_arg> arg;
    ptr<getkeys_sample_res> res;
    ptr<getdata_sample_arg> data_arg;
    ptr<getdata_sample_res> data_res;
    int fd;
    clnt_stat err;
  }

  // Establish a new connection if necessary.
  if (!last_neighbor || (last_neighbor->id () != neighbor->id ())) {
    client = NULL; 
    warn << host_loc << ":" << ctype << " new neighbor " << neighbor << "\n";
    BLOCK {
      tcpconnect (neighbor->address ().hostname,
		  neighbor->address ().port+2, // LAME CONVENTION
		  @(fd));
    }
    if (fd < 0) {
      warn << "connect to remote sample_server failed\n";
      (*cb) (NULL);
      return;
    }
    ptr<axprt_stream> xprt = axprt_stream::alloc (fd);
    client = aclnt::alloc (xprt, sample_program_1);
  }

  arg = New refcounted<getkeys_sample_arg> ();
  arg->ctype = ctype;
  arg->vnode = neighbor->vnode ();
  arg->rngmin = rngmin;
  arg->rngmax = host_loc->id ();
  res = New refcounted<getkeys_sample_res> ();

  warn << host_loc << ":" << ctype 
       << ": calling getkeys with min=" << arg->rngmin
       << " and max=" << arg->rngmax << "\n";
  BLOCK {
    client->call (SAMPLE_GETKEYS, arg, res, @(err));
  }
  if (err) {
    warn << host_loc << ":" << ctype 
         << ": RPC to remote sample_server failed: " << err << "\n";
    (*cb) (NULL);
    return;
  }

  // now we need to look up each of these keys and see which ones we have.
  for (i = 0; i < res->resok->keys.size (); i++) {
    BLOCK {
      db->fetch (res->resok->keys[i], @(stat, key, data));
    }
    // TODO: going to need special mechanisms for NOAUTH blocks
    if (stat != ADB_OK) {
      to_fetch.push_back (res->resok->keys[i]);
    }
  }

  if (to_fetch.size () == 0) {
    // No keys needed this round.
    (*cb) (client);
    return;  
  }

  // now fetch all those keys from that pred
  data_arg = New refcounted<getdata_sample_arg> ();
  data_res = New refcounted<getdata_sample_res> ();
  
  data_arg->ctype = ctype;
  data_arg->vnode = neighbor->vnode ();  
  data_arg->keys.setsize (to_fetch.size ());
  for (i = 0; i < to_fetch.size (); i++) {
    data_arg->keys[i] = to_fetch[i];
  }

  BLOCK {
    client->call (SAMPLE_GETDATA, data_arg, data_res, @(err));
  }

  if (err) {
    warn << host_loc << ":" << ctype
         << ": Data RPC to remote sample_server failed: " << err << "\n";
    (*cb) (NULL);
    return;
  }

  // now insert them all into our db
  assert (data_arg->keys.size () == data_res->resok->data.size ());
  for (i = 0; i < data_res->resok->data.size (); i++) {
    BLOCK {
      str data2 (data_res->resok->data[i].base (), 
		data_res->resok->data[i].size ());
      warn << host_loc << ":" << ctype << " storing block " 
	   << data_arg->keys[i] << "\n";
      db->store (data_arg->keys[i], data2, @(stat));
    }
    // TODO: check for errors
  }

  (*cb) (client);
}
