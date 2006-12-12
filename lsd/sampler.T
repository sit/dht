// -*-c++-*-
// vim: filetype=cpp 

#include <arpc.h>
#include <../tools/rpclib.h>
#include <comm.h>

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
    cur_succ (0),
    replica_timer (60), _pred_client (NULL), _last_pred (NULL), 
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
}

void
sampler::doRPC (const rpc_program &prog,
		int procno, const void *in, void *out, aclnt_cb cb)
{
  chord_node dst;
  host_loc->fill_node (dst);
  ::doRPC (dst, prog, procno, in, out, cb);
}

TAMED void
sampler::update_neighbors ()
{
  VARS {
    ptr<chordID> id;
    chord_noderes *res;
    chord_nodelistres *lst;
    clnt_stat e1, e2;
    ptr<location> pred;
    ptr<location> succ;
    vec<ptr<location> > predlist;
  }
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
    resched ();
    return;
  }

  // Predecessor and predecessor list (for range).
  size_t sz = lst->resok->nlist.size ();
  for (size_t i = 1; i < sz; i++) {
    chord_node n = make_chord_node (lst->resok->nlist[i]);
    ptr<location> s = locations->lookup_or_create (n);
    locations->insert (s);
    predlist.push_back (s);
  }
  if (sz)
    pred = predlist[0];
  delete lst;

  chord_node n = make_chord_node (*res->resok);
  succ = locations->lookup_or_create (n);
  locations->insert (succ);
  delete res;

  sample_replicas_gotneighbors (pred, succ, predlist);
}

void
sampler::sample_replicas ()
{
  if (0 /* XXX */) {
    // still working on the last sample
    resched ();
  } else {
    warn << "sample_replicas: starting (ctype = " << ctype << ")\n";
    update_neighbors (); 
  } 
}

void
sampler::sample_replicas_gotneighbors (ptr<location> pred,
				       ptr<location> succ,
				       vec<ptr<location> > preds)
{
  if (preds.size () < 2) {
    resched ();
    return;
  }

  assert (pred);
  assert (succ);
  assert (preds[preds.size ()-1]);
  assert (host_loc);

  chordID rngmin = (preds[preds.size ()-1])->id ();

  wrap_call_getkeys (succ, false, rngmin,
		     wrap (this, &sampler::wrap_call_getkeys, pred, true, 
			   rngmin, wrap (this, &sampler::resched)));
}

void
sampler::resched ()
{
  delaycb (replica_timer, wrap (this, &sampler::sample_replicas));
}

void
sampler::wrap_call_getkeys (ptr<location> neighbor, bool pred, 
			    chordID rngmin, callback<void>::ref cb)
{
  call_getkeys (neighbor, pred, rngmin, cb);
}

TAMED void
sampler::call_getkeys (ptr<location> neighbor, bool pred, 
		       chordID rngmin, callback<void>::ref cb)
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
    clnt_stat err2;
    ptr<aclnt> client;
  }

  // establish a TCP connection to your predecessor and ask for
  // a sampling of keys
  if (pred && (_last_pred == NULL || _pred_client == NULL || 
	       _last_pred->id () != neighbor->id ()) ||
      !pred && (_last_succ == NULL || _succ_client == NULL || 
	       _last_succ->id () != neighbor->id ())) {

    if (pred) {
      _pred_client = NULL;
    } else {
      _succ_client = NULL;
    }

    warn << host_loc << ":" << ctype << " new neighbor " << neighbor 
	 << ", "  << pred << "\n";

    BLOCK {
      tcpconnect (neighbor->address ().hostname,
		  neighbor->address ().port+2, // LAME CONVENTION
		  @(fd));
    }

    if (fd < 0) {
      warn << "connect to remote sample_server failed\n";
      (*cb)();
      return;
    }
    ptr<axprt_stream> xprt = axprt_stream::alloc (fd);
    ptr<aclnt> newclient = aclnt::alloc (xprt, sample_program_1);

    if (pred) {
      _pred_client = newclient;
    } else {
      _succ_client = newclient;
    } 

  }

  // now we're connected to the latest pred, commence RPCing

  if (pred) {
    _last_pred = neighbor;
    client = _pred_client;
  } else {
    _last_succ = neighbor;
    client = _succ_client;
  }
  arg = New refcounted<getkeys_sample_arg> ();
  arg->ctype = ctype;
  arg->vnode = neighbor->vnode ();
  arg->rngmin = rngmin;
  arg->rngmax = host_loc->id ();
  res = New refcounted<getkeys_sample_res> ();

  warn << host_loc << ":" << ctype << " Calling getkeys with min=" 
       << arg->rngmin << " and max=" << arg->rngmax << "\n";

  BLOCK {

    client->call (SAMPLE_GETKEYS, arg, res, @(err));

  }

  if (err) {
    warn << "RPC to remote sample_server failed\n";
    if (pred) {
      _pred_client = NULL;
    } else {
      _succ_client = NULL;
    }
    (*cb)();
    return;
  }

  // now we need to look up each of these keys and see which ones we have.
  for (i = 0; i < res->resok->keys.size (); i++) {
    BLOCK {
      db->fetch (res->resok->keys[i], @(stat, key, data));
    }

    // TODO: going to need special mechanisms for NOAUTH blocks

    if (stat != ADB_OK) {
      //warn << host_loc << ":" << ctype << " going to get key " 
      //   << res->resok->keys[i] << "\n";
      to_fetch.push_back (res->resok->keys[i]);
    }

  }

  if (to_fetch.size () == 0) {
    // didn't get no keys back
    (*cb)();
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
    client->call (SAMPLE_GETDATA, data_arg, data_res, @(err2));
  }

  if (err2) {
    warn << "Data RPC to remote sample_server failed\n";
    (*cb)();
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

  (*cb)();
}
