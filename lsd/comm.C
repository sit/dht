#include "chord.h"

/*
 *
 * comm.C 
 *
 * This code deals with the actual communication to nodes.
 *
 */

chord_stats stats;


void
p2p::timeout(location *l) {
  assert(l);
  warn << "timeout on " << l->n << " closing socket\n";
  if (l->nout == 0) l->x = NULL;
  else
    {
      warn << "timeout on node " << l->n << " has overdue RPCs\n";
      l->timeout_cb = delaycb(360,0,wrap(this, &p2p::timeout, l));
    }
}


void
p2p::doRPC (sfs_ID &ID, rpc_program progno, int procno, 
	    ptr<void> in, void *out,
	    aclnt_cb cb)

{

  if (lookups_outstanding > 0) lookup_RPCs++;

  if ((insert_or_lookup > 0) && (myID != ID)) stats.total_rpcs++;

  location *l = locations[ID];

  assert (l);
  assert (l->alive);

  l->nout++;
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(360,0,wrap(this, &p2p::timeout, l));
    ptr<aclnt> c = aclnt::alloc(l->x, progno);
    c->call (procno, in, out, cb);
  } else {
    doRPC_cbstate *st = 
      New doRPC_cbstate (progno, procno, in, out,  cb);
    l->connectlist.insert_tail (st);
    if (!l->connecting) {
      l->connecting = true;
      chord_connect(ID, wrap (mkref (this), &p2p::dorpc_connect_cb, l));
    }
  }
}

void
p2p::dorpc_connect_cb(location *l, ptr<axprt_stream> x) {

  assert(l);
  if (x == NULL) {
    warnx << "connect_cb: connect failed\n";
    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      aclnt_cb cb = st->cb;
      (*cb) (RPC_FAILED);
    }
    l->connecting = false;
    return;
  }

  assert(l->alive);
  l->x = x;
  l->connecting = false;
  l->timeout_cb = delaycb(360,0,wrap(this, &p2p::timeout, l));

  doRPC_cbstate *st, *st1;
  for (st = l->connectlist.first; st; st = st1) {
    ptr<aclnt> c = aclnt::alloc (x, st->progno);
    c->call (st->procno, st->in, st->out, st->cb);
    st1 = l->connectlist.next (st);
    l->connectlist.remove(st);
  }
}

void
p2p::chord_connect(sfs_ID ID, callback<void, ptr<axprt_stream> >::ref cb) {
  
  location *l = locations[ID];
  assert (l);
  assert (l->alive);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(360,0,wrap(this, &p2p::timeout, l));
    (*cb)(l->x);
  } else {
    tcpconnect (l->addr.hostname, l->addr.port, wrap (mkref (this), &p2p::connect_cb, cb));
  }
}


void
p2p::connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd)
{

  if (fd < 0) {
    (*cb)(NULL);
  } else {
    tcp_nodelay(fd);
    ptr<axprt_stream> x = axprt_stream::alloc(fd);
    (*cb)(x);
  }
}


void
p2p::stats_cb () {


  warnx << "Chord Node " << myID << " status\n";

  warnx << "Core layer stats\n";
  warnx << "  Per link avg. RPC latencies\n";
  location *l = locations.first ();
  while (l) {
    warnx << "    link " << l->n << "\n";
    fprintf(stderr, "       Average latency: %f\n", ((float)(l->total_latency))/l->num_latencies);
    fprintf(stderr, "       Max latency: %ld\n", l->max_latency);
    int *nkeys = (*stats.balance)[l->n];
    if (nkeys)
      warnx << "      " <<  *nkeys << " keys were sent to this node\n";
    else
      warnx << "       0 keys were sent to this node\n";
    l = locations.next(l);
  }

  warnx << "DHASH layer stats\n";
#if 0
  warnx << "   Caching is ";
  if (do_caching) 
    warnx << "on\n";
  else
    warnx << "off\n";
#endif
  warnx << "   " << stats.insert_ops << " insertions\n";
  fprintf(stderr, "      %f average hops per insert\n", ((float)(stats.insert_path_len))/stats.insert_ops);
  warnx << "   " << stats.lookup_ops << " lookups\n";
  fprintf(stderr, "      %f average hops per lookup\n", ((float)(stats.lookup_path_len))/stats.lookup_ops);
  fprintf(stderr, "      %f ms average latency per lookup operation\n", ((float)(stats.lookup_lat))/stats.lookup_ops);
  fprintf(stderr, "      %ld ms greatest latency for a lookup\n", stats.lookup_max_lat);
  fprintf(stderr, "      %f KB/sec avg. total bandwidth\n", ((float)(stats.lookup_bytes_fetched))/(stats.lookup_lat));
  

}
