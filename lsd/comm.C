#include "chord.h"

/*
 *
 * comm.C 
 *
 * This code deals with the actual communication to nodes.
 *
 */

void
p2p::timeout(location *l) {
  // warn << "timeout on " << l->n << " closing socket\n";
  if (l->nout == 0) l->c = NULL;
  else
    {
      // warn << "timeout on node " << l->n << " has overdue RPCs\n";
      l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));
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
    }
    l->connecting = false;
  } else {
    // warnx << "connect_cb: connect to " << l->n << "succeeded (" << fd << ")\n";
    assert (l->alive);
    ptr<aclnt> c = aclnt::alloc (axprt_stream::alloc (fd), sfsp2p_program_1);
    l->c = c;
    l->connecting = false;
    l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));

    doRPC_cbstate *st, *st1;
    for (st = l->connectlist.first; st; st = st1) {
      st1 = l->connectlist.next (st);
      c->call (st->procno, st->in, st->out, st->cb);
      l->connectlist.remove(st);
    }
  }
}

void
p2p::timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, clnt_stat err) 
{
  struct timeval now;
  gettimeofday(&now, NULL);
  l->total_latency += (now.tv_sec - start->tv_sec)*1000000 + (now.tv_usec - start->tv_usec);
  l->num_latencies++;
  l->nout--;
  (*cb)(err);

}

// just add a time delay to represent distance
void
p2p::doRPC (sfs_ID &ID, int procno, const void *in, void *out,
		      aclnt_cb cb)
{
  // will IDs map to node numbers?? (ie. if ID is 
  // something diff then this may give weird results)

  // get "distance" between self and destination
  int time = 0;

  #ifdef _SIM_ 
  int dist = *(edges+(int)myID.getsi()*numnodes+(int)ID.getsi());
  time = dist*10; // Not sure how to scale time delay
  #endif
  // should not be delayed if not simulating
  timecb_t* decb =  delaycb (time, 0,wrap(mkref (this), &p2p::doRealRPC,ID,procno,in,out,cb));
}

// NOTE: now passing ID by value instead of referencing it...
// (getting compiler errors before). now seems ok
// May want to try to change back later (to avoid passing around
// sfs_ID instead of a ptr

void
p2p::doRealRPC (sfs_ID ID, int procno, const void *in, void *out,
		      aclnt_cb cb)
{

  if (lookups_outstanding > 0) lookup_RPCs++;
 
  location *l = locations[ID];
  assert (l);
  assert (l->alive);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  l->nout++;
  if (l->c) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));
    l->c->call (procno, in, out, wrap(mkref(this), &p2p::timing_cb, cb, l, start));
  } else {
    // If we are in the process of connecting; we should wait
    // warn << "going to connect to " << ID << " ; nout=" << l->nout << "\n";
    doRPC_cbstate *st = New doRPC_cbstate (procno, in, out, wrap(mkref(this), &p2p::timing_cb, cb, l, start));
    l->connectlist.insert_tail (st);
    if (!l->connecting) {
      l->connecting = true;
      tcpconnect (l->addr.hostname, l->addr.port, wrap (mkref (this), &p2p::connect_cb,
						l));
    }
  }
}
