#include "chord.h"

/*
 *
 * comm.C 
 *
 * This code deals with the actual communication to nodes.
 *
 */

#ifdef STATS
chord_stats stats;
#endif /* STATS */

// creates an array of edges of a fake network
// stores in int* edges
// after this function numnodes holds the number
// of nodes in the fake network
// NOTE: may later want to change file input name
void 
p2p::initialize_graph() 
{
  FILE *graphfp;
  int cur_node, to_node, length,i,j;
  int done = 0;
  graphfp = fopen("gg","r");
  if(graphfp == NULL)
    printf("EEK-- didn't open input file correctly\n");
  // first line of file is number of nodes
  fscanf(graphfp,"%d",&numnodes); 
  // create space for a numnodes by numnodes edge array
  edges = (int *)calloc(numnodes*numnodes,sizeof(int));
  // initialize edge array as -1 for all accept i,i entries
  for(i=0;i<numnodes;i++)
    for(j=0;j<numnodes;j++)
      if(i == j)
	*(edges+i*numnodes+j) = 0;
      else
	*(edges+i*numnodes+j) = -1;
  // more nodes to find the edges of
  while(fscanf(graphfp,"%d",&cur_node) != EOF && done == 0) 
    {
      fscanf(graphfp,"%d%d",&to_node,&length);
      *(edges+cur_node*numnodes + to_node) = length;
      // while there are more edges for this node
      while(fscanf(graphfp,"%d",&to_node) != EOF && to_node != -1) 
	{
	  fscanf(graphfp,"%d",&length);
	  *(edges+cur_node*numnodes + to_node) = length;
	}
      if(to_node != -1) // reached end of file
	done = 1;
    }
  fclose(graphfp);
}


void
p2p::timeout(location *l) {
  // warn << "timeout on " << l->n << " closing socket\n";
  if (l->nout == 0) l->x = NULL;
  else
    {
      // warn << "timeout on node " << l->n << " has overdue RPCs\n";
      l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));
    }
}

void
p2p::timing_cb(aclnt_cb cb, location *l, ptr<struct timeval> start, clnt_stat err) 
{
  struct timeval now;
  gettimeofday(&now, NULL);
  long lat = ((now.tv_sec - start->tv_sec)*1000000 + (now.tv_usec - start->tv_usec))/1000;
  l->total_latency += lat;
  l->num_latencies++;
  if (lat > l->max_latency) l->max_latency = lat;
  l->nout--;
  (*cb)(err);

}

// just add a time delay to represent distance
void
p2p::doRPC (sfs_ID &ID, rpc_program progno, int procno, 
	    const void *in, void *out,
	    aclnt_cb cb)
{
  // will IDs map to node numbers?? (ie. if ID is 
  // something diff then this may give weird results)

  // get "distance" between self and destination
  int time = 0;

#ifdef _SIM_ 
  int dist = *(edges+(int)myID.getsi()*numnodes+(int)ID.getsi());
  time = dist*10; // Not sure how to scale time delay
  // should not be delayed if not simulating
  timecb_t* decb =  delaycb (time, 0, 
			     wrap(mkref (this), &p2p::doRealRPC, ID, 
				  progno, procno, in, out, cb));
#else
  doRealRPC (ID, progno, procno, in, out, cb);
#endif
}

// NOTE: now passing ID by value instead of referencing it...
// (getting compiler errors before). now seems ok
// May want to try to change back later (to avoid passing around
// sfs_ID instead of a ptr

void
p2p::doRealRPC (sfs_ID ID, rpc_program progno, int procno, const void *in, void *out,
		aclnt_cb cb)
{
  
  if (lookups_outstanding > 0) lookup_RPCs++;
  
  location *l = locations[ID];
  assert (l);
  assert (l->alive);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  l->nout++;
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));
    ptr<aclnt> c = aclnt::alloc(l->x, progno);
    c->call (procno, in, out, wrap(mkref(this), &p2p::timing_cb, cb, l, start));
  } else {
    doRPC_cbstate *st = 
      New doRPC_cbstate (progno, procno, in, out,  
			 wrap(mkref(this), &p2p::timing_cb, cb, l, start));
    l->connectlist.insert_tail (st);
    if (!l->connecting) {
      l->connecting = true;
      chord_connect(ID, wrap (mkref (this), &p2p::dorpc_connect_cb, l));
    }
  }
}

void
p2p::dorpc_connect_cb(location *l, ptr<axprt_stream> x) {

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
  l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));

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
  
  warn << "chord connect\n";
  location *l = locations[ID];
  assert (l);
  assert (l->alive);
  ptr<struct timeval> start = new refcounted<struct timeval>();
  gettimeofday(start, NULL);
  if (l->x) {    
    timecb_remove(l->timeout_cb);
    l->timeout_cb = delaycb(30,0,wrap(this, &p2p::timeout, l));
    (*cb)(l->x);
  } else {
    tcpconnect (l->addr.hostname, l->addr.port, wrap (mkref (this), &p2p::connect_cb, cb));
  }
}


void
p2p::connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd)
{

  warn << "connect cb\n";
  if (fd < 0) {
    (*cb)(NULL);
  } else {
    ptr<axprt_stream> x = axprt_stream::alloc(fd);
    (*cb)(x);
  }
}


void
p2p::stats_cb () {

#ifdef STATS
  warnx << "Chord Node " << myID << " status\n";

  warnx << "Core layer stats\n";
  warnx << "  Per link avg. RPC latencies\n";
  location *l = locations.first ();
  while (l) {
    warnx << "link " << l->n << "\n";
    printf("    Average latency: %f\n", ((float)(l->total_latency))/l->num_latencies);
    printf("    Max latency: %d\n", l->max_latency);
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
  printf( "   %f average hops per insert\n", ((float)(stats.insert_path_len))/stats.insert_ops);
  warnx << "   " << stats.lookup_ops << " lookups\n";
  printf( "   %f average hops per lookup\n", ((float)(stats.insert_path_len))/stats.insert_ops);
#endif /* STATS */

}
