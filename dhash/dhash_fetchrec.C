/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *                     Frans Kaashoek (kaashoek@lcs.mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
#include <arpc.h>

#include <chord.h>
#include <dbfe.h>

#include "dhash_common.h"
#include "dhash_impl.h"
#include "dhashcli.h"

#include "dhblock.h"
#include "dhblock_srv.h"
#include "dhblock_chash.h"

#include <location.h>
#include <locationtable.h>

#include <id_utils.h>
#include <coord.h>

#ifdef DMALLOC
#include <dmalloc.h>
#endif

#include <modlogger.h>
#define warning modlogger ("dhash_fetchrec", modlogger::WARNING)
#define info  modlogger ("dhash_fetchrec", modlogger::INFO)
#define trace modlogger ("dhash_fetchrec", modlogger::TRACE)


void
dhashcli::dofetchrec_execute (blockID b, cb_ret cb)
{
  chordID myID = clntnode->my_ID ();

  ptr<dhash_fetchrec_arg> arg = New refcounted<dhash_fetchrec_arg> ();
  arg->key    = b.ID;
  arg->ctype  = b.ctype;

  vec<chordID> failed;
  ptr<location> s = clntnode->closestpred (b.ID, failed);
  trace << myID << ": dofetchrec_execute (" << b << ") -> " << s->id () << "\n";
  
  ptr<dhash_fetchrec_res> res = New refcounted<dhash_fetchrec_res> (DHASH_OK);
  timespec start;
  clock_gettime (CLOCK_REALTIME, &start);
  clntnode->doRPC (s, dhash_program_1, DHASHPROC_FETCHREC,
		   arg, res,
		   wrap (this, &dhashcli::dofetchrec_cb, start, b, cb, res));
}

void
dhashcli::dofetchrec_cb (timespec start, blockID b, cb_ret cb,
			 ptr<dhash_fetchrec_res> res, clnt_stat s)
{
  strbuf prefix;
  prefix << clntnode->my_ID () << ": dofetchrec_execute (" << b << "): ";
  trace << prefix << "returned " << s << "\n";
  if (s) {
    route r;
    (*cb) (DHASH_RPCERR, NULL, r);
    return;
  }
  
  route r;
  rpc_vec<chord_node_wire, RPC_INFINITY> *path = (res->status == DHASH_OK) ?
    &res->resok->path :
    &res->resdef->path;
  for (size_t i = 0; i < path->size (); i++) {
    ptr<location> l = clntnode->locations->lookup_or_create
      (make_chord_node ((*path)[i]));
    assert (l != NULL);
    r.push_back (l);
  }
  
  if (res->status != DHASH_OK) {
    trace << prefix << "returned " << res->status << "\n";
    // XXX perhaps one should do something cleverer here.
      (*cb) (res->status, NULL, r);
    return;
  }

  // Okay, we have the block.  Massage it into a dhash_block

  ptr<dhash_block> blk = New refcounted<dhash_block> (res->resok->res.base (),
						      res->resok->res.size (),
						      b.ctype);
  blk->ID = b.ID;
  blk->hops = res->resok->path.size ();
  blk->errors = 0;  // XXX
  blk->retries = 0; // XXX

  // We can't directly measure the time it took for a lookup.
  // However, the remote node does report back to us the amount of time
  // it took to reconstruct the block.  Since there are only two phases,
  // we can use this time to construct the actual time required.
  timespec finish;
  clock_gettime (CLOCK_REALTIME, &finish);
  timespec total = finish - start;
  blk->times.push_back (total.tv_sec * 1000 + int (total.tv_nsec/1000000));

  u_int32_t othertimes = 0;
  for (size_t i = 0; i < res->resok->times.size (); i++) {
    blk->times.push_back (res->resok->times[i]);
    othertimes += res->resok->times[i];
  }
  blk->times[0] -= othertimes;

  (*cb) (DHASH_OK, blk, r);
  return;
}

void
dhash_impl::dofetchrec (user_args *sbp, dhash_fetchrec_arg *arg)
{    
  blockID id (arg->key, arg->ctype);

  strbuf header;
  header << host_node->my_ID () << ": dofetchrec (" << id << "): ";

  // First attempt to directly return the block if we have it.
  // XXX Really should have a better way of knowing when we have
  //     the entire block, as opposed to a fragment of something.
  if (id.ctype == DHASH_KEYHASH && blocksrv[DHASH_KEYHASH] &&
      blocksrv[DHASH_KEYHASH]->key_present (id)) {
    trace << header << "key present!\n";
    dofetchrec_local (sbp, arg);
    return;
  }

  // Otherwise, it's routing time.  Derived from recroute.C (1.12)'s dorecroute
  vec<ptr<location> > cs = host_node->succs ();
  
  chordID myID = host_node->my_ID ();
  Coord mycoords = host_node->my_location ()->coords ();
  u_long m = dhblock_chash::num_efrags ();

  vec<chordID> failed;
  ptr<location> p = host_node->closestpred (arg->key, failed);
  // Update best guess or complete, depending, if successor is in our
  // successor list.
  if (betweenrightincl (myID, cs.back ()->id (), arg->key)) {
    // Calculate the amount of overlap available in the successor list
    size_t overlap = 0;
    size_t succind = 0;
    for (size_t i = 0; i < cs.size (); i++) {
      if (betweenrightincl (myID, cs[i]->id (), arg->key)) {
	// The i+1st successor is the key's successor!
	overlap = cs.size () - i;
	succind = i;
	break;
      }
    }
    trace << header << "overlap = " << overlap << " / m = " << m << "\n";

    // Try to decide who to talk to next.
    if (overlap >= m) {
      // Enough overlap to finish. XXX check succ_list_shaving?
      cs.popn_front (succind); // leave succind+1st succ at front
      if (succind > 0)
	trace << header << "skipping " << succind << " nodes.\n";

      dofetchrec_assembler (sbp, arg, cs);
      return;
    }
    else if ((int)m - (int)overlap < (int)cs.size ()) {
      // Override the absolute best we could've done, which probably
      // is the predecessor since our succlist spans the key, and
      // select someone nice and fast to get more successors from.
      float mindist = -1.0;
      size_t minind = 0;

      size_t start = m - overlap;
      strbuf distbuf;
      distbuf << "going to choose a distance from ";
      for (size_t i = start; i < cs.size (); i++) {
	float dist = Coord::distance_f (mycoords,
					cs[i]->coords ());
	distbuf << cs[i]->id () << "(" << (int)dist << ") ";
	if (mindist < 0 || dist < mindist) {
	  mindist = dist;
	  minind  = i;
	}
      }
      if (minind < succind) {
	distbuf << "; i chose " << cs[minind]->id ()
		<< "(" << (int)mindist << ")\n";;
	trace << header << distbuf;
	p = cs[minind];
      }
#if 0      
      else {
	ptr<location> nexthop = cs[minind];
	trace << header << "going for penult from " << nexthop->id () << "\n";
	cs.popn_front (succind); // just the overlap please
	
	// XXX do this weird stop early thing here too?
	fatal << "XXX";
	// dorecroute_sendpenult (ra, nexthop, p, cs);
	return;
      }
#endif /* 0 */    
    }
  }
  dofetchrec_nexthop (sbp, arg, p);
}

void
dhash_impl::dofetchrec_nexthop (user_args *sbp, dhash_fetchrec_arg *arg,
				ptr<location> p)
{
  blockID id (arg->key, arg->ctype);
  trace << host_node->my_ID () << ": dofetchrec_nexthop (" << id << ") to "
	<< p->id () << "\n";
  ptr<dhash_fetchrec_arg> farg = New refcounted<dhash_fetchrec_arg> ();
  ptr<dhash_fetchrec_res> fres = New refcounted<dhash_fetchrec_res> (DHASH_OK);

  farg->key    = arg->key;
  farg->ctype  = arg->ctype;
  farg->path.setsize (arg->path.size () + 1);
  for (size_t i = 0; i < arg->path.size (); i++)
    farg->path[i] = arg->path[i];
  host_node->my_location ()->fill_node (farg->path[arg->path.size ()]);

  timespec localstart;
  clock_gettime (CLOCK_REALTIME, &localstart);
  doRPC (p, dhash_program_1, DHASHPROC_FETCHREC,
	 farg, fres,
	 wrap (this, &dhash_impl::dofetchrec_nexthop_cb, sbp, arg, fres,
	       localstart));
}

void
dhash_impl::dofetchrec_nexthop_cb (user_args *sbp, dhash_fetchrec_arg *arg,
				   ptr<dhash_fetchrec_res> res,
				   timespec start,
				   clnt_stat err)
{
  blockID id (arg->key, arg->ctype);
  trace << host_node->my_ID () << ": dofetchrec_nexthop (" << id
	<< ") returned " << err << ".\n";
  
  timespec localfinish, diff;
  clock_gettime (CLOCK_REALTIME, &localfinish);
  diff = localfinish - start;
  
  if (err) {
    // XXX actually, we should do something like retry and try to
    //     find someone else to ask.
    dhash_fetchrec_res rres (DHASH_RPCERR);

    rres.resdef->times.setsize (2);
    rres.resdef->times[0] = diff.tv_sec * 1000 +
      int (diff.tv_nsec / 1000000);
    rres.resdef->times[1] = 0; // fetch time 
    
    rres.resdef->path.setsize (arg->path.size () + 1);
    for (size_t i = 0; i < arg->path.size (); i++)
      rres.resdef->path[i] = arg->path[i];
    host_node->my_location ()->fill_node (rres.resdef->path[arg->path.size ()]);
    
    sbp->reply (&rres);
    sbp = NULL;
    return;
  }
  
  // Just pass the reply we got from our next hop back to the previous hop,
  // Except for inserting the RTT it took to get from there and back...

  rpc_vec<u_int32_t, RPC_INFINITY> *rtimes = (res->status == DHASH_OK) ?
    &res->resok->times :
    &res->resdef->times;
  vec<u_int32_t> times;
  u_int32_t othertime = 0;
  for (size_t i = 0; i < rtimes->size (); i++) {
    times.push_back ((*rtimes)[i]);
    othertime += (*rtimes)[i];
  }
  rtimes->setsize (rtimes->size () + 1);
  (*rtimes)[0] = diff.tv_sec * 1000 + int (diff.tv_nsec / 1000000) - othertime;
  for (size_t i = 1; i < rtimes->size (); i++) {
    (*rtimes)[i] = times.pop_front ();
  }
  assert (times.size () == 0);

  sbp->reply (res);
  sbp = NULL;
  return;
}

void
dhash_impl::dofetchrec_local (user_args *sbp, dhash_fetchrec_arg *arg)
{
  ptr<dhblock_srv> s = blocksrv[arg->ctype];
  ptr<dbrec> b = s->fetch (arg->key); assert (b != NULL);
  dhash_fetchrec_res res (DHASH_OK);
  
  res.resok->res.setsize (b->len);
  memcpy (res.resok->res.base (), b->value, b->len);
  
  res.resok->times.setsize (1);
  res.resok->times[0] = 0; // local assembly is fast.
  
  res.resok->path.setsize (arg->path.size () + 1);
  for (size_t i = 0; i < arg->path.size (); i++) {
    res.resok->path[i] = arg->path[i];
  }
  host_node->my_location ()->fill_node (res.resok->path[arg->path.size()]);
  sbp->reply (&res);
  sbp = NULL;
}

void
dhash_impl::dofetchrec_assembler (user_args *sbp, dhash_fetchrec_arg *arg,
				  vec<ptr<location> > succs)
{
  blockID id (arg->key, arg->ctype);
  trace << host_node->my_ID () << ": dofetchrec_assembler (" << id << ")\n";

  vec<chord_node> sl;
  for (size_t i = 0; i < succs.size (); i++) {
    chord_node n;
    succs[i]->fill_node (n);
    sl.push_back (n);
  }
  route r;
  cli->assemble (id,
		 wrap (this, &dhash_impl::dofetchrec_assembler_cb,
		       sbp, arg),
		 sl,
		 r);
}

void
dhash_impl::dofetchrec_assembler_cb (user_args *sbp, dhash_fetchrec_arg *arg,
				     dhash_stat s, ptr<dhash_block> b, route r)
{
  strbuf header;
  header << host_node->my_ID () << ": dofetchrec_assembler_cb ("
	 << arg->key << "): ";

  // We can ignore the route here, because passing it around was just for
  // the iterative case; our route is included in the arg packet;
  // This r is the dummy we threw in in _assembler.

  dhash_fetchrec_res res (s);

  trace << header << "fetch returned " << s << "\n";
  switch (s) {
  case DHASH_OK:
    {
      res.resok->res.setsize (b->data.len ());
      memcpy (res.resok->res.base (), b->data.cstr (), b->data.len ());
      
      res.resok->times.setsize (1);
      res.resok->times[0] = b->times.pop_back ();
      
      res.resok->path.setsize (arg->path.size () + 1);
      for (size_t i = 0; i < arg->path.size (); i++) {
	res.resok->path[i] = arg->path[i];
      }
      host_node->my_location ()->fill_node (res.resok->path[arg->path.size()]);
    }
    break;
  case DHASH_CHORDERR: // Fall through
  case DHASH_NOENT:
  default:
    {
      res.resdef->path.setsize (arg->path.size () + 1);
      for (size_t i = 0; i < arg->path.size (); i++) {
	res.resdef->path[i] = arg->path[i];
      }
      host_node->my_location ()->fill_node (res.resdef->path[arg->path.size()]);
    }
    break;
  }

  sbp->reply (&res);
  sbp = NULL;
  return;
}
