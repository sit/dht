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

#include "dhash_common.h"
#include "dhash_impl.h"
#include "dhashcli.h"

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
dhash_impl::dofetchrec (user_args *sbp, dhash_fetchrec_arg *arg)
{    
  blockID id (arg->key, arg->ctype, arg->dbtype);

  strbuf header;
  header << host_node->my_ID () << ": dofetchrec (" << id << "): ";

  // First attempt to directly return the block if we have it.
  // XXX Really should have a better way of knowing when we have
  //     the entire block, as opposed to a fragment of something.
  if (id.ctype == DHASH_KEYHASH && key_status (id) != DHASH_NOTPRESENT) {
    trace << header << "key present!\n";
    dofetchrec_local (sbp, arg);
    return;
  }

  // Otherwise, it's routing time.  Derived from recroute.C (1.12)'s dorecroute
  vec<ptr<location> > cs = host_node->succs ();
  
  chordID myID = host_node->my_ID ();
  vec<float> mycoords = host_node->my_location ()->coords ();
  u_long m = dhash::num_efrags ();

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
#if 0    
    else if ((int)m - (int)overlap < (int)cs.size ()) {
      // Override the absolute best we could've done, which probably
      // is the predecessor since our succlist spans the key, and
      // select someone nice and fast to get more successors from.
      float mindist = -1.0;
      size_t minind = 0;

      size_t start = m - overlap;
      trace << header << "going to choose a distance from ";
      for (size_t i = start; i < cs.size (); i++) {
	float dist = Coord::distance_f (mycoords,
					cs[i]->coords ());
	warn << cs[i]->id () << "at " << (int)dist << " ";
	if (mindist < 0 || dist < mindist) {
	  mindist = dist;
	  minind  = i;
	}
      }
      trace << " i chose " << cs[minind]->id () << " at " << (int)mindist << "\n";;
      if (minind < succind) {
	p = cs[minind];
      } else {
	ptr<location> nexthop = cs[minind];
	trace << header << "going for penult from " << nexthop->id () << "\n";
	cs.popn_front (succind); // just the overlap please
	
	// XXX do this weird stop early thing here too?
	fatal << "XXX";
	// dorecroute_sendpenult (ra, nexthop, p, cs);
	return;
      }
    }
#endif /* 0 */    
  }
  dofetchrec_nexthop (sbp, arg, p);
}

void
dhash_impl::dofetchrec_nexthop (user_args *sbp, dhash_fetchrec_arg *arg,
				ptr<location> p)
{
  blockID id (arg->key, arg->ctype, arg->dbtype);
  trace << host_node->my_ID () << ": dofetchrec_nexthop (" << id << ") to "
	<< p->id () << "\n";
  ptr<dhash_fetchrec_arg> farg = New refcounted<dhash_fetchrec_arg> ();
  ptr<dhash_fetchrec_res> fres = New refcounted<dhash_fetchrec_res> (DHASH_OK);

  farg->key    = arg->key;
  farg->ctype  = arg->ctype;
  farg->dbtype = arg->dbtype;
  farg->path.setsize (arg->path.size () + 1);
  for (size_t i = 0; i < arg->path.size (); i++)
    farg->path[i] = arg->path[i];
  host_node->my_location ()->fill_node (farg->path[arg->path.size ()]);
  
  doRPC (p, dhash_program_1, DHASHPROC_FETCHREC,
	 farg, fres,
	 wrap (this, &dhash_impl::dofetchrec_nexthop_cb, sbp, arg, fres));	 
}

void
dhash_impl::dofetchrec_nexthop_cb (user_args *sbp, dhash_fetchrec_arg *arg,
				   ptr<dhash_fetchrec_res> res,
				   clnt_stat err)
{
  blockID id (arg->key, arg->ctype, arg->dbtype);
  trace << host_node->my_ID () << ": dofetchrec_nexthop (" << id
	<< ") returned " << err << ".\n";

  if (err) {
    // XXX actually, we should do something like retry and try to
    //     find someone else to ask.
    dhash_fetchrec_res rres (DHASH_RPCERR);
    rres.resdef->path.setsize (arg->path.size () + 1);
    for (size_t i = 0; i < arg->path.size (); i++)
      rres.resdef->path[i] = arg->path[i];
    host_node->my_location ()->fill_node (rres.resdef->path[arg->path.size ()]);
    
    sbp->reply (&rres);
    sbp = NULL;
    return;
  }
  // Just pass the reply we got from our next hop back to the previous hop.
  sbp->reply (res);
  sbp = NULL;
  return;
}

void
dhash_impl::dofetchrec_local (user_args *sbp, dhash_fetchrec_arg *arg)
{
  blockID id (arg->key, arg->ctype, arg->dbtype);
  ptr<dbrec> b = dblookup (id); assert (b != NULL);
  dhash_fetchrec_res res (DHASH_OK);
  
  res.resok->res.setsize (b->len);
  memcpy (res.resok->res.base (), b->value, b->len);
  res.resok->fetch_time = 0;
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
  blockID id (arg->key, arg->ctype, arg->dbtype);
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

  trace << header << "fetch returned " << dhasherr2str (s) << "\n";
  switch (s) {
  case DHASH_OK:
    {
      res.resok->res.setsize (b->len);
      memcpy (res.resok->res.base (), b->data, b->len);
      res.resok->fetch_time = b->times.pop_back ();
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
