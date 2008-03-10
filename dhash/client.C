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

#include <sys/time.h>
#include <stdlib.h>

#include <arpc.h>
#include <crypt.h>
#include <qhash.h>

#include <chord_types.h>
#include <chord.h>
#include <route.h>
#include <configurator.h>
#include <location.h>
#include <locationtable.h>

#include "dhash_common.h"
#include "dhash.h"
#include "dhashcli.h"
#include "dhblock.h"
#include "download.h"
#include "dhash_store.h"

#include <coord.h>
#include <misc_utils.h>
#include <modlogger.h>

// HACK global indicator variable for whether or not to
//      transfer data over TCP pipes.
bool dhash_tcp_transfers = false;

#define warning modlogger ("dhashcli", modlogger::WARNING)
#define info  modlogger ("dhashcli", modlogger::INFO)
#define trace modlogger ("dhashcli", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif
#include <ida.h>

static void
order_succs (ptr<locationtable> locations,
	     const Coord &me, const vec<chord_node> &succs,
	     vec<chord_node> &out, u_long max = 0);

static void
filter_succs (const vec<chord_node> &succs, vec<chord_node> &out)
{
  bhash<str> hosts;
  assert (succs.size () > 0);
  out.clear ();
  for (size_t i = 0; i < succs.size (); i++) {
    strbuf h; h << succs[i].r;
    if (!hosts[h]) {
      out.push_back (succs[i]);
      hosts.insert (h);
    }
  }
}

static struct dhashcli_config_init {
  dhashcli_config_init ();
} dcci;

dhashcli_config_init::dhashcli_config_init ()
{
  bool ok = true;

#define set_int Configurator::only ().set_int

  /** Whether or not to order successors by expected latency */
  ok = ok && set_int ("dhashcli.order_successors", 1);
  /** Default expiration time for objects */
  ok = ok && set_int ("dhash.default_lifetime", -1);

  assert (ok);
#undef set_int
}


// ---------------------------------------------------------------------------
// DHASHCLI

dhashcli::dhashcli (ptr<vnode> node, ptr<dhash> dh)
  : clntnode (node), ordersucc_ (true), default_lifetime (-1), dh (dh)
{
  int ordersucc = 1;
  Configurator::only ().get_int ("dhashcli.order_successors", ordersucc);
  ordersucc_ = (ordersucc > 0);

  (void) Configurator::only ().get_int ("dhash.default_lifetime",
      default_lifetime);
}


void
dhashcli::retrieve (blockID blockID, cb_ret cb, int options, 
		    ptr<chordID> guess)
{
  chordID myID = clntnode->my_ID ();

  trace << myID << ": retrieve (" << blockID << "): new retrieve.\n";
  
  
  ptr<rcv_state> rs = New refcounted<rcv_state> (blockID, cb);
  
  ptr<dhblock> block = allocate_dhblock (blockID.ctype);

  if (guess &&
      (options & (DHASHCLIENT_GUESS_SUPPLIED|DHASHCLIENT_SKIP_LOOKUP)) ==
       (DHASHCLIENT_GUESS_SUPPLIED|DHASHCLIENT_SKIP_LOOKUP)) 
  {
    ptr<location> l = clntnode->locations->lookup (*guess);
    chord_node guessnode;
    l->fill_node (guessnode);
    vec<chord_node> fakesuccs;
    fakesuccs.push_back (guessnode);
    route fakeroute;
    fakeroute.push_back (l);
    delaycb (0, wrap (this,
      &dhashcli::retrieve_lookup_cb, rs, block, fakesuccs, fakeroute, CHORD_OK));
  } else {
    // We would like to obtain enough successors to provide maximal
    // choice to the client when doing the expensive fetch phase.
    // Unfortunately, we are currently hurt by the fact that there
    // are holes in our successor list: nodes without the block
    // that Chord can't tell us about.  Maybe we need up-calls.
    clntnode->find_succlist (block->id_to_dbkey (blockID.ID),
      block->num_fetch (),
      wrap (this, &dhashcli::retrieve_lookup_cb,rs,block),
      guess);
  }
}



void
dhashcli::retrieve_lookup_cb (ptr<rcv_state> rs, ptr<dhblock> block,
			      vec<chord_node> succs,
			      route r, chordstat status)
{
  chordID myID = clntnode->my_ID ();
  rs->timemark ();
  rs->r = r;
  
  if (status) {
    trace << myID << ": retrieve (" << rs->key 
          << "): lookup failure: " << status << "\n";
    rs->complete (DHASH_CHORDERR, NULL); // failure
    rs = NULL;
    return;
  }
  strbuf s;
  s << myID << ": retrieve_verbose (" << rs->key << "): route";
  for (size_t i = 0; i < r.size (); i++)
    s << " " << r[i]->id ();
  s << "\n";
  trace << s;
  s.tosuio ()->clear ();
  s << myID << ": retrieve_verbose (" << rs->key << "): succs";
  for (size_t i = 0; i < succs.size (); i++)
    s << " " << succs[i].x;
  s << "\n";
  trace << s;

  doassemble (rs, block, succs);

}

void
dhashcli::doassemble (ptr<rcv_state> rs, ptr<dhblock> block, 
		      vec<chord_node> succs)
{
  chordID myID = clntnode->my_ID ();

  if (succs.size () < block->min_fetch ()) {
    warning << myID << ": retrieve (" << rs->key << "): "
	    << "insufficient number of successors returned!\n";
    rs->complete (DHASH_CHORDERR, NULL); // failure
    rs = NULL;
    return;
  }

  vec<chord_node> unique_succs;
  filter_succs (succs, unique_succs);

  if (ordersucc_) {
    ptr<locationtable> lt = NULL;
    if (rs->succopt)
      lt = clntnode->locations;
    
    // Store list of successors ordered by expected distance.
    // fetch_frag will pull from this list in order.
    order_succs (lt, clntnode->my_location ()->coords (),
		 unique_succs, rs->succs, block->num_put ());
  } else {
    rs->succs = unique_succs;
  }

  // Dispatch min_fetch parallel requests, even though we don't know
  // how many fragments will truly be needed.
  u_int tofetch = block->min_fetch () + 0;
  if (tofetch > rs->succs.size ()) tofetch = rs->succs.size ();
  for (u_int i = 0; i < tofetch; i++)
    fetch_frag (rs, block);
}

void
dhashcli::fetch_frag (ptr<rcv_state> rs, ptr<dhblock> b)
{
  if (rs->completed) return;
  register size_t i = rs->nextsucc;

  chordID myID = clntnode->my_ID ();

  // Ugh. No more successors available.
  if (i >= rs->succs.size ()) {
    // If there are outstanding fragments, there is still hope.
    // XXX Actually, this is a lie. If we know that they will not
    //     provide us with enough total fragments to reconstruct the
    //     block,  e.g. incoming_rpcs + frags.size < NUM_DFRAGS,
    //     we should just give up to the user now.  However for
    //     book keeping purposes, we don't do this.
    if (rs->incoming_rpcs > 0)
      return;
    
    // Should we try harder? Like, try and get more successors and
    // check out the swath? No, let's just fail and have the higher
    // level know that they should retry.
    trace << myID << ": retrieve (" << rs->key 
          << "): out of successors; failing.\n";
    rs->complete (DHASH_NOENT, NULL);
    rs = NULL;
    return;
  }
  
  rs->incoming_rpcs += 1;

  dhash_download::execute (clntnode, dh, rs->succs[i], 
			   blockID(rs->key.ID, rs->key.ctype),
			   wrap (this, &dhashcli::retrieve_fetch_cb, rs, i, b),
			   wrap (this, &dhashcli::on_timeout, rs, b));
  rs->nextsucc += 1;
}

bool
dhashcli::on_timeout (ptr<rcv_state> rs, 
		      ptr<dhblock> b,
		      chord_node dest,
		      int retry_num) 
{
  trace << clntnode->my_ID () << ": retrieve (" << rs->key
	<< "): timeout " << retry_num << " on " << dest << "\n";
  if (retry_num == 0)
    fetch_frag (rs, b);
  return false;
}

void
dhashcli::retrieve_fetch_cb (ptr<rcv_state> rs, u_int i,
			     ptr<dhblock> block_t,
			     ptr<dhash_block> block)
{
  chordID myID = clntnode->my_ID ();

  rs->incoming_rpcs -= 1;
  if (rs->completed) {
    // Here it might just be that we got a fragment back after we'd
    // already gotten enough to reconstruct the block.
    trace << myID << ": retrieve (" << rs->key
          << "): unexpected fragment from " << rs->succs[i] << ", discarding.\n";
    return;
  }

  if (!block) {
    trace << myID << ": retrieve (" << rs->key
	  << "): failed from successor " << rs->succs[i] << "\n";
    rs->errors++;
    fetch_frag (rs, block_t);
    return;
  }

  trace << myID << ": retrieve_verbose (" << rs->key << "): read from "
	<< rs->succs[i].x << "\n";
    
  
#ifdef VERBOSE_LOG  
  bigint h = compute_hash (block->data, block->len);
  trace << myID << ": retrieve (" << rs->key << ") got frag " << i
	<< " with hash " << h << " " << res->compl_res->res.size () << "\n";
#endif /* VERBOSE_LOG */
  
  int err = block_t->process_download (rs->key, block->data);
  if (err) {
    rs->errors++;
    trace << myID << ": retrieve_verbose (" << rs->key << "): err \n";
    fetch_frag (rs, block_t);
  }

  if (block_t->done ()) {
    rs->timemark ();
    str data = block_t->produce_block_data ();
    ptr<dhash_block> ret_block = New refcounted<dhash_block> (data,
							      rs->key.ctype);
    ret_block->ID = rs->key.ID;
    ret_block->expiration = block->expiration;
    ret_block->hops = rs->r.size ();
    ret_block->errors = rs->errors;
    ret_block->retries = block->errors;
    
    for (size_t i = 1; i < rs->times.size (); i++) {
      timespec diff = rs->times[i] - rs->times[i - 1];
      ret_block->times.push_back (diff.tv_sec * 1000 +
			      int (diff.tv_nsec/1000000));
    }
    
    rs->complete (DHASH_OK, ret_block);
    rs = NULL;
  } else {
    if (rs->incoming_rpcs == 0 && rs->nextsucc > rs->succs.size ()) {
      info << myID << ": retrieve (" << rs->key << "): all RPCs returned but no good block.\n";
      rs->complete (DHASH_NOENT, NULL);
      rs = NULL;
    }
  }
}


struct orderer {
  float d_;
  size_t i_;
  static int cmp (const void *a_, const void *b_) {
    const orderer *a = (orderer *) a_, *b = (orderer *) b_;
    return (int) (a->d_ - b->d_);
  }
};

static void
order_succs (ptr<locationtable> locations,
	     const Coord &me, const vec<chord_node> &succs,
	     vec<chord_node> &out, u_long max)
{

  //max is the number of successors we should order: the first max in the list
  u_long lim = max;
  // 0 means order them all
  if (max == 0 || max > succs.size ()) lim = succs.size ();

  orderer *d2me = New orderer [succs.size()];
  for (size_t i = 0; i < lim; i++) {
    ptr<location> l = NULL;
    if (locations) 
      l = locations->lookup (succs[i].x);
    if (l) {
      // Have actual measured latencies, so might as well use them.
      d2me[i].d_ = l->distance ();
    } else {
      Coord cursucc (succs[i]);
      d2me[i].d_ = Coord::distance_f (me, cursucc);
    }
    d2me[i].i_ = i;
  }
  qsort (d2me, lim, sizeof (*d2me), &orderer::cmp);
  out.clear ();
  for (size_t i = 0; i < lim; i++) {
#ifdef VERBOSE_LOG
    char buf[10]; // argh. please shoot me.
    sprintf (buf, "%5.2f", d2me[i].d_);
    modlogger ("orderer", modlogger::TRACE) << d2me[i].i_ << " "
					    << succs[d2me[i].i_] << " "
					    << buf << "\n";
#endif /* VERBOSE_LOG */    
    out.push_back (succs[d2me[i].i_]);
  }
  delete[] d2me;

  //copy any of the ones we didn't consider verbatim
  if (max == 0) return;
  long remaining = succs.size () - lim;
  if (remaining > 0) 
    for (int i = 0; i < remaining; i++)
      out.push_back (succs[lim+i]);

}



// Pull block fragments down from a successor list
// public interface, used by dhash_impl if completing a FETCHREC
void
dhashcli::assemble (blockID blockID, cb_ret cb, vec<chord_node> succs, route r)
{
  ptr<rcv_state> rs = New refcounted<rcv_state> (blockID, cb);
  rs->r = r;
  if (dhash_tcp_transfers)
    rs->succopt = true;
  ptr<dhblock> blk = allocate_dhblock (blockID.ctype);
  doassemble (rs, blk, succs);
}

//---------------------------- insert -----------------------

void
dhashcli::insert (ref<dhash_block> block, cbinsert_path_t cb, 
		  int options, ptr<chordID> guess)
{
  ptr<location> l = NULL;
  if (guess) 
    l =  clntnode->locations->lookup (*guess);
  if (!(options & DHASHCLIENT_EXPIRATION_SUPPLIED))
    block->expiration = (default_lifetime < 0) ?
      0 :
      (timenow + default_lifetime);

  if (!l) 
    lookup (block->ID, wrap (this, &dhashcli::insert_lookup_cb, 
			     block, cb, options));
  else 
    clntnode->get_succlist (l, wrap (this, &dhashcli::insert_succlist_cb, 
				     block, cb, *guess, options));
  
}

void
dhashcli::insert_succlist_cb (ref<dhash_block> block, cbinsert_path_t cb,
			      chordID guess, int options,
			      vec<chord_node> succs, chordstat status)
{
  if (status) {
    vec<chordID> rrr;
    cb (DHASH_CHORDERR, rrr);
    info << "insert_succlist_cb: failure (" << block->ID << "): "
	 << status << "\n";
    return;
  }

  route r;
  r.push_back (clntnode->locations->lookup (guess));
  insert_lookup_cb (block, cb, options, DHASH_OK, succs, r);
}

void
dhashcli::insert_lookup_cb (ref<dhash_block> block, cbinsert_path_t cb, 
			    int options, dhash_stat status, 
			    vec<chord_node> succs, route r)
{
  vec<chordID> mt;
  if (status) {
    (*cb) (DHASH_CHORDERR, mt);
    return;
  }

  //XXX run an allocator 
  ptr<dhblock> blk = allocate_dhblock (block->ctype);

  if (succs.size () < blk->min_put ()) {
    // this is a failure condition, since we can't hope to reconstruct
    // the block reliably.
    info << "Not enough successors for insert: |succs| " << succs.size ()
	 << ", DFRAGS " << blk->min_put () << "\n";
    (*cb) (DHASH_CHORDERR, mt);
    return;
  }

  if (succs.size () < blk->num_put ())
    // benjie: this is not a failure condition, since if we don't
    // receive num_efrags number of store replies in insert_store_cb,
    // we are still allowed to proceed.
    info << "Number of successors less than desired: |succs| " << succs.size ()
	 << ", EFRAGS " << blk->num_put () << "\n";

  vec<chord_node> unique_succs;
  filter_succs (succs, unique_succs);
  succs = unique_succs;

  while (succs.size () > blk->num_put ()) 
    succs.pop_back ();

  ref<sto_state> ss = New refcounted<sto_state> (block, cb);
  ss->succs = succs;
  ss->r = r;
  ss->blk = blk;

  // Track number of times insert_store_cb is to be called.
  ss->out = succs.size ();

  for (u_int i = 0; i < succs.size(); i++) {
      ptr<location> dest = clntnode->locations->lookup_or_create (succs[i]);

      str frag = blk->generate_fragment (block, i);
      dhash_store::execute (clntnode, dest, 
			    blockID(block->ID, block->ctype),
			    frag,
			    block->expiration,
			    wrap (this, &dhashcli::insert_store_cb,  
				  ss, i, getusec ()),
			    get_store_status (block->ctype)); 
      // XXX reactivate retry later
  }
  return;
    
}

void
dhashcli::insert_store_cb (ref<sto_state> ss, u_int i, u_int64_t t,
			   dhash_stat err, chordID id, bool present)
{
  u_int nstores = ss->blk->num_put ();
  u_int min_needed = ss->blk->min_put ();
  ss->out -= 1;
  info << "store " << ss->block->ID << " (frag " <<
    i + 1 << "/" << nstores << ") -> " << id << " in " <<
    (getusec () - t)/1000 << "ms: " << err << "\n";
  if (!err)
    ss->good += 1;
  if (err == DHASH_DISKFULL)
    ss->diskfull = true;

  // Count down until all outstanding RPCs have returned
  if (ss->out == 0) {
    vec<chordID> r_ret;
    chordID myID = clntnode->my_ID ();
    if (ss->good < nstores) {
      warning << myID << ": store (" << ss->block->ID << "): only stored "
	      << ss->good << " of " << nstores << " encoded.\n";
      if (ss->good < min_needed) {
	warning << myID << ": store (" << ss->block->ID << "): failed;"
	  " insufficient frags/blocks stored.\n";
	
	r_ret.push_back (ss->succs[0].x);
	(*ss->cb) (ss->diskfull ? DHASH_DISKFULL : DHASH_ERR, r_ret);
	// We should do something here to try and store this fragment
	// somewhere else.
	return;
      }
    }
    
    for (unsigned int i = 0; i < ss->r.size (); i++)
      r_ret.push_back (ss->r[i]->id ());

    (*ss->cb) (DHASH_OK, r_ret);
  }
}


//------------------ helper chord wrappers -------------
void
dhashcli::lookup (chordID blockID, dhashcli_lookupcb_t cb)
{
  clntnode->find_successor
    (blockID, wrap (this, &dhashcli::lookup_findsucc_cb, blockID, cb));
}

void
dhashcli::lookup_findsucc_cb (chordID blockID, dhashcli_lookupcb_t cb,
			      vec<chord_node> s, route path, chordstat err)
{
  if (err) 
    (*cb) (DHASH_CHORDERR, s, path);
  else
    (*cb) (DHASH_OK, s, path);
}


//------------------ sendblock ------------------------

void
dhashcli::sendblock (ptr<location> dst, blockID bid, str data,
		     u_int32_t expiration,
		     sendblockcb_t cb, int nonce /* = 0 */)
{

  dhash_store::execute 
    (clntnode, dst, bid, data, expiration,
     wrap (this, &dhashcli::sendblock_cb, cb, data.len ()),
     DHASH_FRAGMENT,
     nonce); //XXX choose DHASH_STORE if appropriate (i.e. full replica)
}

void
dhashcli::sendblock (ptr<location> dst, blockID bid_to_send, 
		     ptr<dhblock_srv> srv,
		     sendblockcb_t cb, int nonce /* = 0 */)
{

  trace << clntnode->my_ID () << ": dhashcli::sendblock (" << bid_to_send 
	<< ") to dest: " << dst << "\n";
  srv->fetch (bid_to_send.ID, 
		  wrap (this, &dhashcli::sendblock_fetch_cb, dst, 
			bid_to_send, cb, nonce));
  trace << clntnode->my_ID () << ": dhashcli::sendblock (" << bid_to_send 
	<< ") adbd request sent\n";

}

void
dhashcli::sendblock_fetch_cb (ptr<location> dst, blockID bid_to_send,
			      sendblockcb_t cb, int nonce,
			      adb_status stat, adb_fetchdata_t obj)
{

  trace << clntnode->my_ID () << ": dhashcli::sendblock_fetch_cb (" 
	<< bid_to_send << ") has been fetched " << dst << ", stat " 
	<< stat << "\n";

  if (stat != ADB_OK) {
    cb (DHASH_NOENT, false, 0);
    return;
  }

  dhash_store::execute 
    (clntnode, dst, bid_to_send, obj.data, obj.expiration,
     wrap (this, &dhashcli::sendblock_cb, cb, obj.data.len ()),
     get_store_status(bid_to_send.ctype), //XXX store_status broken
     nonce); 
}

void
dhashcli::sendblock_cb (sendblockcb_t cb, 
		        u_int32_t sz,
			dhash_stat err, chordID dest, bool present)
{
  (*cb) (err, present, sz);
}
