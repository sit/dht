#include "dhash_common.h"
#include <dhash.h>
#include <chord_types.h>
#include <route.h>
#include "route_dhash.h"
#include <location.h>
#include <locationtable.h>
#include <chord.h>
#include <misc_utils.h>
#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include "download.h"

extern u_int MTU;
#define LOOKUP_TIMEOUT 60
static int gnonce;

// ---------------------------------------------------------------------------
// route_dhash -- lookups and downloads a block.

route_dhash::route_dhash (ptr<route_factory> f, blockID blockID, dhash *dh,
                          ptr<vnode> host_node, int options)
  : dh (dh), host_node (host_node), options (options), blckID (blockID), 
    f (f), dcb (NULL), retries_done (0)
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = blckID.ID;
  arg->ctype = blckID.ctype;
  arg->dbtype = blckID.dbtype;
  f->get_node (&arg->from);
  arg->start = 0;
  arg->len = MTU;
  arg->cookie = 0;
  arg->nonce = gnonce++;
  nonce = arg->nonce;
  start = getusec ();

  dh->register_block_cb (arg->nonce, wrap (mkref(this), &route_dhash::block_cb));

  // Along the chord lookup path, 'arg' will get upcalled to each dhash server (dhash::do_upcall).
  // The dhash server will respond back by sending an RPC *request* to us.  We'll associate the
  // incoming RPC request with 'arg' via gnonce.  We'll field the RPC request in 
  // route_dhash::block_cb. Our RPC response is essentially ignored.
  chord_iterator = f->produce_iterator_ptr (blockID.ID, dhash_program_1, DHASHPROC_FETCHITER, arg);
}

route_dhash::~route_dhash () 
{
  dh->unregister_block_cb (nonce);
  delete chord_iterator;
  chord_iterator = NULL;
  timecb_remove (dcb);
  dcb = NULL;
}

void
route_dhash::reexecute ()
{
  if (retries == 0 || options & DHASHCLIENT_NO_RETRY_ON_LOOKUP) {
    warn << "route_dhash: no more retries...giving up\n";
    (*cb) (DHASH_NOENT, NULL, path ());
    dh->unregister_block_cb (nonce);
  } else {
    // XXX what if 'this' route_dhash was invoked with the other execute() ???
    retries--;
    retries_done++;
    timecb_remove (dcb);
    dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
    dh->register_block_cb (nonce, wrap (mkref(this), &route_dhash::block_cb));
    chord_iterator->send (NULL); // hint it with the end of the route?
  }
}

void
route_dhash::execute (cb_ret cbi, chordID first_hop, u_int _retries)
{
  start = getusec ();
  retries = _retries;
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  ptr<chordID> fh = New refcounted<chordID> (first_hop);
  chord_iterator->send (fh);
}

void
route_dhash::execute (cb_ret cbi, u_int _retries)
{
  start = getusec ();
  retries = _retries;
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (NULL);
}

void
route_dhash::timed_out ()
{
  dcb = NULL;
  warn << "lookup TIMED OUT\n";
  reexecute ();
}


// If the block isn't on the home node, walk down  
// the home node's successors looking for the block
void
route_dhash::walk (vec<chord_node> succs)
{
  if (options & DHASHCLIENT_NO_RETRY_ON_LOOKUP)
    reexecute (); // reexecute will call user callback
  else if (succs.size() == 0) {
    warn << "walk: No luck walking successors, retrying..\n";
    delaycb (5, wrap (mkref(this), &route_dhash::reexecute));
  } else {
    bool ok = false;
    chord_node s;
    ptr<location> l;
    while (!ok && succs.size () > 0) {
      chord_node s = succs.pop_front ();
      l = f->get_vnode ()->locations->lookup_or_create (s);
      // XXX
    }
    if (ok) {
      dhash_download::execute
	(f->get_vnode (), s, blckID, NULL, 0, 0, 0,
	 wrap (mkref(this), &route_dhash::walk_gotblock, succs));
    } else {
      warn << "walk: No luck walking successors, retrying..\n";
      delaycb (5, wrap (mkref(this), &route_dhash::reexecute));
    }
  }
}

void
route_dhash::walk_gotblock (vec<chord_node> succs, ptr<dhash_block> block)
{
  if (block) 
    gotblock (block);
  else {
    warn << "walk_gotblock failed\n";
    walk (succs);
  }
}



//  A node along the lookup path will send an RPC *REQUEST* to us
//  if it responsible for the block we requested.  This RPC *REQUEST*
//  is dispatched from dhash::dispatch DHASHPROC_BLOCK to here.
void
route_dhash::block_cb (s_dhash_block_arg *arg)
{
  //  warn << blockID << ": got the block cb " << (getusec () - start) << " usecs later\n";
  start = getusec ();
  timecb_remove (dcb);
  dcb = NULL;
  if (arg->offset == -1) {
    warn << "Responsible node did not have block.  Walking\n";
    vec<chord_node> succs;
    for (u_int i = 0; i < arg->nodelist.size (); i++)
      succs.push_back (make_chord_node (arg->nodelist[i]));
    walk (succs);
  } else {
    chord_node n;
    host_node->locations->lookup (arg->source)->fill_node (n);

    dhash_download::execute (f->get_vnode (), n, blckID,
			     arg->res.base (), arg->res.size (), 
			     arg->attr.size, arg->cookie,
			     wrap (mkref(this), &route_dhash::gotblock));
  }
}

void
route_dhash::gotblock (ptr<dhash_block> block)
{
  //  warn << blockID << ": finished grabbing the block " << (getusec () - start) << " after that\n";
  if (block) {
    // XXX fix the path, we might have fetched the block off a replica
    block->hops = path ().size ();
    block->errors = chord_iterator->failed_path ().size ();
    block->retries = retries_done;

    cb (DHASH_OK, block, path ());
  } else
    (*cb) (DHASH_NOENT, NULL, path ());
}

route
route_dhash::path ()
{
  route p = chord_iterator->path ();
  return p;
}
