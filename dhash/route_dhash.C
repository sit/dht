#include <dhash.h>
#include <route.h>
#include <chord.h>
#include <chord_prot.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include <sys/time.h>
#include "chord_util.h"
#ifdef DMALLOC
#include "dmalloc.h"
#endif

unsigned int MTU = (getenv ("DHASH_MTU") ?
		   atoi (getenv ("DHASH_MTU")) :
		   1024);

static int gnonce;

#define LOOKUP_TIMEOUT 30

// ---------------------------------------------------------------------------
// dhash_retrieve

class dhash_retrieve {
private:
  ptr<vnode> clntnode;
  uint npending;
  bool error;
  chordID sourceID;
  chordID blockID;
  cbretrieve_t cb;  
  bool askforlease;


  ptr<dhash_block> block;
  int nextblock;
  int numblocks;
  vec<long> seqnos;


  dhash_retrieve (ptr<vnode> clntnode, chordID sourceID, chordID blockID,
		  char *data, u_int len, u_int totsz, int cookie, int lease, 
		  bool askforlease, cbretrieve_t cb)
    : clntnode (clntnode),
      sourceID (sourceID),
      blockID (blockID),
      cb (cb),
      askforlease (askforlease)
  {
    block            = New refcounted<dhash_block> ((char *)NULL, totsz);
    block->source    = sourceID;
    block->hops      = 0;
    block->errors    = 0;
    block->lease     = 0;

    if (askforlease)
      block->lease = lease;

    npending = 0;
    error = false;
    nextblock = 1;
    numblocks = 1;
    seqnos.push_back (0);

    if (data) {
      get_remaining_chunks (len, totsz, cookie);
      add_data (data, len, 0);
    } else {
      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
      arg->v     = sourceID;
      arg->key   = blockID;
      arg->start = 0;
      arg->len   = MTU;
      arg->cookie = 0;
      arg->lease = askforlease;
      
      ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();
      long seqno = clntnode->doRPC (sourceID, dhash_program_1, 
				    DHASHPROC_FETCHITER, 
				    arg, res,
				    wrap (this,&dhash_retrieve::first_chunk_cb,
					  res, numblocks++));
      seqnos.push_back (seqno);
    } 
  }

  void
  get_remaining_chunks (size_t nread, size_t totsz, int cookie)
  {
    //issue the RPCs to get the other chunks
    while (nread < totsz) {
      int length = MIN (MTU, totsz - nread);
      npending++;
      
      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
      arg->v     = sourceID;
      arg->key   = blockID;
      arg->start = nread;
      arg->len   = length;
      arg->cookie = cookie;
      arg->lease = askforlease;
      
      ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();
      long seqno = clntnode->doRPC (sourceID, dhash_program_1, 
				    DHASHPROC_FETCHITER, 
				    arg, res,
				    wrap (this,&dhash_retrieve::finish_block_fetch, 
					  res, numblocks++));
      
      seqnos.push_back (seqno);
      nread += length;
    }
  }

  void
  first_chunk_cb  (ptr<dhash_fetchiter_res> res, int blocknum,
		   clnt_stat err)
  {
    if (err || (res && res->status != DHASH_COMPLETE))
      fail (dhasherr2str (res->status));
    else {
      int cookie     = res->compl_res->cookie;
      size_t totsz   = res->compl_res->attr.size;
      size_t datalen = res->compl_res->res.size ();
      char  *data    = res->compl_res->res.base ();
      size_t dataoff = res->compl_res->offset;

      get_remaining_chunks (datalen, totsz, cookie);
      add_data (data, datalen, dataoff);
      check_finish ();
    }
  }

  void
  add_data (char *data, int len, int off)
  {
    if ((unsigned)(off + len) > block->len)
      fail (strbuf ("bad fragment: off %d, len %d, block %d", 
		    off, len, block->len));
    else
      memcpy (&block->data[off], data, len);
  }

  void
  check_finish ()
  {
    if (npending == 0) {
      /* got all chunks */
      if (error) 
	block = NULL;
      (*cb) (block);
      delete this;
    }
  }

  void
  finish_block_fetch (ptr<dhash_fetchiter_res> res, int blocknum,
		      clnt_stat err)
  {
    npending--;
    
    if (err || (res && res->status != DHASH_COMPLETE)) 
      fail (dhasherr2str (res->status));
    else {
      if (askforlease && block->lease > res->compl_res->lease)
	block->lease = res->compl_res->lease;
      
      if ((blocknum > nextblock) && (numblocks - blocknum > 1)) {
	warn << "FAST retransmit: " << blockID << " chunk " << nextblock << " being retransmitted\n";
	
	clntnode->resendRPC(seqnos[nextblock]);
	//only one per fetch; finding more is too much bookkeeping
	numblocks = 0;
      }
      
      nextblock++;
      add_data (res->compl_res->res.base (), res->compl_res->res.size (), 
		res->compl_res->offset);
    }
    check_finish ();
  }

  void
  fail (str errstr)
  {
    warn << "dhash_store failed: " << blockID << ": " << errstr << "\n";
    error = true;
  }

public:
  static void execute (ptr<vnode> clntnode, chordID sourceID, chordID blockID,
		       char *data, u_int len, u_int totsz, int cookie, int lease, 
		       bool askforlease, cbretrieve_t cb)
  {
    vNew dhash_retrieve (clntnode, sourceID, blockID, data, len, totsz, cookie, lease, askforlease, cb);
  }
};




// ---------------------------------------------------------------------------
// route_dhash

route_dhash::route_dhash (ptr<route_factory> f,
			  chordID blockID,
			  dhash *dh,
			  bool lease,
			  bool ucs) 

  : dh (dh),
    ask_for_lease (lease),
    use_cached_succ (ucs), 
    npending (0), fetch_error (false),
    blockID (blockID),
    f (f),
    ignore_block (false)
{

  last_hop = false;
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = blockID;
  f->get_node (&arg->from);
  arg->start = 0;
  arg->len = MTU;
  arg->cookie = 0;
  arg->nonce = gnonce++;
  arg->lease = ask_for_lease;

  // XXX on timeout this never gets unregistered !!!
  dh->register_block_cb (arg->nonce, wrap (mkref(this), &route_dhash::block_cb));

  // Along the chord lookup path, 'arg' will get upcalled to each dhash server (dhash::do_upcall).
  // The dhash server will respond back by sending an RPC *request* to us.  We'll associate the
  // incoming RPC request with 'arg' via gnonce.  We'll field the RPC request in 
  // route_dhash::block_cb. Our RPC response is essentially ignored.
  chord_iterator = f->produce_iterator_ptr (blockID, dhash_program_1, DHASHPROC_FETCHITER, arg);
}

route_dhash::~route_dhash () 
{
  delete chord_iterator;
}

void
route_dhash::reexecute ()
{
  // XXX ugly code... duplicates the constructor
  delete chord_iterator;

  timecb_remove (dcb);
  dcb = NULL;

  last_hop = false;
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = blockID;
  f->get_node (&arg->from);
  arg->start = 0;
  arg->len = MTU;
  arg->cookie = 0;
  arg->nonce = gnonce++;
  arg->lease = ask_for_lease;

  // XXX on timeout this never gets unregistered !!!
  dh->register_block_cb (arg->nonce, wrap (mkref(this), &route_dhash::block_cb));

  // Along the chord lookup path, 'arg' will get upcalled to each dhash server (dhash::do_upcall).
  // The dhash server will respond back by sending an RPC *request* to us.  We'll associate the
  // incoming RPC request with 'arg' via gnonce.  We'll field the RPC request in 
  // route_dhash::block_cb. Our RPC response is essentially ignored.
  chord_iterator = f->produce_iterator_ptr (blockID, dhash_program_1, DHASHPROC_FETCHITER, arg);

  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (use_cached_succ);
}


void
route_dhash::execute (cb_ret cbi, chordID first_hop)
{
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (first_hop);
}

void
route_dhash::execute (cb_ret cbi)
{
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (use_cached_succ);
}

void
route_dhash::timed_out ()
{
  warn << "lookup TIMED OUT\n";
  // give the network a bit to heal, then retry the block fetch
  delaycb (5, wrap (mkref(this), &route_dhash::timed_out_after_wait));

#if 0
 (*cb)(DHASH_TIMEDOUT, NULL, path ());
  //XXX what happens if the request comes back
  //    after we've given up on it?
  // gotta do something, the timecb_remove below is causing a panic
  // once the block comes in after the timer has timed_out. ignore it?
  ignore_block = true;
#endif
}

void
route_dhash::timed_out_after_wait ()
{
  // retry the block fetch
  execute (cb);
}

void
route_dhash::walk (vec<chord_node> succs)
{
  if (succs.size() == 0) {
    warn << "walk: No luck walking successors, retrying..\n";
    delaycb (5, wrap (mkref(this), &route_dhash::reexecute));
  } else {
    chord_node s = succs.pop_front ();
    cbchallengeID_t cb = wrap (mkref(this), &route_dhash::walk_cachedloc, succs);
    f->get_vnode ()->locations->cacheloc (s.x, s.r, cb);
  }
}

void
route_dhash::walk_cachedloc (vec<chord_node> succs, chordID id, bool ok, chordstat stat)
{
  if (!ok || stat) {
    warn << "walk: challenge of " << id << " failed\n";
    walk (succs);
  } else {
    warn << "walk: challenge of " << id << " succeeded\n";
    dhash_retrieve::execute (f->get_vnode (),
			     id,
			     blockID,
			     NULL,
			     0,
			     0,
			     0,
			     0,
			     ask_for_lease,
			     wrap (mkref(this), &route_dhash::walk_gotblock, succs));
    // cb (DHASH_OK, block, path ());
    // assert (0);
    // try_fetch_block (id, wrap (mkref<>));
  }
}

void
route_dhash::walk_gotblock (vec<chord_node> succs, ptr<dhash_block> block)
{
  if (block)
    cb (DHASH_OK, block, path ());
  else {
    warn << "walk_gotblock failed\n";
    walk (succs);
  }
}




//  A node along the lookup path will send an RPC (request!) to us
//  if it has (or should have) the block we request.  This request
//  is dispatched from dhash::dispatch DHASHPROC_BLOCK to here.
void
route_dhash::block_cb (s_dhash_block_arg *arg)
{
  if (ignore_block)
    return;
  timecb_remove (dcb);
  dcb = NULL;
  if (arg->offset == -1) {
    vec<chord_node> succs;
    for (u_int i = 0; i < arg->nodelist.size (); i++)
      succs.push_back (arg->nodelist[i]);
    warn << "Responsible node did not have block.  Walking\n";
    walk (succs);
    return;
  }

  dhash_retrieve::execute (f->get_vnode (),
			   arg->source,
			   blockID,
			   arg->res.base (),
			   arg->res.size (),
			   arg->attr.size,
			   arg->cookie,
			   arg->lease,
			   ask_for_lease,
			   wrap (mkref(this), &route_dhash::gotblock));
}

void
route_dhash::gotblock (ptr<dhash_block> block)
{
  // XXX fix the path, we might have got the block off a replica
  block->hops = path ().size ();

  if (block) 
    cb (DHASH_OK, block, path ());
  else
    (*cb) (DHASH_NOENT, NULL, path ());
}

route
route_dhash::path ()
{
  route p = chord_iterator->path ();
  return p;
}
