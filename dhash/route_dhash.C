#include "dhash_common.h"
#include <dhash.h>
#include <route.h>
#include "route_dhash.h"
#include <location.h>
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

u_int MTU = (getenv ("DHASH_MTU") ? atoi (getenv ("DHASH_MTU")) : 1024);
#define LOOKUP_TIMEOUT 60
static int gnonce;

typedef callback<void, ptr<dhash_block> >::ref cbretrieve_t;


// ---------------------------------------------------------------------------
// dhash_download -- downloads a block of a specific chord node.  That node
//                   should already be in the location table.

class dhash_download {
private:
  typedef callback<void, ptr<dhash_fetchiter_res>, int, clnt_stat>::ptr
    gotchunkcb_t;

  ptr<vnode> clntnode;
  uint npending;
  bool error;
  chordID sourceID;
  chordID blockID;
  cbretrieve_t cb;

  ptr<dhash_block> block;
  int nextchunk;     //  fast
  int numchunks;     //   retransmit
  vec<long> seqnos;  //   parameters
  bool didrexmit;

  u_int64_t start;

  dhash_download (ptr<vnode> clntnode, chordID sourceID, chordID blockID,
		  char *data, u_int len, u_int totsz, int cookie,
		  cbretrieve_t cb)
    : clntnode (clntnode),  npending (0), error (false), sourceID (sourceID), 
      blockID (blockID), cb (cb), nextchunk (0), numchunks (0),
      didrexmit (false)
  {
    start = getusec ();
    // the first chunk of data may be passed in
    if (data) {
      process_first_chunk (data, len, totsz, cookie);
      check_finish ();
    } else
      getchunk (0, MTU, 0, wrap (this,&dhash_download::first_chunk_cb));
  }

  long
  getchunk (u_int start, u_int len, int cookie, gotchunkcb_t cb)
  {
    ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
    arg->v     = sourceID;
    arg->key   = blockID;
    arg->start = start;
    arg->len   = len;
    arg->cookie = cookie;

    npending++;
    ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();
    //    warn << "SENT RPC for chunk " << numchunks << " at " << (getusec () - start) << "\n";

    return clntnode->doRPC 
      (sourceID, dhash_program_1, DHASHPROC_FETCHITER, arg, res, 
       wrap (this, &dhash_download::gotchunk, cb, res, numchunks++));
  }
  
  void
  gotchunk (gotchunkcb_t cb, ptr<dhash_fetchiter_res> res,
            int chunknum, clnt_stat err)
  {
    (*cb) (res, chunknum, err);
  }


  void
  first_chunk_cb  (ptr<dhash_fetchiter_res> res, int chunknum, clnt_stat err)
  {
    npending--;

    if (err || (res && res->status != DHASH_COMPLETE))
      fail (dhasherr2str (res->status));
    else {
      int cookie     = res->compl_res->cookie;
      size_t totsz   = res->compl_res->attr.size;
      size_t datalen = res->compl_res->res.size ();
      char  *data    = res->compl_res->res.base ();
      process_first_chunk (data, datalen, totsz, cookie);
    }
    check_finish ();
  }

  void
  process_first_chunk (char *data, size_t datalen, size_t totsz, int cookie)
  {
    block            = New refcounted<dhash_block> ((char *)NULL, totsz);
    block->source    = sourceID;
    block->hops      = 0;
    block->errors    = 0;

    add_data (data, datalen, 0);

    //issue the RPCs to get the other chunks
    size_t nread = datalen;
    while (nread < totsz) {
      int length = MIN (MTU, totsz - nread);
      //      warn << "SENT RPC for [" << nread << ", " << nread + length << "]  at " << (getusec () - start) << "\n";
      long seqno = getchunk (nread, length, cookie, wrap (this, &dhash_download::later_chunk_cb));
      seqnos.push_back (seqno);
      nread += length;
    }
  }

  void
  later_chunk_cb (ptr<dhash_fetchiter_res> res, int chunknum, clnt_stat err)
  {
    npending--;
    
    if (err || (res && res->status != DHASH_COMPLETE))
      fail (dhasherr2str (res->status));
    else {
      //      warn << "GOT RPC for chunk " << chunknum << " at " << (getusec () - start) << "\n";

      if (!didrexmit && (chunknum > nextchunk)) {
	warn << "FAST retransmit: " << blockID << " chunk " << nextchunk << " being retransmitted\n";
	clntnode->resendRPC(seqnos[nextchunk]);
	didrexmit = true;  // be conservative: only fast rexmit once per block
      }

      nextchunk++;
      add_data (res->compl_res->res.base (), res->compl_res->res.size (), 
		res->compl_res->offset);
    }
    check_finish ();
  }

  void
  add_data (char *data, int len, int off)
  {
    if ((unsigned)(off + len) > block->len)
      fail (strbuf ("bad chunk: off %d, len %d, block %d", 
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
  fail (str errstr)
  {
    warn << "dhash_download failed: " << blockID << ": " << errstr << "\n";
    error = true;
  }

public:
  static void execute (ptr<vnode> clntnode, chordID sourceID, chordID blockID,
		       char *data, u_int len, u_int totsz, int cookie, cbretrieve_t cb)
  {
    vNew dhash_download (clntnode, sourceID, blockID, data, len, totsz, cookie, cb);
  }
};




// ---------------------------------------------------------------------------
// route_dhash -- lookups and downloads a block.

route_dhash::route_dhash (ptr<route_factory> f, chordID blockID, dhash *dh,
                          int options)
  : dh (dh), options (options), blockID (blockID), f (f), dcb (NULL),
    retries_done (0)
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = blockID;
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
  chord_iterator = f->produce_iterator_ptr (blockID, dhash_program_1, DHASHPROC_FETCHITER, arg);
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
    dh->register_block_cb 
      (nonce, wrap (mkref(this), &route_dhash::block_cb));
    chord_iterator->send (options & DHASHCLIENT_USE_CACHED_SUCCESSOR);
  }
}

void
route_dhash::execute (cb_ret cbi, chordID first_hop, u_int _retries)
{
  start = getusec ();
  retries = _retries;
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (first_hop);
}

void
route_dhash::execute (cb_ret cbi, u_int _retries)
{
  start = getusec ();
  retries = _retries;
  cb = cbi;
  dcb = delaycb (LOOKUP_TIMEOUT, wrap (mkref(this), &route_dhash::timed_out));
  chord_iterator->send (options & DHASHCLIENT_USE_CACHED_SUCCESSOR);
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
//
// IDEA: perhaps challenge all successors in parallel
//       and down load the block off the first that 
//       responds.  This should optimize transfer speed.

void
route_dhash::walk (vec<chord_node> succs)
{
  if (options & DHASHCLIENT_NO_RETRY_ON_LOOKUP)
    reexecute (); // reexecute will call user callback
  else if (succs.size() == 0) {
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
    walk (succs); // just go on to next successor
  } else {
    warn << "walk: challenge of " << id << " succeeded\n";
    dhash_download::execute
      (f->get_vnode (), id, blockID, NULL, 0, 0, 0,
       wrap (mkref(this), &route_dhash::walk_gotblock, succs));
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
      succs.push_back (arg->nodelist[i]);
    walk (succs);
  } else {
    dhash_download::execute (f->get_vnode (), arg->source, blockID,
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
