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

route_dhash::route_dhash (ptr<vnode> vi, 
			  chordID xi,
			  bool ucs) 
  : route_iterator (vi, xi), nerror (0), nhops (0),
    use_cached_succ (ucs), 
    npending (0), fetch_error (false), given_first (false)  {};

route_dhash::route_dhash (ptr<vnode> vi, 
			  chordID xi,
			  chordID first_hop,
			  bool ucs) 
  : route_iterator (vi, xi), nerror (0), nhops (0),
    use_cached_succ (ucs), 
    npending (0), fetch_error (false),
    given_first (true), first_hop_guess (first_hop) {};


void
route_dhash::first_hop (cbhop_t cbi)
{
  cb = cbi;
  search_path.push_back (v->my_ID ());
  if (!given_first)
    first_hop_guess = lookup_next_hop (x);
  if (first_hop_guess != v->my_ID ()) 
    search_path.push_back (first_hop_guess);
  next_hop ();
}

void
route_dhash::next_hop ()
{
  make_hop (search_path.back ());
}

void
route_dhash::make_hop (chordID &n) 
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = x;
  arg->start = 0;
  arg->len   = MTU;
  arg->v     = n;
  arg->cookie = 0;

  ptr<dhash_fetchiter_res> res = New refcounted <dhash_fetchiter_res> ();

  v->doRPC (n, dhash_program_1, DHASHPROC_FETCHITER, arg, res, 
	    wrap (this, &route_dhash::make_hop_cb, res));
}


chordID
route_dhash::lookup_next_hop (chordID k)
{
  if (use_cached_succ)
    return v->lookup_closestsucc (k);
  else
    return v->lookup_closestpred (k);
}

void
route_dhash::make_hop_cb (ptr<dhash_fetchiter_res> res, 
			  clnt_stat err) 
{

  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = x;
  arg->start = 0;
  arg->len   = MTU;

  nhops++;
  if (err) {
    chordID last, plast;

    warn << "route_dhash: RPC failure " << err << "\n";
    nerror++;
    if (search_path.size () > 0) 
      last = search_path.pop_back (); //pop off the failed node
    if (search_path.size () > 0) {
      plast = search_path.back (); /* pop off the node that told us about 
				      the failed node */
      v->alert (plast, last);
    } else {                      /* _we_ told us about the failed node */
      plast = lookup_next_hop (x); //pick a new node from tables
      search_path.push_back (plast);
    }
    if (plast == v->my_ID ()) {  /* we are our best live guess
					     and we don't have the block
					     FAIL */
      result = DHASH_NOENT;
      cb ((done = true));
    } else {
      /* ask the node which gave us the failed node for a better guess */
      /* we'll define "next" hop as the next _live_ hop and not return */
      arg->v = plast;
      ref<dhash_fetchiter_res> nres = New refcounted<dhash_fetchiter_res> ();
      v->doRPC (plast, dhash_program_1, DHASHPROC_FETCHITER, arg, nres,
		wrap(this, &route_dhash::make_hop_cb, nres));
    }
    
    
    /* below are the no error cases */
  } else if (res->status == DHASH_COMPLETE) {
    /* we've got the first part of the chunk, go into retreive mode */

    size_t totsz     = res->compl_res->attr.size;
    size_t nread     = res->compl_res->res.size ();
    chordID sourceID = res->compl_res->source;
    int cookie       = res->compl_res->cookie;

    block            = New refcounted<dhash_block> ((char *)NULL, totsz);
    block->hops      = nhops;
    block->errors    = nerror;
    block->source    = sourceID;

    npending = 1;

    //issue the RPCs to get the other chunks
    while (nread < totsz) {
      int offset = nread;
      int length = MIN (MTU, totsz - nread);
      npending++;

      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
      arg->v     = sourceID;
      arg->key   = x;
      arg->start = offset;
      arg->len   = length;
      arg->cookie = cookie;
      ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();

      v->doRPC (sourceID, dhash_program_1, DHASHPROC_FETCHITER, arg, res,
	     wrap (this, &route_dhash::finish_block_fetch, res));
      nread += length;
    }
    //process the first block
    finish_block_fetch (res, RPC_SUCCESS);

    /* still looking for the next chunk */
  } else if (res->status == DHASH_CONTINUE) {

    chordID next = res->cont_res->next.x;
    chordID prev = search_path.back ();
    
    if ((next == prev) || (straddled (search_path, x))) {
      /* the search got stuck: FAIL */
      result = DHASH_CHORDERR;
      print ();
      cb ((done = true));
    } else {
      v->locations->cacheloc (next, res->cont_res->next.r,
			      wrap (this, &route_dhash::nexthop_chalok_cb));
    }

    /* the last node queried was resposnible for the block
       but doesn't have it */
  } else {
    result = DHASH_NOENT;
    cb ((done = true));
  }

}

void
route_dhash::nexthop_chalok_cb (chordID s, bool ok, chordstat status)
{
  if (ok && status == CHORD_OK) {
    search_path.push_back (s);
    if (search_path.size () >= 1000) {
      warnx << "make_hop_done_cb: too long a search path: " << v->my_ID() 
	    << " looking for " << x << "\n";
      print ();
      assert (0);
    }
  } else if (status == CHORD_RPCFAILURE) {
    //make_hop_cb knows how to handle RPC erors, consult it
    make_hop_cb (NULL, RPC_TIMEDOUT);
  } else {
    warnx << v->my_ID () << ": make_hop_done_cb: step challenge for "
	  << s << " failed. (chordstat " << status << ")\n";
    assert (0); // XXX handle malice more intelligently
  }
  cb ((done = false));
}

void 
route_dhash::finish_block_fetch (ptr<dhash_fetchiter_res> res,
				 clnt_stat err)
{
  npending--;
  
  if (err || (res && res->status != DHASH_COMPLETE)) 
    fail (dhasherr2str (res->status));
  else {
    uint32 off = res->compl_res->offset;
    uint32 len = res->compl_res->res.size ();
    if (off + len > block->len)
      fail (strbuf ("bad fragment: off %d, len %d, block %d", 
		    off, len, block->len));
    else
      memcpy (&block->data[off], res->compl_res->res.base (), len);
  }
  
  /* got the last chunk */
  if (npending == 0) {
    if (fetch_error) {
      block = NULL;
      result = DHASH_NOENT;
    }
    result = DHASH_OK;
    cb (done = true);

  }
}


void
route_dhash::fail (str errstr)
{
  warn << "dhash_store failed: " << x << ": " << errstr << "\n";
  fetch_error = true;
}

//--utility------------
bool 
route_dhash::straddled (route path, chordID &k)
{
  int n = path.size ();
  if (n < 2) return false;
  chordID prev = path[n-1];
  chordID pprev = path[n-2];
  if (between (pprev, prev, k)) {
    warn << "straddled: " << pprev << " " << prev << " " << k << "\n";
    return true;
  } else
    return false;
  
}
