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
    npending (0), fetch_error (false), given_first (false)  
{
  last_hop = false;
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = xi;
  arg->start = 0;
  arg->len = MTU;
  arg->cookie = 0;
  
  chord_iterator = New refcounted<route_chord> (vi, xi,
						dhash_program_1,
						DHASHPROC_FETCHITER,
						arg);
};

route_dhash::route_dhash (ptr<vnode> vi, 
			  chordID xi,
			  chordID first_hop,
			  bool ucs) 
  : route_iterator (vi, xi), nerror (0), nhops (0),
    use_cached_succ (ucs), 
    npending (0), fetch_error (false),
    given_first (true), first_hop_guess (first_hop) 
{ 
  
};

void
route_dhash::first_hop (cbhop_t cbi)
{
  
  cb = cbi;
  //XXX need to specify guess/use cached succ
  chord_iterator->first_hop (wrap (this, &route_dhash::hop_cb));

}

void 
route_dhash::hop_cb (bool done)
{
  ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();
  chord_iterator->get_upcall_res (res);

  if (res->status == DHASH_COMPLETE) {
    //go into retrieve mode
    size_t totsz     = res->compl_res->attr.size;
    size_t nread     = res->compl_res->res.size ();
    chordID sourceID = res->compl_res->source;
    int cookie       = res->compl_res->cookie;

    block            = New refcounted<dhash_block> ((char *)NULL, totsz);
    block->hops      = path().size();
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
  } else if (done) {
    if (last_hop) {
      result = DHASH_NOENT;
      (*cb) (done = true);
      return;
    }
    last_hop = true;
    (*cb) (false);
  } else {
    (*cb) (false);
  }

}

void
route_dhash::next_hop ()
{
  chord_iterator->next_hop ();
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

route
route_dhash::path ()
{
 route p = chord_iterator->path ();
 p.pop_back ();
 return p;
}
