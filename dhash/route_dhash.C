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

int gnonce;

#define LOOKUP_TIMEOUT 30

route_dhash::route_dhash (ptr<route_factory> f,
			  chordID xi,
			  dhash *dh,
			  bool lease,
			  bool ucs) 

  : ask_for_lease (lease),
    use_cached_succ (ucs), 
    npending (0), fetch_error (false),
    xi (xi),
    f (f),
    ignore_block (false)
{

  last_hop = false;
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg> ();
  arg->key = xi;
  f->get_node (&arg->from);
  arg->start = 0;
  arg->len = MTU;
  arg->cookie = 0;
  arg->nonce = gnonce++;
  arg->lease = ask_for_lease;

  dh->register_block_cb (arg->nonce, wrap (mkref(this), 
					   &route_dhash::block_cb));
  chord_iterator = f->produce_iterator_ptr (xi, dhash_program_1, DHASHPROC_FETCHITER, arg);
};

route_dhash::~route_dhash () 
{
  delete chord_iterator;
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
  (*cb)(DHASH_TIMEDOUT, NULL, path ());
  //XXX what happens if the request comes back
  //    after we've given up on it?
  // gotta do something, the timecb_remove below is causing a panic
  // once the block comes in after the timer has timed_out. ignore it?
  ignore_block = true;
}

void
route_dhash::block_cb (s_dhash_block_arg *arg)
{

  if (ignore_block)
    return;
  timecb_remove (dcb);
  if (arg->offset == -1) {
    (*cb)(DHASH_NOENT, NULL, path ());
    return;
  }

  //go into retrieve mode
  size_t totsz     = arg->attr.size;
  size_t nread     = arg->res.size ();
  chordID sourceID = arg->source;
  int cookie       = arg->cookie;

  block            = New refcounted<dhash_block> ((char *)NULL, totsz);
  block->hops      = path().size();
  block->errors    = 0;
  block->source    = sourceID;
  block->lease     = 0;
  if (ask_for_lease)
    block->lease = arg->lease;
  
  npending = 0;
  
  //issue the RPCs to get the other chunks
  while (nread < totsz) {
    int offset = nread;
      int length = MIN (MTU, totsz - nread);
      npending++;

      ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
      arg->v     = sourceID;
      arg->key   = xi;
      arg->start = offset;
      arg->len   = length;
      arg->cookie = cookie;
      arg->lease = ask_for_lease;

      ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();
      f->get_vnode ()->doRPC (sourceID, dhash_program_1, DHASHPROC_FETCHITER, 
			   arg, res,
			   wrap (mkref(this), 
				 &route_dhash::finish_block_fetch, res));
      nread += length;
    }
  //process the first block
  add_data(arg->res.base (), arg->res.size (), arg->offset);
  check_finish ();
}

void
route_dhash::add_data (char *data, int len, int off)
{
    if ((unsigned)(off + len) > block->len)
      fail (strbuf ("bad fragment: off %d, len %d, block %d", 
		    off, len, block->len));
    else
      memcpy (&block->data[off], data, len);
 
}

void
route_dhash::check_finish ()
{
  /* got the last chunk */
  if (npending == 0) {
    if (fetch_error) {
      (*cb) (DHASH_NOENT, NULL, path ());
      block = NULL;
    }
    cb (DHASH_OK, block, path ());
  }
}

void 
route_dhash::finish_block_fetch (ptr<dhash_fetchiter_res> res,
				 clnt_stat err)
{
  npending--;
  
  if (err || (res && res->status != DHASH_COMPLETE)) 
    fail (dhasherr2str (res->status));
  else {
    if (ask_for_lease && block->lease > res->compl_res->lease)
      block->lease = res->compl_res->lease;
  
    add_data (res->compl_res->res.base (), res->compl_res->res.size (), 
	      res->compl_res->offset);
  }

  check_finish ();
}


void
route_dhash::fail (str errstr)
{

  warn << "dhash_store failed: " << xi << ": " << errstr << "\n";

  fetch_error = true;
}

route
route_dhash::path ()
{
 route p = chord_iterator->path ();
 return p;
}
