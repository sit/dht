#include <chord.h>
#include <location.h>
#include <locationtable.h>
#include <dhash_common.h>
#include <dhash.h>
#include <misc_utils.h>
#include "dhblock.h"
#include "download.h"

// ---------------------------------------------------------------------------
// dhash_download -- downloads a block of a specific chord node.  That node
//                   should already be in the location table.

dhash_download::dhash_download (ptr<vnode> clntnode, ptr<dhash> dh, 
				chord_node source,
				blockID blockID, cbretrieve_t cb,
				cbtmo_t cb_tmo) :
  error (false),
  source (source),
  blckID (blockID),
  cb (cb),
  buffer (NULL),
  buf_len (0),
  dh (dh),
  bytes_read (0),
  expiration (0),
  fetch_acked (false),
  called_cb (false)
{

  assert (dh);
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = blckID.ID;
  arg->ctype = blckID.ctype;

  ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();

  
  arg->nonce = dh->register_fetch_callback (wrap (this, 
						  &dhash_download::gotchunk));
  nonce = arg->nonce;
  clntnode->doRPC 
    (source, dhash_program_1, DHASHPROC_FETCHITER, arg, res, 
     wrap (this, &dhash_download::sent_request, res), cb_tmo);
  
  
}

dhash_download::~dhash_download ()
{
  if (buffer) {
    free (buffer);
    buffer = NULL;
  }
  
  dh->unregister_fetch_callback (nonce);
}


void
dhash_download::sent_request (ptr<dhash_fetchiter_res> res, clnt_stat err)
{

  assert (!fetch_acked);
  fetch_acked = true;

  if (err) {
    strbuf e;
    e << "RPC error, status " << err;
    fail ((str) e); 
  } else if (res && res->status != DHASH_INPROGRESS) {
    strbuf e;
    e << "bad status, status " << res->status;
    fail ((str) e);
  } else
    ;

  // don't call check_finish before gotchunk has finished, unless gotchunk
  // will never be called.  otherwise you'll have issues with
  // buf_len == bytes_read == 0
  if (called_cb || error) {
    check_finish ();
  }
}
  

void 
dhash_download::gotchunk (str data, int offset, dhash_valueattr attr)
{
  //first chunk
  if (offset < 0) {
    fail ("Block not found");
  } else {
    if (buf_len == 0) {
      expiration = attr.expiration;
      buf_len = attr.size;
      buffer = (char *)malloc (buf_len);
    }
    add_data (data, offset);
  }
  
  check_finish ();
}

void 
dhash_download::add_data (str sdata, int off)
{
  int len = sdata.len ();
  const char *data = sdata.cstr ();

  if ((unsigned)(off + len) > (u_int)buf_len)
    fail (strbuf ("bad chunk: off %d, len %d, block %ld", 
		  off, len, buf_len));
  else {
    memcpy (buffer + off, data, len);
    bytes_read += len;
  }
}

void
dhash_download::check_finish ()
{
  //XXX new finish condition
  if (bytes_read == buf_len || error) { 
    ptr<dhash_block> block = NULL;
    /* got all chunks */
    if (!error)  {
      block = New refcounted<dhash_block> (buffer, buf_len, blckID.ctype);
      block->source = source.x;
      block->expiration = expiration;
      block->hops   = 0;
      block->errors = 0;
    }
    if (!called_cb) {
      (*cb) (block);
      called_cb = true;
    }
    // If the fetch has been acked (sent_request has been called), then we're
    // good to delete, because either (a) gotchunk has not been called yet,
    // and sent_request got an error, so it never will get called, or 
    // (b) gotchunk was called and is either finished or gave an error,
    // in either case we're done (otherwise we wouldn't be in this block)
    if( fetch_acked ) {
      delete this;
    }
  }
}

void
dhash_download::fail (str errstr)
{
  warn << "dhash_download failed: " << blckID << ": "
       << errstr << " at " << source << "\n";
  error = true;
}

