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

dhash_download::dhash_download (ptr<vnode> clntnode, chord_node source,
				blockID blockID, char *data, u_int len,
				u_int totsz, int cookie, cbretrieve_t cb,
				cbtmo_t cb_tmo) :
  clntnode (clntnode),
  npending (0),
  error (false),
  source (source),
  blckID (blockID),
  cb (cb),
  cb_tmo (cb_tmo),
  buffer (NULL),
  buf_len (0),
  nextchunk (0),
  numchunks (0),
  didrexmit (false),
  start (getusec ())
{
  // the first chunk of data may be passed in
  if (data) {
    process_first_chunk (data, len, totsz, cookie);
    check_finish ();
  } else {
    unsigned mtu = (clntnode->my_location ()->id () == source.x)
      ? 8192 : dhblock::dhash_mtu ();
    getchunk (0, mtu, 0, wrap (this, &dhash_download::first_chunk_cb));
  }
}

dhash_download::~dhash_download ()
{
  if (buffer) {
    free (buffer);
    buffer = NULL;
  }
}

void
dhash_download::getchunk (u_int start, u_int len, int cookie, gotchunkcb_t cb)
{
  ptr<s_dhash_fetch_arg> arg = New refcounted<s_dhash_fetch_arg>;
  arg->key   = blckID.ID;
  arg->ctype = blckID.ctype;
  arg->start = start;
  arg->len   = len;
  arg->cookie = cookie;

  npending++;
  ptr<dhash_fetchiter_res> res = New refcounted<dhash_fetchiter_res> ();

  long seqno = clntnode->doRPC 
    (source, dhash_program_1, DHASHPROC_FETCHITER, arg, res, 
     wrap (this, &dhash_download::gotchunk, cb, res, numchunks++),
     cb_tmo);

  seqnos.push_back (seqno);
}
  
void
dhash_download::gotchunk (gotchunkcb_t cb, ptr<dhash_fetchiter_res> res,
			  int chunknum, clnt_stat err)
{
  (*cb) (res, chunknum, err);
}

void
dhash_download::first_chunk_cb  (ptr<dhash_fetchiter_res> res, int chunknum,
				 clnt_stat err)
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
dhash_download::process_first_chunk (char *data, size_t datalen, size_t totsz,
				     int cookie)
{
  buf_len = totsz;
  buffer = (char *)malloc (buf_len);

  nextchunk++;
  add_data (data, datalen, 0);

  //issue the RPCs to get the other chunks
  size_t nread = datalen;
  while (nread < totsz) {
    unsigned mtu = (clntnode->my_location ()->id () == source.x)
      ? 8192 : dhblock::dhash_mtu ();
    int length = MIN (mtu, totsz - nread);
    getchunk (nread, length, cookie,
	      wrap (this, &dhash_download::later_chunk_cb));
    nread += length;
  }
}

void
dhash_download::later_chunk_cb (ptr<dhash_fetchiter_res> res, int chunknum,
				clnt_stat err)
{
  npending--;
    
  if (err || (res && res->status != DHASH_COMPLETE))
    fail (dhasherr2str (res->status));
  else {

    if (!didrexmit && (chunknum > nextchunk)) {
      warn << "FAST retransmit: " << blckID << " chunk "
	   << nextchunk << " being retransmitted.\n";
      clntnode->resendRPC (seqnos[nextchunk]);
      didrexmit = true;  // be conservative: only fast rexmit once per block
    }

    nextchunk++;
    add_data (res->compl_res->res.base (), res->compl_res->res.size (), 
	      res->compl_res->offset);
  }
  check_finish ();
}

void
dhash_download::add_data (char *data, int len, int off)
{
  if ((unsigned)(off + len) > (u_int)buf_len)
    fail (strbuf ("bad chunk: off %d, len %d, block %d", 
		  off, len, buf_len));
  else
    memcpy (buffer + off, data, len);
}

void
dhash_download::check_finish ()
{
  if (npending == 0) {
    ptr<dhash_block> block = NULL;
    /* got all chunks */
    if (!error)  {
      block = New refcounted<dhash_block> (buffer, buf_len, blckID.ctype);
      block->source = source.x;
      block->hops   = 0;
      block->errors = 0;
    }
    (*cb) (block);
    delete this;
  }
}

void
dhash_download::fail (str errstr)
{
  warn << "dhash_download failed: " << blckID << ": "
       << errstr << " at " << source.x << "\n";
  error = true;
}

