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

#include "dhash_common.h"
#include "dhashclient.h"
#include "verify.h"
#include <chord_types.h>
#include "sfsmisc.h"
#include "arpc.h"
#include <sfscrypt.h>
#ifdef DMALLOC
#include "dmalloc.h"
#endif

/*
 * dhashclient
 */

dhashclient::dhashclient(str sockname)
{
  int fd = unixsocket_connect(sockname);
  if (fd < 0) {
    fatal ("dhashclient: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));
  }

  gwclnt = aclnt::alloc
    (axprt_unix::alloc (fd, 1024*1025), dhashgateway_program_1);
}

dhashclient::dhashclient (ptr<axprt_stream> xprt)
{
  gwclnt = aclnt::alloc (xprt, dhashgateway_program_1);
}


/* 
 * append block layout 
 *
 * long contentlen
 * char data[contentlen}
 *
 * key = whatever you say
 */
void
dhashclient::append (chordID to, const char *buf, size_t buflen, 
		     cbinsertgw_t cb)
{
  // stick on the [type,contentlen] the server will have to strip it
  // off before appending
  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if ((m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (to, m_dat, m_len, cb, DHASH_APPEND, buflen, NULL);
      xfree (m_dat);
    } else {
      (*cb) (DHASH_ERR, NULL); // marshalling failed.
    }

}


/*
 * content hash block layout
 * 
 * long contentlen
 * char data[contentlen]
 *
 * key = HASH (data)
 */
void
dhashclient::insert (bigint key, const char *buf,
                     size_t buflen, cbinsertgw_t cb, 
		     ptr<option_block> options)
{
  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if ((m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (key, m_dat, m_len, cb, DHASH_CONTENTHASH, buflen, options);
      xfree (m_dat);
    } else {
      (*cb) (DHASH_ERR, NULL); // marshalling failed.
    }
}

void
dhashclient::insert (const char *buf, size_t buflen, cbinsertgw_t cb,
                     ptr<option_block> options)
{
  bigint key = compute_hash (buf, buflen);
  insert (key, buf, buflen, cb, options);
}


/* 
 * keyhash block convention:
 * 
 * sfs_pubkey2 pub_key
 * sfs_sig2 sig (taken over salt, version (in NBO), payload)
 * long payload_len
 * signed payload (see struct keyhash_payload)
 *   long version
 *   salt_t salt
 *   char block_data[payload_len-sizeof(salt_t)-sizeof(long)]
 */
void
dhashclient::insert (bigint hash, sfs_pubkey2 key, sfs_sig2 sig,
                     keyhash_payload& p,
		     cbinsertgw_t cb, ptr<option_block> options)
{
  xdrsuio x;
  long plen = p.payload_len ();
  
  if (!xdr_sfs_pubkey2 (&x, &key) ||
      !xdr_sfs_sig2 (&x, &sig) ||
      !XDR_PUTLONG (&x, &plen) || 
      p.encode(x)) {
    vec<chordID> r;
    ptr<insert_info> i = New refcounted<insert_info>(hash, r);
    cb (DHASH_ERR, i); // marshalling failed.
    return;
  }

  int m_len = x.uio ()->resid ();
  char *m_dat = suio_flatten (x.uio ());
  insert (hash, m_dat, m_len, cb, DHASH_KEYHASH, x.uio()->resid(), options);
  xfree (m_dat);
}


/*
 * generic insert (called by above methods)
 */
void
dhashclient::insert (bigint key, const char *buf, 
		     size_t buflen, cbinsertgw_t cb,
		     dhash_ctype t, size_t realsize,
		     ptr<option_block> options)
{
  dhash_insert_arg arg;
  arg.blockID = key;
  arg.ctype = t;
  arg.len = realsize;
  arg.block.setsize (buflen);
  memcpy (arg.block.base (), buf, buflen);
  arg.options = 0;
  if (options) {
    arg.options = options->flags;
    if (options->flags & DHASHCLIENT_GUESS_SUPPLIED) {
      arg.guess = options->guess;
      warn << "starting with guess: " << arg.guess << "\n";
    }
  }

  ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
  gwclnt->call (DHASHPROC_INSERT, &arg, res, 
		wrap (this, &dhashclient::insertcb, cb, key, res));
}

void
dhashclient::insertcb (cbinsertgw_t cb, bigint key, 
		       ptr<dhash_insert_res> res,
		       clnt_stat err)
{
  str errstr;
  vec<chordID> r;
  ptr<insert_info> i = New refcounted<insert_info>(key, r);
  if (err) {
    errstr = strbuf () << "rpc error " << err;
    warn << "dhashclient::insert failed: " << key << ": " << errstr << "\n";
    (*cb) (DHASH_RPCERR, i); //RPC failure
  } else {
    if (res->status != DHASH_OK) {
      errstr = dhasherr2str (res->status);
      warn << "dhashclient::insert failed: " << key << ": " << errstr << "\n";
    }
    else {
      i->path.setsize (res->resok->path.size ());
      for (unsigned int j = 0; j < res->resok->path.size (); j++)
	i->path[j] = res->resok->path[j];
    }
    (*cb) (res->status, i); 
  }
}

/*
 * retrieve code
 */
void
dhashclient::retrieve (bigint key, cb_cret cb, ptr<option_block> options)
{
  retrieve(key, DHASH_CONTENTHASH, cb, options);
}

void
dhashclient::retrieve (bigint key, dhash_ctype ct, cb_cret cb, 
		       ptr<option_block> options)
{
  ref<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> (DHASH_OK);
  dhash_retrieve_arg arg;
  arg.blockID = key;
  arg.ctype = ct;
  arg.options = 0;
  if (options) {
    arg.options = options->flags;
    if (options->flags & DHASHCLIENT_GUESS_SUPPLIED)
      arg.guess = options->guess;
  }

  gwclnt->call (DHASHPROC_RETRIEVE, &arg, res, 
		wrap (this, &dhashclient::retrievecb, cb, key, res));
}

void
dhashclient::retrievecb (cb_cret cb, bigint key, 
			 ref<dhash_retrieve_res> res, 
			 clnt_stat err)
{
  str errstr;
  if (err)
    errstr = strbuf () << "rpc error " << err;
  else if (res->status != DHASH_OK)
    errstr = dhasherr2str (res->status);
  else {
    if (!verify (key, res->resok->ctype, res->resok->block.base (), 
		      res->resok->len)) {
      errstr = strbuf () << "data did not verify. len: " << res->resok->len
	                 << " ctype " << res->resok->ctype;
    } else {
      // success
      ptr<dhash_block> blk = get_block_contents (res->resok->block.base(), 
						 res->resok->block.size(), 
						 res->resok->ctype);
      blk->hops = res->resok->hops;
      blk->errors = res->resok->errors;
      blk->retries = res->resok->retries;
      for (u_int i = 0; i < res->resok->times.size (); i++)
	blk->times.push_back (res->resok->times[i]);
      
      vec<chordID> path;
      for (u_int i = 0; i < res->resok->path.size (); i++)
	path.push_back (res->resok->path[i]);
      (*cb) (DHASH_OK, blk, path);
      return;
    } 
  }

  warn << "dhashclient::retrieve failed: " << key << ": " << errstr << "\n";
  vec<chordID> e_path;
  (*cb) (res->status, NULL, e_path); // failure
}

