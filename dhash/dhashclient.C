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
#include "dhash.h"
#include "dhashclient.h"
#include "verify.h"
#include <chord_types.h>
#include "sfsmisc.h"
#include "arpc.h"
#include <sfscrypt.h>
#ifdef DMALLOC
#include "dmalloc.h"
#endif


bigint
compute_hash (const void *buf, size_t buflen)
{
  char h[sha1::hashsize];
  bzero(h, sha1::hashsize);
  sha1_hash (h, buf, buflen);
  
  bigint n;
  mpz_set_rawmag_be(&n, h, sha1::hashsize);  // For big endian
  return n;
}


// -----------------------------------------------------------------------------
// DHASHCLIENT

dhashclient::dhashclient(str sockname)
{
  int fd = unixsocket_connect(sockname);
  if (fd < 0) {
    fatal ("dhashclient: Error connecting to %s: %s\n",
	   sockname.cstr (), strerror (errno));
  }

  gwclnt = aclnt::alloc (axprt_unix::alloc (fd), dhashgateway_program_1);
}

//append
/* block layout 
 * long type = DHASH_APPEND
 * long contentlen
 * char data[contentlen}
 *
 * key = whatever you say
 */
void
dhashclient::append (chordID to, const char *buf, size_t buflen, 
		     cbinsertgw_t cb)
{
  //stick on the [type,contentlen] the server will have to strip it off before appending
  long type = DHASH_APPEND;
  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (XDR_PUTLONG (&x, (long int *)&type) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (to, m_dat, m_len, cb, DHASH_APPEND, false);
      xfree (m_dat);
    } else {
      (*cb) (DHASH_ERR, NULL); // marshalling failed.
    }

}


// content hash

#if 0
void
dhashclient::insert2 (bigint key, const char *buf,
		      size_t buflen, cbinsertgw_t cb, int options)
{
  dhash_insert2_arg arg;
  arg.block.set_ctype (DHASH_CONTENTHASH);
  arg.block.chash->key = key;
  arg.block.chash->block.setsize (buflen);
  memcpy (arg.block.chash->block.base (), buf, buflen);



  ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
  gwclnt->call (DHASHPROC_INSERT2, &arg, res, 
		wrap (this, &dhashclient::insertcb, cb, key, res));
}

// public key

void
dhashclient::insert2 (ptr<sfspriv> key, const char *buf, size_t buflen, long ver,
		      cbinsertgw_t cb, int options)
{
  str msg (buf, buflen);
  sfs_sig2 s;
  key->sign (&s, msg);
  sfs_pubkey2 pk;
  key->export_pubkey (&pk);

  // format RPC request 
  dhash_insert2_arg arg;
  arg.block.set_ctype (DHASH_KEYHASH);
  arg.block.pkhash->pub_key = pk;
  arg.block.pkhash->sig = s;
  arg.block.pkhash->block.setsize (buflen);
  memcpy (arg.block.pkhash->block.base (), buf, buflen);

  // hash the public key    
  strbuf b;
  ptr<sfspub> pk2 = sfscrypt.alloc (pk);
  pk2->export_pubkey (b, false);
  str pk_raw = b;
  chordID hash = compute_hash (pk_raw.cstr (), pk_raw.len ());

  ptr<dhash_insert_res> res = New refcounted<dhash_insert_res> ();
  gwclnt->call (DHASHPROC_INSERT2, &arg, res, 
		wrap (this, &dhashclient::insertcb, cb, hash, res));
}
#endif



//content-hash insert
/* content hash convention
   
   long type
   long contentlen
   char data[contentlen]

   key = HASH (data)
*/

void
dhashclient::insert (bigint key, const char *buf,
                     size_t buflen, cbinsertgw_t cb, int options)
{
  long type = DHASH_CONTENTHASH;
  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (XDR_PUTLONG (&x, (long int *)&type) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (key, m_dat, m_len, cb, DHASH_CONTENTHASH, options);
      xfree (m_dat);
    } else {
      (*cb) (DHASH_ERR, NULL); // marshalling failed.
    }
}

void
dhashclient::insert (const char *buf, size_t buflen, cbinsertgw_t cb,
                     int options)
{
  bigint key = compute_hash (buf, buflen);
  insert(key, buf, buflen, cb, options);
}



/* 
 * Public Key convention:
 * 
 * long type;
 * sfs_pubkey2 pub_key
 * sfs_sig2 sig
 * long version
 * long datalen
 * char block_data[datalen]
 */
void
dhashclient::insert (ptr<sfspriv> key, const char *buf, size_t buflen, long ver,
                     cbinsertgw_t cb, int options)
{
  str msg (buf, buflen);
  sfs_sig2 s;
  key->sign (&s, msg);
  sfs_pubkey2 pk;
  key->export_pubkey (&pk);
  insert(pk, s, buf, buflen, ver, cb, options);
}

void
dhashclient::insert (sfs_pubkey2 key, sfs_sig2 sig,
                     const char *buf, size_t buflen, long ver,
		     cbinsertgw_t cb, int options)
{
  strbuf b;
  ptr<sfspub> pk = sfscrypt.alloc (key);
  pk->export_pubkey (b, false);
  str pk_raw = b;
  chordID hash = compute_hash (pk_raw.cstr (), pk_raw.len ());
  insert (hash, key, sig, buf, buflen, ver, cb, options);
}

void
dhashclient::insert (bigint hash, sfs_pubkey2 key, sfs_sig2 sig,
                     const char *buf, size_t buflen, long ver,
		     cbinsertgw_t cb, int options)
{
  long type = DHASH_KEYHASH;

  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (XDR_PUTLONG (&x, (long int *)&type) &&
      xdr_sfs_pubkey2 (&x, &key) &&
      xdr_sfs_sig2 (&x, &sig) &&
      XDR_PUTLONG (&x, &ver) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (hash, m_dat, m_len, cb, DHASH_KEYHASH, options);
      xfree (m_dat);
    } else {
      ptr<insert_info> i = New refcounted<insert_info>(hash, bigint(0));
      cb (DHASH_ERR, i); // marshalling failed.
    }
}

// generic insert (called by above methods)
void
dhashclient::insert (bigint key, const char *buf, 
		     size_t buflen, cbinsertgw_t cb,
		     dhash_ctype t, int options)
  /* XXX delete t --  it isn't used */
{
  dhash_insert_arg arg;
  arg.blockID = key;
  arg.block.setsize (buflen);
  memcpy (arg.block.base (), buf, buflen);
  arg.options = options;

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
  ptr<insert_info> i = New refcounted<insert_info>(key, bigint(0));
  if (err) {
    errstr = strbuf () << "rpc error " << err;
    warn << "1dhashclient::insert failed: " << key << ": " << errstr << "\n";
    (*cb) (DHASH_RPCERR, i); //RPC failure
  } else {
    if (res->status != DHASH_OK) {
      errstr = dhasherr2str (res->status);
      if (res->status != DHASH_WAIT)
	warn << "2dhashclient::insert failed: " << key 
	     << ": " << errstr << "\n";
    }
    else {
      //if I wanted to pass back the destID do this:
      // (*cb) (false, key, res->resok->destID);
      i->destID = res->resok->destID;
    }
    (*cb) (res->status, i); 
  }
}

void
dhashclient::retrieve (bigint key, cb_ret cb, int options)
{
  ref<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> (DHASH_OK);
  dhash_retrieve_arg arg;
  arg.blockID = key;
  arg.options = options;
  gwclnt->call (DHASHPROC_RETRIEVE, &arg, res, 
		wrap (this, &dhashclient::retrievecb, cb, key, res));
}

void
dhashclient::retrievecb (cb_ret cb, bigint key, 
			 ref<dhash_retrieve_res> res, 
			 clnt_stat err)
{
  str errstr;
  if (err)
    errstr = strbuf () << "rpc error " << err;
  else if (res->status != DHASH_OK)
    errstr = dhasherr2str (res->status);
  else {
    dhash_ctype ctype = block_type (res->resok->block.base (), 
					   res->resok->block.size ());
    if (!verify (key, ctype, res->resok->block.base (), 
		      res->resok->block.size ())) {
      errstr = strbuf () << "data did not verify";
    } else {
      // success
      ptr<dhash_block> blk = get_block_contents (res->resok->block.base(), 
						 res->resok->block.size(), 
						 ctype);
      blk->hops = res->resok->hops;
      blk->errors = res->resok->errors;
      blk->retries = res->resok->retries;
      for (u_int i = 0; i < res->resok->times.size (); i++)
	blk->times.push_back (res->resok->times[i]);
      
      route path;
      for (u_int i = 0; i < res->resok->path.size (); i++)
	path.push_back (res->resok->path[i]);
      (*cb) (DHASH_OK, blk, path);
      return;
    } 
  }

  warn << "dhashclient::retrieve failed: " << key << ": " << errstr << "\n";
  route e_path;
  (*cb) (res->status, NULL, e_path); // failure
}



bool
dhashclient::sync_setactive (int32 n)
{
  dhash_stat res;
  clnt_stat err = gwclnt->scall (DHASHPROC_ACTIVE, &n, &res);

  if (err)
    warn << "sync_setactive: rpc error " << err << "\n";
  else if (res != DHASH_OK)
    warn << "sync_setactive: dhash error " << dhasherr2str (res) << "\n";
  
  return (err || res != DHASH_OK);
}
