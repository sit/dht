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

#include <chord.h>
#include <dhash.h>
#include <chord_prot.h>
#include "sfsmisc.h"
#include "arpc.h"
#include "crypt.h"
#include <sys/time.h>
#include "chord_util.h"
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



// ------------------------------------------------------------------------
// DHASHGATEWAY

dhashgateway::dhashgateway (ptr<axprt_stream> x, ptr<chord> node)
{
  clntsrv = asrv::alloc (x, dhashgateway_program_1, wrap (this, &dhashgateway::dispatch));
  clntnode = node;
  dhcli = New refcounted<dhashcli> (clntnode);
}


void
dhashgateway::dispatch (svccb *sbp)
{
  if (!sbp)
    return;

  assert (clntnode);

  switch (sbp->proc ()) {
  case DHASHPROC_NULL:
    sbp->reply (NULL);
    return;

  case DHASHPROC_INSERT:
    {
      warnt ("DHASHGW: insert_request");
      dhash_insert_arg *arg = sbp->template getarg<dhash_insert_arg> ();
      ref<dhash_block> block = New refcounted<dhash_block> (arg->block.base (), arg->block.size ());
      dhcli->insert (arg->blockID, block, arg->ctype, wrap (this, &dhashgateway::insert_cb, sbp));
    }
    break;

  case DHASHPROC_RETRIEVE:
    {
      warnt ("DHASHGW: retrieve_request");
      dhash_retrieve_arg *arg = sbp->template getarg<dhash_retrieve_arg> ();
      dhcli->retrieve (arg->blockID, wrap (this, &dhashgateway::retrieve_cb, sbp));
    }
    break;

  case DHASHPROC_ACTIVE:
    {
      warnt ("DHASHGW: change_active_request");
      int32 *n =  sbp->template getarg<int32> ();
      clntnode->set_active (*n);
      sbp->replyref (DHASH_OK);
    }
    break;

  default:
    sbp->reject (PROC_UNAVAIL);
    break;
  }
}

void
dhashgateway::insert_cb (svccb *sbp, bool err, chordID blockID)
{
  ///warn << "XXXXXX dhashgateway::insert_cb\n";
  if (err) 
    sbp->replyref (DHASH_CHORDERR);
  else
    sbp->replyref (DHASH_OK);
}


void
dhashgateway::retrieve_cb (svccb *sbp, ptr<dhash_block> block)
{
  ///warn << "dhashgateway::retrieve_cb\n";

  dhash_retrieve_res res (DHASH_OK);

  if (!block) 
    res.set_status (DHASH_ERR); // XXX what about DHASH_NOTPRESENT
  else {
    res.block->setsize (block->len);
    memcpy (res.block->base (), block->data, block->len);
  }
  sbp->reply (&res);
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
/* block layout [in db, not on the upload]
 * long type
 * long contentlen
 * char data[contentlen}
 *
 * key = whatever you say
 */
void
dhashclient::append (chordID to, const char *buf, size_t buflen, cbinsert_t cb)
{
  //just insert this at the right node, the server has to do all of the work
  insert (to, buf, buflen, cb, DHASH_APPEND);
}

//content-hash insert
/* content hash convention
   
   long type
   long contentlen
   char data[contentlen]

   key = HASH (data)
*/

void
dhashclient::insert (bigint key, const char *buf,
                     size_t buflen, cbinsert_t cb)
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
      insert (key, m_dat, m_len, cb, DHASH_CONTENTHASH);      
      xfree (m_dat);
    } else {
      cb (true, bigint (0)); // marshalling failed.
    }
}

void
dhashclient::insert (const char *buf, size_t buflen, cbinsert_t cb)
{
  bigint key = compute_hash (buf, buflen);
  insert(key, buf, buflen, cb);
}



/* 
 * Public Key convention:
 * 
 * long type;
 * bigint pub_key
 * bigint sig
 * long datalen
 * char block_data[datalen]
 *
 */
void
dhashclient::insert (const char *buf, size_t buflen, 
		     rabin_priv key, cbinsert_t cb)
{
  str msg (buf, buflen);
  bigint sig = key.sign (msg);
  insert(buf, buflen, sig, key, cb);
}

void
dhashclient::insert (const char *buf, size_t buflen, 
		     bigint sig, rabin_pub key, cbinsert_t cb)
{
  bigint pubkey = key.n;
  str pk_raw = pubkey.getraw ();
  chordID pkID = compute_hash (pk_raw.cstr (), pk_raw.len ());
  insert (pkID, buf, buflen, sig, key, cb);
}

void
dhashclient::insert (bigint hash, const char *buf, size_t buflen, 
		     bigint sig, rabin_pub key, cbinsert_t cb)
{
  long type = DHASH_KEYHASH;
  bigint pubkey = key.n;

  xdrsuio x;
  int size = buflen + 3 & ~3;
  char *m_buf;
  if (XDR_PUTLONG (&x, (long int *)&type) &&
      xdr_putbigint (&x, pubkey) &&
      xdr_putbigint (&x, sig) &&
      XDR_PUTLONG (&x, (long int *)&buflen) &&
      (m_buf = (char *)XDR_INLINE (&x, size)))
    {
      memcpy (m_buf, buf, buflen);
      
      int m_len = x.uio ()->resid ();
      char *m_dat = suio_flatten (x.uio ());
      insert (hash, m_dat, m_len, cb, DHASH_KEYHASH);
      xfree (m_dat);
    } else {
      cb (true, hash); // marshalling failed.
    }
}

//generic insert (called by above methods)
void
dhashclient::insert (bigint key, const char *buf, 
		     size_t buflen, cbinsert_t cb,
		     dhash_ctype t)
{
  dhash_stat *res = New dhash_stat (); // XXX how to make enums refcounted??
  dhash_insert_arg arg;
  arg.blockID = key;
  arg.block.setsize (buflen);
  memcpy (arg.block.base (), buf, buflen);
  arg.ctype = t;
  gwclnt->call (DHASHPROC_INSERT, &arg, res, 
		wrap (this, &dhashclient::insertcb, cb, key, res));
}

void
dhashclient::insertcb (cbinsert_t cb, bigint key, dhash_stat *res, clnt_stat err)
{
  str errstr;

  if (err)
    errstr = strbuf () << "rpc error " << err;
  else if (*res != DHASH_OK) 
    errstr = dhasherr2str (*res);
  else {
    (*cb) (false, key); // success
    delete res; // XXX make refcounted..
    return;
  }

  warn << "dhashclient::insert failed: " << key << ": " << errstr << "\n";
  (*cb) (true, key); // failure
  delete res; // XXX make refcounted..
}


void
dhashclient::retrieve (bigint key, dhash_ctype type, cbretrieve_t cb)
{
  ref<dhash_retrieve_res> res = New refcounted<dhash_retrieve_res> (DHASH_OK);
  dhash_retrieve_arg arg;
  arg.blockID = key;
  gwclnt->call (DHASHPROC_RETRIEVE, &arg, res, 
		wrap (this, &dhashclient::retrievecb, cb, key, type, res));
}


void
dhashclient::retrievecb (cbretrieve_t cb, bigint key, dhash_ctype ctype, 
			 ref<dhash_retrieve_res> res, clnt_stat err)
{
  str errstr;

  if (err)
    errstr = strbuf () << "rpc error " << err;
  else if (res->status != DHASH_OK)
    errstr = dhasherr2str (res->status);
  else if (!dhash::verify (key, ctype, res->block->base (), res->block->size ()))
    errstr = strbuf () << "data did not verify";
  else {
    // success
    (*cb) (dhash::get_block_contents (res->block->base(), res->block->size(), ctype));
    return;
  }

  warn << "dhashclient::retrieve failed: " << key << ": " << errstr << "\n";
  (*cb) (NULL); // failure
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
