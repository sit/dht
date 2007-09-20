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
#include "dhblock.h"
#include "dhblock_chash.h"
#include "dhblock_noauth.h"
#include "dhblock_keyhash.h"
#include "dhashclient.h"
#include <chord_types.h>
#include "arpc.h"
#include <sfscrypt.h>
#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include "misc_utils.h"

/*
 * dhashclient
 */

dhashclient::dhashclient (str sockname)
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

void
dhashclient::seteofcb (cbv::ptr cb)
{
  // Just pass through the callback; it takes no arguments anyway.
  gwclnt->seteofcb (cb);
}

/*
 * nopk block layout
 *
 * char data[contentlen]
 *
 * key = provided by user or hash of data depending on type of insert
 */
void
dhashclient::insert (bigint key, const char *buf,
                     size_t buflen, cbinsertgw_t cb,
		     ptr<option_block> options,
		     dhash_ctype t)
{
  assert (t == DHASH_CONTENTHASH || t == DHASH_NOAUTH);

  str data (buf, buflen);
  str marshalled_data;
  if (t == DHASH_CONTENTHASH)
    marshalled_data = dhblock_chash::marshal_block (data);
  else
    marshalled_data = dhblock_noauth::marshal_block (data);

  int m_len = marshalled_data.len ();
  char *m_dat = (char *)marshalled_data.cstr ();
  insert_togateway (key, m_dat, m_len, cb, t, options);
}

void
dhashclient::insert (const char *buf, size_t buflen, cbinsertgw_t cb,
                     ptr<option_block> options)
{
  bigint key = compute_hash (buf, buflen);
  insert (key, buf, buflen, cb, options, DHASH_CONTENTHASH);
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
  str mdata = dhblock_keyhash::marshal_block (key,
					     sig,
					     p);
  int m_len = mdata.len ();
  char *m_dat = (char *)mdata.cstr ();
  insert_togateway (hash, m_dat, m_len, cb, DHASH_KEYHASH, options);
}


/*
 * generic insert (called by above methods)
 */
void
dhashclient::insert_togateway (bigint key, const char *buf, 
                               size_t buflen, cbinsertgw_t cb,
                               dhash_ctype t,
                               ptr<option_block> options)
{
  dhash_insert_arg arg;
  arg.blockID = key;
  arg.ctype = t;
  arg.block.setsize (buflen);
  memcpy (arg.block.base (), buf, buflen);
  arg.expiration = 0;
  arg.options = 0;
  if (options) {
    arg.options = options->flags;
    if (options->flags & DHASHCLIENT_GUESS_SUPPLIED) {
      arg.guess = options->guess;
      warn << "starting with guess: " << arg.guess << "\n";
    }
    if (options->flags & DHASHCLIENT_EXPIRATION_SUPPLIED)
      arg.expiration = options->expiration;
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
  vec<chordID> r;
  ptr<insert_info> i = New refcounted<insert_info>(key, r);
  if (err) {
    warn << "dhashclient::insert failed (1): " << key 
	 << ": rpc error" << err << "\n";
    (*cb) (DHASH_RPCERR, i); //RPC failure
  } else {
    //warn << "dhashclient::insertcb dhash stat " << res->status << "\n";
    if (res->status != DHASH_OK) {
      warn << "dhashclient::insert failed (2): " << key << ": " << res->status << "\n";
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
  dhash_stat ret (res->status);
  if (err) {
    errstr = strbuf () << "rpc error " << err;
    ret    = DHASH_RPCERR;
  }
  else if (res->status != DHASH_OK)
    errstr = dhasherr2str (res->status);
  else {
    str block_data (res->resok->block.base (), res->resok->block.size ());

    if (!verify (key, block_data, res->resok->ctype)) {
      errstr = strbuf () << "data did not verify. ctype " << res->resok->ctype;
      ret = DHASH_RETRIEVE_NOVERIFY;
    } else {
      // success
      vec<str> contents = get_block_contents (block_data, res->resok->ctype);

      ptr<dhash_block> blk;
      if (contents.size () == 1)
	blk = New refcounted<dhash_block> (contents[0],
					   res->resok->ctype);
      else {
	assert (contents.size () > 0);
	blk = New refcounted<dhash_block> (str (""),
					   res->resok->ctype);
	blk->vData = contents;
      }

      blk->expiration = res->resok->expiration;
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
  (*cb) (ret, NULL, e_path); 
}

