/* $Id: sfsrodb_core.C,v 1.15 2001/09/10 01:21:59 fdabek Exp $ */

/*
 *
 * Copyright (C) 1999 Kevin Fu (fubob@mit.edu)
 * and Frans Kaashoek (kaashoek@mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

#include "sfsrodb_core.h"
#include "sfsrodb.h"
#include "dhash.h"
#include "dhash_prot.h"
#include "arpc.h"

long out=0;

#define SMTU MTU

void check_cbs ();

bigint
fh2mpz(const void *keydata, size_t keylen) 
{
  bigint n;
  mpz_set_rawmag_be(&n, (const char *)keydata, keylen); 
  return n;
}

/* always returns true (regardless of whether key is duplicated) */
bool
sfsrodb_put (const void *keydata, size_t keylen, 
	     void *contentdata, size_t contentlen)
{
  assert (keylen == 20);
  bigint n = fh2mpz(keydata, keylen);
  
  ptr<dhash_insertarg> arg = New refcounted<dhash_insertarg> ();
  int remain = (SMTU <= contentlen) ? SMTU : contentlen;
  arg->key = n;
  arg->data.setsize (remain);
  arg->offset = 0;
  arg->attr.size = contentlen;
  memset(arg->data.base (), 'a', remain);
  memcpy(arg->data.base (), (char *)contentdata, remain);
  arg->type = DHASH_STORE;

  dhash_storeres *res = New dhash_storeres();
  
  void *cd = malloc(contentlen);
  memcpy (cd, contentdata, contentlen);
  cclnt->call (DHASHPROC_INSERT, arg, res, wrap(&sfsrodb_put_cb, res,
						cd, contentlen, n, remain));
  out++;

  check_cbs ();
  return true;
}

void
sfsrodb_put_cb (dhash_storeres *res, 
		void *contentdata, size_t contentlen, bigint n, int offset,
		clnt_stat err)
{
  out--;
  if ((err) || (res->status != DHASH_OK)) { 
     warn << "insert failed: " << err << strerror(err) << "\n";
    warn << "Aborting\n";
    exit(1);
  }

  unsigned int written = offset;  

  while (written < contentlen) {
    dhash_send_arg *sarg = New dhash_send_arg ();
    sarg->dest = res->resok->source;
    sarg->iarg.key = n;
    unsigned int s = (written + SMTU < contentlen) ? SMTU : contentlen - written;
    sarg->iarg.data.setsize (s);
    memcpy (sarg->iarg.data.base (), (char *)contentdata + written, s);
    sarg->iarg.type = DHASH_STORE;
    sarg->iarg.attr.size = contentlen;
    sarg->iarg.offset = written;
    dhash_storeres *nres = New dhash_storeres;
    out++;
    cclnt->call (DHASHPROC_SEND, sarg, nres, wrap (&sfsrodb_put_finish_cb, nres));
    written += s;
    delete sarg;
  }
  delete res;
  free (contentdata);
  //  check_cbs ();
}


void
sfsrodb_put_finish_cb (dhash_storeres *res, clnt_stat err) 
{
  if (err) 
    fatal << "RPC error; aborting\n";

  out--;
  delete res;
}

void
check_cbs () 
{
  while (out > 8) acheck();
}


/* Library SFRODB routines used by the database creation,
   server, and client.  */

void
create_sfsrofh (sfs_hash *fh, char *buf, size_t buflen)
{
  bzero(fh->base (), fh->size ());
  sha1_hash (fh->base (), buf, buflen);
}

bool
verify_sfsrofh (const sfs_hash *fh,
		char *buf, size_t buflen)
{
  char tempbuf[fh->size ()];

  sha1_hash (tempbuf, buf, buflen);

  if (memcmp (tempbuf, fh->base (), fh->size ()) == 0) {
    return true;
  }

  warnx << "XXX verify_sfsrofh: hash doesn't match\n";
  warnx << "Given    fh: " << hexdump(fh->base (), fh->size ()) << "\n";
  warnx << "Computed fh: " << hexdump(tempbuf, fh->size ()) << "\n";

  return false;
}

