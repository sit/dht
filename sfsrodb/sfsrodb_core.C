/* $Id: sfsrodb_core.C,v 1.19 2002/02/04 19:47:34 cates Exp $ */

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

static void
check_cbs () 
{
  while (out > 8) 
    acheck();
}

bigint
fh2mpz(const void *keydata, size_t keylen) 
{
  bigint n;
  mpz_set_rawmag_be(&n, (const char *)keydata, keylen); 
  return n;
}

/*
  Requires: You have at some point called random_init();
  Given: A filled buffer and allocated fh
  Return: A file handle in fh.  Generate random bytes for the first
  SFSRO_IVSIZE bytes in the opaque fh.  Add fh to fh_list
*/

void
create_sfsrofh (sfs_hash *fh, char *buf, size_t buflen)
{
  bzero(fh->base (), fh->size ());
  sha1_hash (fh->base (), buf, buflen);
}



static void
sfsrodb_put_cb (bool failed, chordID key)
{
  out--;
  if (failed)
    fatal << "Could not store block " << key << "\n";
}

sfs_hash
sfsrodb_put (void *data, size_t len)
{
  sfs_hash h;
  create_sfsrofh (&h, (char *)data, len);

  bigint key = compute_hash (data, len);
  check_cbs ();
  out++;
  dhash->insert (key, (char *)data, len, wrap (sfsrodb_put_cb));

  return h;
}

//  void
//  sfsrodb_put (const void *keydata, size_t keylen,
//  	     void *data, size_t len)
//  {
//    assert (keylen == sha1::hashsize);
//    bigint key = fh2mpz(keydata, keylen);
//    insert (key, data, len, DHASH_KEYHASH);
//  }


void
sfsrodb_put (ptr <rabin_priv> sk, void *data, size_t len)
{
  check_cbs ();
  out++;
  dhash->insert ((char *)data, len, *sk, wrap (sfsrodb_put_cb));
}



