/* $Id: sfsrodb_core.C,v 1.17 2002/01/07 22:30:25 cates Exp $ */

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

void
create_sfsrofh (sfs_hash *fh, char *buf, size_t buflen)
{
  bzero(fh->base (), fh->size ());
  sha1_hash (fh->base (), buf, buflen);
}


static void
sfsrodb_put_cb (bool failed)
{
  out--;
  if (failed)
    fatal << "Could not store block";
}

void
sfsrodb_put (void *data, size_t len)
{
  bigint key = compute_hash (data, len);
  sfsrodb_put (key, data, len);
}

void
sfsrodb_put (const void *keydata, size_t keylen, 
	     void *data, size_t len)
{
  assert (keylen == 20);
  bigint key = fh2mpz(keydata, keylen);
  sfsrodb_put (key, data, len);
}

void
sfsrodb_put (bigint key, void *data, size_t len)
{
  check_cbs ();
  out++;
  // XXX get rid of cast.
  dhash->insert (key, (char *)data, len, wrap (sfsrodb_put_cb));
}

