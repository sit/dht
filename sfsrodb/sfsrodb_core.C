/* $Id: sfsrodb_core.C,v 1.21 2002/02/14 22:14:30 cates Exp $ */

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
  while (out >= 100) 
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


str 
t()
{
  str buf ("");
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  buf = strbuf (" %d:%06d", int (ts.tv_sec), int (ts.tv_nsec/1000));
  return buf;
}

  

static void
sfsrodb_put_cb (timespec ts, bool failed, chordID key)
{
#if 0
  timespec ts2;
  clock_gettime (CLOCK_REALTIME, &ts2);
  uint32 ns = 1000000000 * (ts2.tv_sec - 1 - ts.tv_sec) + 1000000000 + ts2.tv_nsec - ts.tv_nsec;
  uint32 us = ns / 1000;
  uint32 ms = us / 1000;

  //warn ("ts2  %d:%09d\n", int (ts2.tv_sec), int (ts2.tv_nsec));
  //warn ("ts   %d:%09d\n", int (ts.tv_sec), int (ts.tv_nsec));

  warnx << ms << " ms\n";
#endif

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

  //warn << t() << " -- INSERT\n";
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  dhash->insert (key, (char *)data, len, wrap (sfsrodb_put_cb, ts));

  return h;
}


void
sfsrodb_put (ptr <rabin_priv> sk, void *data, size_t len)
{
  check_cbs ();
  out++;
  //warn << t() << " -- INSERT\n";
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  dhash->insert ((char *)data, len, *sk, wrap (sfsrodb_put_cb, ts));
}





static bool get_done;
static ptr<sfsro_data> get_result;

static void
sfsrodb_get_cb (ptr<dhash_block> blk)
{
  ///warn << "---------------------- get_cb\n";

  get_done = true;
  get_result = NULL;

  if (blk) {
    ptr<sfsro_data> data = New refcounted<sfsro_data>;
    xdrmem x (blk->data, blk->len, XDR_DECODE);
    if (xdr_sfsro_data (x.xdrp (), data))
      get_result = data;
    else
      warn << "Couldn't unmarshall data\n";
  } else {
    warn << "No such block\n";
  }
}


ptr<sfsro_data>
sfsrodb_get (bigint key, dhash_ctype t)
{
  get_done = false; // reset

  //warn << "---------------------- get: " << key << "\n";
  dhash->retrieve (key, t, wrap (&sfsrodb_get_cb));

  while (!get_done)
    acheck ();



  return get_result;
}


