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
#include "dhash_prot.h"
#include "dhash_common.h"
#include "dhashclient.h"
#include "arpc.h"
#include "sys/time.h"

#define mytimespecsub(vvp, uvp)						\
	do {								\
		(vvp)->tv_sec -= (uvp)->tv_sec;				\
		(vvp)->tv_nsec -= (uvp)->tv_nsec;			\
		if ((vvp)->tv_nsec < 0) {				\
			(vvp)->tv_sec--;				\
			(vvp)->tv_nsec += 1000000000;			\
		}							\
	} while (0)

long out=0;
long blkcnt = 0;

#define SMTU MTU

bool usr1 = false;

void
sfsrodb_core_sigusr1 ()
{
  usr1 = !usr1;  // toggle
}

static void
check_cbs () 
{
  while (out >= 16)
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

  
static timespec periodic;


static void
sfsrodb_put_cb (timespec ts, dhash_stat status, ptr<insert_info> i)
{
  if (blkcnt % 1000 == 0) {
    static timespec diff;
    clock_gettime (CLOCK_REALTIME, &diff);
    mytimespecsub(&diff, &periodic);
    warn << blkcnt << ", sec/1k " << diff.tv_sec << "\n";
    clock_gettime (CLOCK_REALTIME, &periodic);
  }

  blkcnt++;

  if (usr1) {
    timespec ts2;
    clock_gettime (CLOCK_REALTIME, &ts2);
    timespec diff = ts2;
    mytimespecsub(&diff, &ts);
    
    uint32 ms = 1000 * diff.tv_sec  + diff.tv_nsec / 1000000;
    //warn ("ts2  %ld:%09ld\n", ts2.tv_sec, ts2.tv_nsec);
    //warn ("ts   %ld:%09ld\n", ts.tv_sec, ts.tv_nsec);
    //warn ("df   %ld:%09ld\n", diff.tv_sec, diff.tv_nsec);
    warn << ms << " ms (" << blkcnt << ")\n";
    //  warnx << ms << "\n";
  }

  out--;
  if (status != DHASH_OK)
    warn << "Could not store block " << i->key << "\n";
}



sfs_hash
sfsrodb_put (void *data, size_t len)
{
  if (blkcnt == 0)  clock_gettime (CLOCK_REALTIME, &periodic);

  sfs_hash h;
  create_sfsrofh (&h, (char *)data, len);
  check_cbs ();
  out++;

  //warn << t() << " -- INSERT (" << blkcnt << ")\n";
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);
  dhash_cli->insert ((char *)data, len, wrap (sfsrodb_put_cb, ts));

  return h;
}


void
sfsrodb_put (ptr<sfspriv> sk, void *data, size_t len)
{
  if (blkcnt == 0) clock_gettime (CLOCK_REALTIME, &periodic);

  check_cbs ();
  out++;

  //warn << t() << " -- INSERT\n";
  timespec ts;
  clock_gettime (CLOCK_REALTIME, &ts);

  keyhash_payload p (0, str((char*)data, len));
  sfs_pubkey2 pk;
  sfs_sig2 s;
  p.sign (sk, pk, s);
  dhash_cli->insert (p.id (pk), pk, s, p, wrap (sfsrodb_put_cb, ts));
}





static bool get_done;
static ptr<sfsro_data> get_result;

static void
sfsrodb_get_cb (dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
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
sfsrodb_get (bigint key, dhash_ctype ct)
{
  get_done = false; // reset

  //warn << "---------------------- get: " << key << "\n";
  dhash_cli->retrieve (key, ct, wrap (&sfsrodb_get_cb));

  while (!get_done)
    acheck ();



  return get_result;
}



// 53300 - 54300:                       (107 sec/1k)
// 71000 = Mon Mar 25 17:54:35 EST 2002
// 74000 = Mon Mar 25 18:01:17 EST 2002 (134 sec/1k)
// 86000 = Mon Mar 25 18:27:12 EST 2002 (129 sec/1k)
// 93000 = Mon Mar 25 18:43:34 EST 2002 (140 sec/1k)
// 98000 = Mon Mar 25 18:59:46 EST 2002 (194 sec/1k)
// 136000 - 140000                         (380 sec/1k)
