/* $Id: sfsrodb_core.C,v 1.8 2001/03/21 16:10:01 fdabek Exp $ */

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
#include "dhash.h"
#include "dhash_prot.h"
#include "arpc.h"

bigint
fh2mpz(const void *keydata, size_t keylen) 
{
  bigint n;
  mpz_set_rawmag_be(&n, (const char *)keydata, keylen); 
  warnx << n << "\n";
  return n;
}

/* Return false if duplicate key */
bool
sfsrodb_put (ptr<aclnt> db, const void *keydata, size_t keylen, 
	     void *contentdata, size_t contentlen)
{

#if 0
  static int i=0;
  char filename[128];
  sprintf(filename, "key-%d", i++);

  int fd = open (filename, O_WRONLY | O_CREAT, 0666);
  write (fd, contentdata, contentlen);
  close (fd);
#endif

  int err;
  
  //  warn << "inserting " << contentlen << "bytes of data under a " << keylen << " byte key\n";
  dhash_insertarg *arg = New dhash_insertarg ();
  
  bigint n = fh2mpz(keydata, keylen);

  arg->key = n;
  arg->data.setsize (contentlen);
  memcpy(arg->data.base (), contentdata, contentlen);
  arg->type = DHASH_STORE;

  dhash_stat res;
  err = db->scall (DHASHPROC_INSERT, arg, &res);
  delete arg;

  if (err) {
    warn << "insert returned " << err << strerror(err) << "\n";
    return false;
  } else return true;

}


/* Library SFRODB routines used by the database creation,
   server, and client.  */

void
create_sfsrofh (char *iv, uint iv_len,
		sfs_hash *fh, 
		char *buf, size_t buflen)
{

  assert (iv_len == SFSRO_IVSIZE);

  bzero(fh->base (), fh->size ());

  struct iovec iov[2];
  iov[0].iov_base = static_cast<char *>(iv);
  iov[0].iov_len = SFSRO_IVSIZE;  
  iov[1].iov_base = buf;
  iov[1].iov_len = buflen;

  sha1_hashv (fh->base (), iov, 2);
}

bool
verify_sfsrofh (char *iv, uint iv_len,
		const sfs_hash *fh,
		char *buf, size_t buflen)
{
  assert (iv_len == SFSRO_IVSIZE);

  return 1;

  char tempbuf[fh->size ()];
  struct iovec iov[2];

  iov[0].iov_base = static_cast<char *>(iv);
  iov[0].iov_len = SFSRO_IVSIZE;
  
  iov[1].iov_base = buf;
  iov[1].iov_len = buflen;

  sha1_hashv (tempbuf, iov, 2);

  if (memcmp (tempbuf, fh->base (), fh->size ()) == 0) {
    return true;
  }

  warnx << "XXX verify_sfsrofh: hash doesn't match\n";
  warnx << "Given    fh: " << hexdump(fh->base (), fh->size ()) << "\n";
  warnx << "Computed fh: " << hexdump(tempbuf, fh->size ()) << "\n";

  return false;
}

void
create_sfsrosig (sfs_sig *sig, sfsro1_signed_fsinfo *info,
		 const char *seckeyfile)
{
  ptr<rabin_priv> sk;
  
  if (!seckeyfile) {
    warn << "cannot locate default file sfs_host_key\n";
    fatal ("errors!\n");
  }
  else {
    str key = file2wstr (seckeyfile);
    if (!key) {
      warn << seckeyfile << ": " << strerror (errno) << "\n";
      fatal ("errors!\n");
    }
    else if (!(sk = import_rabin_priv (key, NULL))) {
      warn << "could not decode " << seckeyfile << "\n";
      warn << key << "\n";
      fatal ("errors!\n");
    }
  }

  *sig = sk->sign (xdr2str (*info));
}


bool
verify_sfsrosig (const sfs_sig *sig, const sfsro1_signed_fsinfo *info,
		 const sfs_pubkey *key)
{
  rabin_pub rpk (*key);
  
  return rpk.verify (xdr2str (*info), *sig);
}
