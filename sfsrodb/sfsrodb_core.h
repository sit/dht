/* $Id: sfsrodb_core.h,v 1.7 2002/01/07 22:30:25 cates Exp $ */

/*
 *
 * Copyright (C) 2000 Kevin Fu (fubob@mit.edu)
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

#ifndef _SFSRODB_CORE_H_
#define _SFSRODB_CORE_H_

#include "sysconf.h"
#include "sfs_prot.h"
#include "crypt.h"
#include "str.h"
#include "sha1.h"
#include "xdrmisc.h"
#include "dbfe.h"
#include "dhash_prot.h"
#ifdef DMALLOC
#include "dmalloc.h"
#endif

#define CONCUR_OPS 8

bigint fh2mpz(const void *keydata, size_t keylen);

/*
  Requires: You have at some point called random_init();
  Given: A filled buffer and allocated fh
  Return: A file handle in fh.  Generate random bytes for the first
  SFSRO_IVSIZE bytes in the opaque fh.  Add fh to fh_list
*/
void create_sfsrofh (sfs_hash *fh, 
		     char *buf, size_t buflen);

void sfsrodb_put (void *data, size_t len);
void sfsrodb_put (const void *keydata, size_t keylen, void *data, size_t len);
void sfsrodb_put (bigint key, void *data, size_t len);


#endif /* _SFSRODB_CORE_H_ */
