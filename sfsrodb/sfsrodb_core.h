/* $Id: sfsrodb_core.h,v 1.10 2002/04/02 16:38:48 cates Exp $ */

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
#include "sfsro_prot_cfs.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif

#define CONCUR_OPS 8

bigint fh2mpz(const void *keydata, size_t keylen);
sfs_hash sfsrodb_put (void *data, size_t len);
void sfsrodb_put (ptr <rabin_priv> sk, void *data, size_t len);
ptr<sfsro_data> sfsrodb_get (bigint key, dhash_ctype t = DHASH_CONTENTHASH);

void sfsrodb_core_sigusr1 ();

#endif /* _SFSRODB_CORE_H_ */
