/* $Id: sfsrodb.h,v 1.5 2002/01/07 22:30:24 cates Exp $ */

/*
 *
 * Copyright (C) 1999 Kevin Fu (fubob@mit.edu)
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

#ifndef _SFSRODB_H_
#define _SFSRODB_H_

#include "str.h"
#include "sfsro_prot_cfs.h"
#include "sfsrodb_core.h"
#include "sfsmisc.h"
#include "dhash.h"

#define SFSROSD_DB_FILE "/etc/sfsro.sdb"

#include "dbfe.h"
extern ptr<dhashclient> dhash;
extern long out;

/* Statistics */
extern u_int32_t reginode_cnt;
extern u_int32_t lnkinode_cnt;
extern u_int32_t filedatablk_cnt;
extern u_int32_t sindir_cnt;
extern u_int32_t dindir_cnt;
extern u_int32_t tindir_cnt;
extern u_int32_t directory_cnt;
extern u_int32_t fhdb_cnt;
extern u_int32_t fh_cnt;

extern u_int32_t identical_block;
extern u_int32_t identical_sindir;
extern u_int32_t identical_dindir;
extern u_int32_t identical_tindir;
extern u_int32_t identical_dir;
extern u_int32_t identical_inode;
extern u_int32_t identical_sym;
extern u_int32_t identical_fhdb;
extern u_int32_t identical_fh;

/* sfsrodb.C prototypes */
int recurse_directory (const str path, sfs_hash *fh);



#endif /* _SFSRODB_H_ */



