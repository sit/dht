/* $Id: fhdb.C,v 1.1 2001/01/16 22:00:08 fdabek Exp $ */

/*
 * Copyright (C) 1999, 2000 Kevin Fu (fubob@mit.edu)
 * Copyright (C) 1999 Frans Kaashoek (kaashoek@mit.edu)
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

#include "sfsrodb.h"

/* rpc_vec sucks.  It can't realloc.  So we use a vec kludge */
struct fhdb_indir_temp {
  vec<sfs_hash> key;
  vec<sfs_hash> child;
};

/*
  Given: A vec of file handles
  Effects: Creates a FHDB block from the handles, then
           adds the block to the database.
  Returns: File handle of the block written to the database
 */
sfs_hash fhdb_dir_add (vec<sfs_hash> &fhv, char *iv) 
{
  sfsro_data dat (SFSRO_FHDB_DIR);
  dat.fhdb_dir->set (fhv.base (), fhv.size ());

   /*  fhdb_dir.setsize (fhv.size ());
  for (u_int32_t i = 0; i < fhv.size (); i++) {
    fhdb_dir[i] = fhv [i];
  }
   */

  // hash fhv and add to database
  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);
      
  if (xdr_sfsro_data (x.xdrp (), &dat)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  sfs_hash fh;
  create_sfsrofh (iv, SFSRO_IVSIZE, &fh, callbuf, calllen);

  if (!sfsrodb_put (sfsrofhdb, fh.base (), 
		    fh.size (), callbuf, calllen)) {
    identical_fhdb++;
  } else {
    fhdb_cnt++;
  }
  xfree (callbuf);
  
  return fh;
}

/*
  Given: vecs of keys and children
  Effects: Pops a key off the back, then creates a FHDB_INDIR block
           from the vecs, then adds the block to the database.
  Returns: File handle of the block written to the database 
*/
sfs_hash fhdb_indir_add (fhdb_indir_temp &indir, char *iv) 
{
  sfsro_data dat (SFSRO_FHDB_INDIR);

  assert (indir.key.size () == indir.child.size ());
  assert (indir.key.size () > 0);
  
  indir.key.pop_back (); // Get rid of unnecessary key
  dat.fhdb_indir->key.set   (indir.key.base (), indir.key.size ());
  dat.fhdb_indir->child.set (indir.child.base (), indir.child.size ());

  // hash fhv and add to database
  size_t calllen = 0;
  char *callbuf = NULL;
  xdrsuio x (XDR_ENCODE);
      
  if (xdr_sfsro_data (x.xdrp (), &dat)) {
    calllen = x.uio ()->resid ();
    callbuf = suio_flatten (x.uio ());
  }

  sfs_hash fh;
  create_sfsrofh (iv, SFSRO_IVSIZE, &fh, callbuf, calllen);

  if (!sfsrodb_put (sfsrofhdb, fh.base (), 
		    fh.size (), callbuf, calllen)) {
    identical_fhdb++;
  } else {
    fhdb_cnt++;
  }
  xfree (callbuf);

  return fh;
}

/*
  Given: A path of the hash tree (indirv), a level
         to insert at, and a key/child to insert
  Effects: Insert the key/child into indirv[level]
  Requires: indirv[level] not be already full and
  that level not more than one+ already existing.
 */ 
void fhdb_insert (vec<fhdb_indir_temp> &indirv, 
		  sfs_hash &child, sfs_hash &key,
		  u_int16_t level)
{
  assert (indirv.size () >= level);

  if (indirv.size () == level) {
    indirv.push_back ();
  }

  assert (indirv[level].child.size () < SFSRO_FHDB_CHILDREN);

  indirv[level].child.push_back (child);
  indirv[level].key.push_back (key);

}
    
/*
  Given: A vec of indirect nodes, a level
  Effects: If this level is full, insert into db and propagate
           hash upwards. Restore the "no full nodes" property
           If force == true, then it will forcibly insert
           the partially complete block, etc. and propagate.
*/
void fhdb_propagate (vec<fhdb_indir_temp> &indirv,
		     u_int16_t level, char *iv)
{
  assert (indirv.size () > level);
   
  if (indirv[level].child.size () == SFSRO_FHDB_CHILDREN) {
    sfs_hash child, key;
    
    child = fhdb_indir_add (indirv[level], iv);
    key = indirv[level].key[0];    

    indirv[level].key.clear ();
    indirv[level].child.clear ();

    fhdb_insert (indirv, child, key, level + 1);
 
    // Top of the tree?
    if (indirv[level + 1].child.size () != 1)
      fhdb_propagate (indirv, level + 1, iv);
   
  }
}

/* Like propagate, but for finalizer.
   Requires: No empty indirvs at bottom of tree.  At least one indir.
 */
void fhdb_finalize_propagate (vec<fhdb_indir_temp> &indirv, char *iv)
{
  assert (indirv.size () != 0);

  // Top of tree with single hash
  if ((indirv.size () == 1) && (indirv[0].child.size () == 1))
    return;


  //  Is it possible to have an inner node with one child?  If so, compress that level
  //  upwards?  That is, cut out this indir, and embed in parent.

  sfs_hash child, key;

  // Is this a singular node?  Then just move upwards.
  if (indirv.front ().child.size () == 1) {
    assert (indirv[0].child.size () == indirv[0].key.size ());

    child = indirv.front ().child[0];
    key = indirv.front ().key[0];    
    
  } else {
    
    child = fhdb_indir_add (indirv.front (), iv);
    key = indirv.front ().key[0];    
  }
   
  indirv.pop_front ();
  fhdb_insert (indirv, child, key, 0);
  fhdb_finalize_propagate (indirv, iv);

}

/*
  Given: Vec of indirs
  Effects: Write to database.
  Requires: No indirs are full.  All must be partially full or empty.
 */
sfs_hash fhdb_finalize (vec<fhdb_indir_temp> &indirv, char *iv)
{

  // Get rid of empty indirvs at the bottom of tree
  while (indirv.front ().child.size () == 0)
    indirv.pop_front ();

  fhdb_finalize_propagate (indirv, iv);
  
  // At this point, the fhdb is the child of the backmost indirv.
  assert (indirv.back ().child.size () == 1);
  return (indirv.back ().child[0]);


}

void fhdb_copydb ()
{
#if 0
#ifdef SLEEPYCAT
  DBT key, content;
  DBC *cursor;

  bzero (&key,     sizeof (key));
  bzero (&content, sizeof (content));

  sfsrofhdb->cursor (sfsrofhdb, NULL, &cursor, 0);

  while (cursor->c_get (cursor, &key, &content, DB_NEXT)
	 != DB_NOTFOUND) {
    
    if (!sfsrodb_put (sfsrodb, key.data, key.size, 
		      content.data, content.size)) {

#else
    bIteration *cursor = new bIteration();
    record *item;
    
    while (sfsrofhdb->iterate(cursor, &item) == 0) {
      bSize_t keySize, contentSize;
      void *keyData = item->getKey(&keySize);
      void *contentData = item->getValue(&contentSize);
      if (!sfsrodb_put (sfsrodb, keyData, keySize, 
			contentData, contentSize)) {
#endif
      warn << "Found identical fhdb, compressing.\n";
      identical_fhdb++;
      } 
    }
#endif
}
    
int
create_fhdb (sfs_hash *fhdb, const char *dbfile, char *iv)
{
  ref<dbImplInfo> info = dbGetImplInfo();

  //create the generic object
  dbfe* db = new dbfe();

  //set up the options we want
  dbOptions opts;
  //ideally, we would check the validity of these...
  opts.addOption("opt_async", 0);
  opts.addOption("opt_cachesize", 80000);
  opts.addOption("opt_nodesize", 4096);

  int err = db->createdb("/tmp/fhdb.db", opts);
  err = db->opendb("/tmp/fhdb.db", opts);
  if (err) {
    warn << "open returned: " << strerror(err) << err << "\n";
    exit (1);
  }

  vec<sfs_hash> fhv;
  vec<fhdb_indir_temp> indirv;
  sfs_hash fh;

#if 0
#ifdef SLEEPYCAT
  while (cursor->c_get (cursor, &key, &content, DB_NEXT)
	   != DB_NOTFOUND) { 
    memcpy (fh.base (), key.data, key.size);
#else
  record *item;
  while (sfsrodb->iterate(cursor, &item) == 0) {
    bSize_t keySize;
    void *keyData = item->getKey(&keySize);
    memcpy(fh.base(), keyData, keySize);
    delete item;
#endif


    fhv.push_back (fh);

    if (fhv.size () == SFSRO_FHDB_CHILDREN) {

      fh = fhdb_dir_add (fhv, iv);
      fhdb_insert (indirv, fh, fhv[0], 0);
      fhdb_propagate (indirv, 0, iv);
      fhv.clear ();
      
    }
  }
  
#endif

  if (fhv.size () > 0) {
    
    fh = fhdb_dir_add (fhv, iv);
    fhdb_insert (indirv, fh, fhv[0], 0);
    fhdb_propagate (indirv, 0, iv);
    fhv.clear ();
  }
  
  *fhdb = fhdb_finalize (indirv, iv); 
  
  fhdb_copydb ();

  sfsrofhdb->closedb();

  if (!fhdb) {
    warnx << "fhdb null!\n";
    return -1;
  }
  
  return 0;
}
