#ifndef _SFSDB_H_
#define _SFSDB_H_

#include "str.h"
#include "sfsro_prot.h"
#include "../sfsrodb/dbfe.h"

class sfsrodb {

 dbfe *dbp;

 public:
  sfsrodb ();
  sfsrodb (const char *dbfile);
  void getinfo (sfs_fsinfo *fsinfo, callback<void>::ref cb);
  void getconnectres (sfs_connectres *conres, callback<void>::ref cb);
  void getdata (sfs_hash *fh, sfsro_datares *res,  callback<void>::ref cb);
  void getpartialdata (sfs_hash *fh, sfsro_datares *res,  
		       callback<void>::ref cb,int fraction, int whole);
  void putdata (sfs_hash *fh, sfsro_datares *res);

 private:
  void getinfo_cb(callback<void>::ref cb, sfs_fsinfo *fsinfo, ptr<dbrec> res);
  void getconnectres_cb(callback<void>::ref cb, sfs_connectres *cres, 
			ptr<dbrec> res);
  void getdata_cb(callback<void>::ref cb, sfsro_datares *res, ptr<dbrec> res);
  void getpartialdata_cb(callback<void>::ref cb, sfsro_datares *res, 
		  int fraction,
		  int whole,
		  ptr<dbrec> res);

  void putdata_cb (int err);
};

#endif /* _SFSDB_H_ */
