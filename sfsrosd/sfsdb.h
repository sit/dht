#ifndef _SFSDB_H_
#define _SFSDB_H_

#include "str.h"
#include "sfsro_prot.h"
#include "../sfsrodb/dbfe.h"
#include "dhash_prot.h"
#include "dhash.h"

#define NOTSOPRIVATE_KEY "SK1,,AAAARAAAAACdwf7Qlc/gu/bv0kLFPB/UI/G3FWDgm+sAAEavnBDiiOysNW9pW7D1PEHgVtvYsmorFlZqJVOdfh2kP5VQdlfvAAAARAAAAAFDlFpk2iNzn0MgJupyIcrf4sR6LOUpG+iyDTUn580o97K+1y6k65RJheugvgDSJnfmP6ktTnNNi6EGWyiyhpFrTcfy3b/3WUnJbWXoPdRtlAqdhZ4=,0xc767305cd765cea46fdb44bd81ddb80ccf0e40aaf992bc238ca0f5ad48b89180dfacf05be82e4ffc7d66d8168c438eafd2d9ebcc649e089d80a889d5a67c2ed71f61a231c2349f0743c41c4da68c52474bcff6a08128d004f573a57a5fd8fd781b4d4c4c911e76c9a3c3a4966bc9848e7302381a1b36ef76881e3f69de5f1fe5,latex.lcs.mit.edu"

class sfsrodb {

 ptr<aclnt> dbp;
 ptr <rabin_priv> sk;
  
 public:
  sfsrodb ();
  sfsrodb (const char *dbfile);
  void getinfo (sfs_fsinfo *fsinfo);
  void getconnectres (sfs_connectres *conres);
  void getdata (sfs_hash *fh, sfsro_datares *res,  callback<void>::ref cb);

 private:

  void getdata_cb(callback<void>::ref cb, sfsro_datares *res, dhash_res *res, clnt_stat err);
};

#endif /* _SFSDB_H_ */
