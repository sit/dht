#include <dhash_prot.h>
#include <route.h>
class insert_info;
class dhash_block;


typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;

class dhash_block;

typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;
typedef callback<void, dhash_stat, ptr<dhash_block>, vec<chordID> >::ptr cb_cret;

class dhashclient {
 private:
  ptr<aclnt> gwclnt;

  // inserts under the specified key
  // (buf need not remain involatile after the call returns)
  void insert (bigint key, const char *buf, size_t buflen, 
	       cbinsertgw_t cb,  dhash_ctype t, int options, size_t realsize);
  void insertcb (cbinsertgw_t cb, bigint key, 
		 ptr<dhash_insert_res>, clnt_stat err);
  void retrievecb (cb_cret cb, bigint key,  
		   ref<dhash_retrieve_res> res, clnt_stat err);

 public:
  // sockname is the unix path (ex. /tmp/chord-sock) used
  // to communicate to lsd. 
  dhashclient(str sockname);

  void append (chordID to, const char *buf, size_t buflen, cbinsertgw_t cb);

  // inserts under the contents hash. 
  // (buf need not remain involatile after the call returns)
  void insert (const char *buf, size_t buflen, cbinsertgw_t cb, int options = 0);
  void insert (bigint key, const char *buf, size_t buflen, cbinsertgw_t cb,
               int options = 0);

  // insert under hash of public key
  void insert (ptr<sfspriv> key, const char *buf, size_t buflen, long ver,
               cbinsertgw_t cb, int options = 0);
  void insert (sfs_pubkey2 pk, sfs_sig2 sig, const char *buf, size_t buflen,
               long ver, cbinsertgw_t cb, int options = 0);
  void insert (bigint hash, sfs_pubkey2 pk, sfs_sig2 sig,
               const char *buf, size_t buflen, long ver,
	       cbinsertgw_t cb, int options = 0);

  // retrieve block and verify
  void retrieve (bigint key, cb_cret cb, int options = 0);
  void retrieve (bigint key, dhash_ctype ct, cb_cret cb, int options = 0);

  // synchronouslly call setactive.
  // Returns true on error, and false on success.
  bool sync_setactive (int32 n);
};
