#include <dhashgateway_prot.h>
#include <route.h>

struct insert_info { 
  chordID key;
  vec<chordID> path;
  insert_info (chordID k, vec<chordID> p) :
    key (k), path (p) {};
};

class dhash_block;

typedef callback<void, dhash_stat, ptr<insert_info> >::ref cbinsertgw_t;
typedef callback<void, dhash_stat, ptr<dhash_block>, vec<chordID> >::ptr cb_cret;

struct option_block {
  int flags;
  chordID guess;
};

class dhashclient {
 private:
  ptr<aclnt> gwclnt;

  // inserts under the specified key
  // (buf need not remain involatile after the call returns)
  void insert (bigint key, const char *buf, size_t buflen, 
	       cbinsertgw_t cb,  dhash_ctype t, 
	       size_t realsize, ptr<option_block> options = NULL);

  void insertcb (cbinsertgw_t cb, bigint key, 
		 ptr<dhash_insert_res>, clnt_stat err);
  void retrievecb (cb_cret cb, bigint key,  
		   ref<dhash_retrieve_res> res, clnt_stat err);

 public:
  // sockname is the unix path (ex. /tmp/chord-sock) used
  // to communicate to lsd. 
  dhashclient(str sockname);

  //this version connects to the dhash service on TCP
  // this is for RSC
  dhashclient(ptr<axprt_stream> xprt);

  void append (chordID to, const char *buf, size_t buflen, cbinsertgw_t cb);

  // inserts under the contents hash. 
  // (buf need not remain involatile after the call returns)
  void insert (const char *buf, size_t buflen, cbinsertgw_t cb, 
	       ptr<option_block> options = NULL);
  void insert (bigint key, const char *buf, size_t buflen, cbinsertgw_t cb,
               ptr<option_block> options = NULL);

  // insert under hash of public key
  void insert (ptr<sfspriv> key, const char *buf, size_t buflen, long ver,
               cbinsertgw_t cb, ptr<option_block> options = NULL);
  void insert (sfs_pubkey2 pk, sfs_sig2 sig, const char *buf, size_t buflen,
               long ver, cbinsertgw_t cb, ptr<option_block> options = NULL);
  void insert (bigint hash, sfs_pubkey2 pk, sfs_sig2 sig,
               const char *buf, size_t buflen, long ver,
	       cbinsertgw_t cb, ptr<option_block> options = NULL);

  // retrieve block and verify
  void retrieve (bigint key, cb_cret cb, ptr<option_block> options = NULL);
  void retrieve (bigint key, dhash_ctype ct, cb_cret cb, 
		 ptr<option_block> options = NULL);

};
