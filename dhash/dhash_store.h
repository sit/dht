#ifndef __DHASH_STORE__H_
#define __DHASH_STORE__H_

#include <dhash_prot.h>
#include <dhash_common.h>
#include <dhash.h>
#include <route.h>
#include <chord.h>

typedef callback<void, dhash_stat, chordID, bool>::ref cbclistore_t;

//store a block/fragment by sending RPCs smaller than the MTU
class dhash_store : public virtual refcount {

public:
  
  static void execute (ptr<vnode> clntnode, ptr<location> dest, 
		       blockID bid,
		       dhash *dh, ref<dhash_block> block, 
		       cbclistore_t cb, 
		       store_status store_type = DHASH_STORE)
  {
    ptr<dhash_store> d = New refcounted<dhash_store> 
      (clntnode, dest, bid, dh, block, store_type, cb);
  }


protected:
  uint npending;
  bool error;
  dhash_stat status;

  dhash *dh;
  ptr<location> dest;
  ptr<dhash_block> block;
  blockID bid;
  cbclistore_t cb;
  dhash_ctype ctype;
  store_status store_type;
  ptr<vnode> clntnode;

  int num_retries;
  int nextblock;
  int numblocks;
  vec<long> seqnos;
  timecb_t *dcb;
  bool returned;

  dhash_store (ptr<vnode> clntnode, ptr<location> dest, blockID bid,
               dhash *dh, ptr<dhash_block> _block, store_status store_type, 
	       cbclistore_t cb)
    : dh (dh), dest (dest), block (_block), bid (bid), cb (cb),
      ctype (_block->ctype), store_type (store_type),
      clntnode (clntnode), num_retries (0)
  {
    returned = false;
    dcb = NULL;
    start ();
  }
  
  ~dhash_store ()
  {
    if (dcb)
      timecb_remove (dcb);
    dcb = NULL;
  }

  void done (bool present);
  void timed_out ();
  void start ();
  void finish (ptr<dhash_storeres> res, int num, clnt_stat err);
  void store (ptr<location> dest, blockID blockID, char *data, size_t len,
	      size_t off, size_t totsz, int num, dhash_ctype ctype, 
	      store_status store_type);  

};

#endif
