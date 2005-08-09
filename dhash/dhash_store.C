#include "dhash_store.h"
#include "dhblock.h"

#include <location.h>
#include <locationtable.h>
#include <misc_utils.h>
#include <modlogger.h>

#define warning modlogger ("dhash_store", modlogger::WARNING)
#define info  modlogger ("dhash_store", modlogger::INFO)
#define trace modlogger ("dhash_store", modlogger::TRACE)

// ---------------------------------------------------------------------------
// DHASH_STORE
//     - store a give block of data on a give nodeID.
//     - the address of the nodeID must already by in the location cache.
//     - XXX don't really handle RACE conditions..

#define STORE_TIMEOUT 60
    
void 
dhash_store::start ()
{

  dcb = delaycb
    (STORE_TIMEOUT, wrap (this, &dhash_store::timed_out, deleted));
 
  unsigned int mtu = dhblock::dhash_mtu ();
  size_t nstored = 0;
  int blockno = 0;
  while (nstored < data.len ()) {
    size_t chunklen = MIN (mtu, data.len () - nstored);
    char  *chunkdat = (char *)(data.cstr () + nstored);
    size_t chunkoff = nstored;
    npending++;
    store (dest, bid, chunkdat, chunklen, chunkoff, 
	   data.len (), blockno, ctype, store_type);
    nstored += chunklen;
    blockno++;
  }
}


void 
dhash_store::finish (ptr<bool> p_deleted, 
		     ptr<dhash_storeres> res, int num, clnt_stat err)
{
  if (*p_deleted) return;

  npending--;
  chord_node pred_node;

  if (err) {
    trace << "store failed: " << bid << ": RPC error " << err << "\n";
    error = true;
    status = DHASH_RPCERR;
  } else if (res->status != DHASH_OK && !error) {
    status = res->status;
    error = true;
  } else if (res->resok->already_present)
    present = res->resok->already_present;
  

  if (npending == 0) 
    done (present);
  
}


void 
dhash_store::store (ptr<location> dest, blockID blockID, char *data, 
		    size_t len, size_t off, size_t totsz, int num, 
		    dhash_ctype ctype, 
		    store_status store_type)
{
  ref<dhash_storeres> res = New refcounted<dhash_storeres> (DHASH_OK);
  ref<s_dhash_insertarg> arg = New refcounted<s_dhash_insertarg> ();
  arg->key     = blockID.ID;
  arg->ctype   = blockID.ctype;
  arg->data.setsize (len);
  memcpy (arg->data.base (), data, len);
  arg->offset  = off;
  arg->type    = store_type;
  arg->attr.size     = totsz;
    
  bool stream = (totsz > 8000);
  clntnode->doRPC
    (dest, dhash_program_1, DHASHPROC_STORE, arg, res,
     wrap (this, &dhash_store::finish, deleted, res, num), NULL, stream);
    
}

void 
dhash_store::done (bool present)
{
  (*cb) (status, dest->id (), present);
  *deleted = true;
  delete this;
}


void 
dhash_store::timed_out (ptr<bool> p_deleted)
{
  if (*p_deleted) return;

  error = true;
  status = DHASH_TIMEDOUT;
  dcb = NULL;
  done (false);
  // npending might still be > 0;
  // need to wait until all the RPCs really time out, and
  // get collected by finish ().
}
