#include "dhash_store.h"
#include "location.h"
#include "misc_utils.h"
#include "locationtable.h"

// ---------------------------------------------------------------------------
// DHASH_STORE
//     - store a give block of data on a give nodeID.
//     - the address of the nodeID must already by in the location cache.
//     - XXX don't really handle RACE conditions..

#define STORE_TIMEOUT 60
    
void 
dhash_store::start ()
{
  error = false;
  status = DHASH_OK;
  npending = 0;
  nextblock = 0;
  numblocks = 0;
  int blockno = 0;
  
  if (dcb)
    timecb_remove (dcb);
  
  dcb = delaycb
    (STORE_TIMEOUT, wrap (mkref(this), &dhash_store::timed_out));
  
  size_t nstored = 0;
  while (nstored < block->len) {
    size_t chunklen = MIN (MTU, block->len - nstored);
    char  *chunkdat = &block->data[nstored];
    size_t chunkoff = nstored;
    npending++;
    store (dest, bid, chunkdat, chunklen, chunkoff, 
	   block->len, blockno, ctype, store_type);
    nstored += chunklen;
    blockno++;
  }
  numblocks = blockno;
}


void 
dhash_store::finish (ptr<dhash_storeres> res, int num, clnt_stat err)
{
  npending--;
  chord_node pred_node;

  if (err) {
    error = true;
    warn << "dhash_store failed: " << bid << ": RPC error" << "\n";
  } 

  else if (res->status != DHASH_OK) {
    if (res->status == DHASH_RETRY) {
      pred_node = make_chord_node (res->pred->p);
    } else if (res->status != DHASH_WAIT)
      warn << "dhash_store failed: " << bid << ": "
	   << dhasherr2str(res->status) << "\n";
    if (!error)
      status = res->status;
    error = true;
  } else { 
    if ((num > nextblock) && (numblocks - num > 1)) {
      warn << "(store) FAST retransmit: " << bid << " got " 
	   << num << " chunk " << nextblock << " of " << numblocks 
	   << " being retransmitted\n";
      clntnode->resendRPC(seqnos[nextblock]);
      //only one per fetch; finding more is too much bookkeeping
      numblocks = -1;
    }
    nextblock++;
  }

  if (npending == 0) {
    if (status == DHASH_RETRY) {
      ptr<location> pn = clntnode->locations->lookup_or_create (pred_node);
      if (!pn && !returned) {
	(*cb) (DHASH_CHORDERR, dest->id (), false);
	returned = true;
	return;
      }
      num_retries++;
      if (num_retries > 2) {
	if (!returned) {
	  (*cb)(DHASH_RETRY, dest->id (), false);
	  returned = true;
	  return;
	}
      } else {
	warn << "retrying (" << num_retries << "): dest was " 
	     << dest->id () << " now is " << pred_node.x << "\n";
	dest = pn;
	start ();
      }
    }
    else
      done (res->resok->already_present);
  }
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
  arg->dbtype  = blockID.dbtype;
  clntnode->my_location ()->fill_node (arg->from);
  arg->data.setsize (len);
  memcpy (arg->data.base (), data, len);
  arg->offset  = off;
  arg->type    = store_type;
  arg->nonce   = 0; // XXX remove!
  arg->attr.size     = totsz;
  //    arg->last    = last;
    
  long rexmitid = clntnode->doRPC
    (dest, dhash_program_1, DHASHPROC_STORE, arg, res,
     wrap (mkref(this), &dhash_store::finish, res, num));
  seqnos.push_back (rexmitid);
}

void 
dhash_store::done (bool present)
{
  if (!returned && npending == 0) {
    (*cb) (status, dest->id (), present);
    returned = true;
  }
}


void 
dhash_store::timed_out ()
{
  dcb = 0;
  error = true;
  status = DHASH_TIMEDOUT;
  npending = 0;
  done (false);
}



