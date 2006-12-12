#ifndef __SAMPLER_H
#define __SAMPLER_H 1

#include "dhash_types.h"
#include <tame.h>

class adb;
class locationtable;
class location;

class sampler {
  ptr<locationtable> locations;

  dhash_ctype ctype;

  u_int dfrags;
  u_int efrags;

  ptr<location> host_loc;
  ptr<adb> db;

  int replica_timer;

public:

  sampler::sampler (ptr<locationtable> locations,
		  ptr<location> h,
		  str dbname,
		  str dbext,
		  dhash_ctype ctype,
		  u_int dfrags = 0,
		  u_int efrags = 0);
  ~sampler ();

  ptr<adb> get_db () { return db; }
  
private:

  ptr<aclnt> _pred_client;
  ptr<location> _last_pred;
  ptr<aclnt> _succ_client;
  ptr<location> _last_succ;

  void doRPC (const rpc_program &prog,
	      int procno, const void *in, void *out, aclnt_cb cb);
  
  void sample_replicas ();
  void resched ();
  void update_neighbors (CLOSURE);
  void sample_replicas_gotneighbors (ptr<location> pred,
				     ptr<location> succ,
				     vec<ptr<location> > preds);

  void wrap_call_getkeys (ptr<location> pred, bool pred, chordID rngmin, 
			  callback<void>::ref cb);
  void call_getkeys (ptr<location> pred, bool pred, chordID rngmin, 
		     callback<void>::ref cb, CLOSURE);
};

#endif // __SAMPLER_H
