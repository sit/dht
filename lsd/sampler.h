#ifndef __SAMPLER_H
#define __SAMPLER_H 1

#include "dhash_types.h"
#include "libadb.h"
#include <sample_prot.h>

class locationtable;
class location;

typedef callback<void, ptr<location> >::ref  cb_location;
typedef callback<void, vec<ptr<location> > >::ref cb_locationlist;

class sampler {
  ptr<locationtable> locations;

  dhash_ctype ctype;

  u_int dfrags;
  u_int efrags;

  ptr<location> host_loc;
  ptr<adb> db;

  u_int cur_succ;

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

  ptr<adb> get_db() { return db; }
  
private:

  ptr<aclnt> _client;
  ptr<location> _last_pred;

  void doRPC (const rpc_program &prog,
	      int procno, const void *in, void *out, aclnt_cb cb);
  void update_pred (cb_location cb);
  void update_pred_cb (cb_location cb,  chord_noderes *res, clnt_stat err);
  
  void get_succlist (cb_locationlist cb);
  void get_succlist_cb (chord_nodelistres *res,
			cb_locationlist cb,
			clnt_stat status);
  
  void sample_replicas ();
  void sample_replicas_predupdated (ptr<location> pred);
  void sample_replicas_gotsucclist (ptr<location> pred,
				  vec<ptr<location> > succs);

  void tcp_connect_cb( callback<void>::ptr cb, int fd );
  void call_getkeys( ptr<location> pred );
  void getkeys_done( ref<getkeys_sample_arg> arg, 
		     ref<getkeys_sample_res> res, 
		     clnt_stat err );
};

#endif // __SAMPLER_H
