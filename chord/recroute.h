#ifndef _RECROUTE_H_
#define _RECROUTE_H_

#include "chord.h"
#include <recroute_prot.h>
#include "route_recchord.h"
#include <ihash.h>

template<class T>
class recroute : public T {
 private:
  ihash<long, route_recchord,
    &route_recchord::routeid_, &route_recchord::hlink_> routers;
  
 protected:
  recroute (ref<chord> _chord, ref<rpc_manager> _rpcm, ref<location> _l);
  ~recroute (void);

  void recroute_hop_cb (ptr<recroute_route_arg> nra, ptr<location> p,
			vec<chordID> failed,
			recroute_route_stat *res,
			clnt_stat status);
  void recroute_sent_complete_cb (clnt_stat status);
  void dispatch (user_args *sbp);

  void dorecroute (user_args *sbp, recroute_route_arg *ra);
  void docomplete (user_args *sbp, recroute_complete_arg *ca);

  void route_cb (cbroute_t cb, route_iterator *ri, bool done);
  
 public:
  static ref<vnode> produce_vnode (ref<chord> _chordnode,
				   ref<rpc_manager> _rpcm,
				   ref<location> _l);
  
  // Override select routing related methods of vnode
  ptr<route_iterator> produce_iterator (chordID xi);
  ptr<route_iterator> produce_iterator (chordID xi,
					const rpc_program &uc_prog,
					int uc_procno,
					ptr<void> uc_args);
  route_iterator *produce_iterator_ptr (chordID xi);
  route_iterator *produce_iterator_ptr (chordID xi,
					const rpc_program &uc_prog,
					int uc_procno,
					ptr<void> uc_args);
};
#endif /* _RECROUTE_H_ */
