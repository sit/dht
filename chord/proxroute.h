#ifndef _PROXROUTE_H_
#define _PROXROUTE_H_

#include "chord_impl.h"
#include "fingerroute.h"
#include "prox_prot.h"

class finger_table;
class toe_table;

class proxroute : public fingerroute {
  ptr<toe_table> toes;

  void dofindtoes (user_args *sbp, prox_findtoes_arg *ta);
  void dogettoes (user_args *sbp, prox_gettoes_arg *ta);

  void get_toes_cb (cbchordIDlist_t cb,
		    chordID x, chord_nodelistres *res, clnt_stat err);

  ptr<location> closestgreedpred (const chordID &x, const vec<float> &n,
				  const vec<chordID> &failed);
  ptr<location> closestproxpred (const chordID &x, const vec<float> &n,
				 const vec<chordID> &failed);
  
 public:
  static ref<vnode> produce_vnode (ref<chord> _chordnode, 
				   ref<rpc_manager> _rpcm,
				   ref<location> _l);

  proxroute (ref<chord> _chordnode, 
	     ref<rpc_manager> _rpcm,
	     ref<location> _l);
  virtual ~proxroute (void);
  
  virtual void dispatch (user_args *a);
  
  virtual void print (strbuf &outbuf) const;
  virtual ptr<location> closestpred (const chordID &x, const vec<chordID> &failed);
};
#endif /* _PROXROUTE_H_ */
