#ifndef _FINGERROUTE_H_
#define _FINGERROUTE_H_

#include "chord_impl.h"
#include <fingers_prot.h>

class finger_table;

class fingerroute : public vnode_impl {
 protected:
  fingerroute (ref<chord> _chordnode, 
	       ref<rpc_manager> _rpcm,
	       ref<location> _l,
	       cb_fingertableproducer_t ftp);
    
  fingerroute (ref<chord> _chordnode, 
	       ref<rpc_manager> _rpcm,
	       ref<location> _l);

  ptr<finger_table> fingers_;
  
 private:
  bool gotfingers_; // fed locationtable with pred's fingers?

  void init ();
  
  void dogetfingers (user_args *sbp);
  void dogetfingers_ext (user_args *sbp);

  void first_fingers ();
  void first_fingers_cb (vec<chord_node> nlist, chordstat s);
  void get_fingers_cb (cbchordIDlist_t cb,
		       chordID x, chord_nodelistres *res, clnt_stat err);
  
 public:
  static ref<vnode> produce_vnode (ref<chord> _chordnode, 
				   ref<rpc_manager> _rpcm,
				   ref<location> _l);

  virtual ~fingerroute (void);
  
  virtual void dispatch (user_args *a);
  virtual void print (strbuf &outbuf) const;
  virtual vec<ptr<location> > fingers ();

  void get_fingers (ptr<location> n, cbchordIDlist_t cb);
  virtual ptr<location> closestpred (const chordID &x, const vec<chordID> &f);
};

#endif /* _FINGERROUTE_H_ */
