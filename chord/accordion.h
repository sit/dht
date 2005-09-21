#ifndef _ACCORDIONROUTE_H_
#define _ACCORDIONROUTE_H_

#include <recroute_prot.h>
#include <accordion_prot.h>

#include "chord_impl.h"
#include "route_recchord.h"

//jy: this is all hard coded now
#define BYTES_PER_ID 36
#define BYTES_PER_LOOKUP 76
#define BYTES_PER_LOOKUPCOMPLETE 12


class accordion_table;
class accordion;

class route_accordion : public route_recchord {

 public:
  route_accordion (ptr<vnode> vi, chordID xi) : route_recchord (vi,xi) {arouteid_ = routeid_;};
  route_accordion (ptr<vnode> vi, chordID xi,
		  rpc_program uc_prog,
		  int uc_procno,
		  ptr<void> uc_args) : route_recchord (vi, xi, uc_prog, uc_procno, uc_args) {arouteid_ = routeid_;};
  ~route_accordion() {};

  u_long arouteid_;
  ihash_entry<route_accordion> ahlink_;


  void send (ptr<chordID> guess) { assert(0); }

  void first_hop (cbhop_t cb, ptr<chordID> guess);
};

class sentinfo {
  public:
  sentinfo(unsigned id, time_t now, unsigned o) : routeid_(id), sent_t(now), outstanding(o) {}
  unsigned routeid_;
  time_t sent_t;
  int outstanding;
  vec<ptr<location> > tried;
  ihash_entry<sentinfo> hlink_;
};

class accordion : public vnode_impl {
  protected:
    accordion (ref<chord> _chordnode,
		    ref<rpc_manager> _rpcm,
		    ref<location> _l);

    ~accordion () {}
    ptr<accordion_table> fingers_;

  private:
    time_t starttime;
    void init ();

    void dofillgap (user_args *sbp, accordion_fillgap_arg *ra, ptr<location> src);
    void dogetfingers_ext (user_args *sbp);
    void docomplete (user_args *sbp, recroute_complete_arg *ra, ptr<location> src);
    void doaccroute_sendcomplete (recroute_route_arg *ra,
	                          const vec<ptr<location> > cs);

    void accroute_sent_complete_cb (ptr<location> l, chordID x, clnt_stat status);
    bool accroute_sent_complete_timeout_cb (ptr<location> l, chordID x, chord_node n, int rexmit_number);
    void doaccroute_sendroute (recroute_route_arg *ra, ptr<location> p, bool redun=false, unsigned para=1);
    void fill_gap_cb (ptr<location> l, 
	              cbchordIDlist_t cb,
	              chord_nodelistres *res, 
		      clnt_stat err);
    bool fill_gap_timeout_cb (ptr<location> l,
	              chord_node n,
		      int rexmit_number);

    void accroute_hop_cb (long *seqp, ptr<recroute_route_arg> ra, 
	                  ptr<location> p,
			  chord_nodelistres *res,
			  clnt_stat status);

    bool accroute_hop_timeout_cb (long *seqp, u_int64_t t, ptr<recroute_route_arg> ra,
				  ptr<location> p,
				  chord_node n,
				  int rexmit_number);

    void update_bavail();
    unsigned get_parallelism ();
    void fill_nodelistres (chord_nodelistres *res, vec<ptr<location> > fs);

    ihash<u_long, route_accordion, &route_accordion::arouteid_, &route_accordion::ahlink_> routers;
    ihash<unsigned, sentinfo, &sentinfo::routeid_, &sentinfo::hlink_> sent;

    int budget_;
    int burst_;
    int stopearly_;
    int bavail;
    time_t bavail_t;

    unsigned lookups_;
    unsigned explored_;
    unsigned para_;

  public:
    void stabilize (void);
    void doaccroute (user_args *sbp, recroute_route_arg *ra);
    static ref<vnode> produce_vnode (ref<chord> _chordnode,
				     ref<rpc_manager> _rpcm,
				     ref<location> _l);
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

    virtual void dispatch (user_args *a);
    void fill_gap (ptr<location> n, ptr<location> end, cbchordIDlist_t cb);
    void periodic();

    int get_bavail () { update_bavail(); return bavail; }
    int get_budget () { return budget_; }
    int burst () { return burst_;}
    void explored () { explored_++;}
    void bytes_sent (unsigned b) { bavail -= b; if ((bavail+burst_) < 0) bavail = -burst_; }
    unsigned get_para() { return para_;}
    ptr<location> closestpred (const chordID &x, const vec<chordID> &f) { assert (0); return NULL;}
};

#endif
