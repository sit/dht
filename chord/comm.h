#ifndef _SFSNET_COMM_H_
#define _SFSNET_COMM_H_

#include "chord_types.h"
#include "ihash.h"
#include "arpc.h"
#include <misc_utils.h>
#include "aclnt_chord.h"

class location;

struct rpcstats {
  str key;
  int bytes;
  int calls;

  ihash_entry<rpcstats> h_link;
};

extern ihash<str, rpcstats, &rpcstats::key, &rpcstats::h_link> rpc_stats_tab;



struct sent_elm {
  tailq_entry<sent_elm> q_link;
  long seqno;
  
  sent_elm (long s) : seqno (s) {};
};

struct RPC_delay_args {
  ptr<location> l;
  ptr<location> from;
  const rpc_program &prog;
  int procno;
  ptr<void> in;
  void *out;
  aclnt_cb cb;
  long fake_seqno;
  u_int64_t now;

  tailq_entry<RPC_delay_args> q_link;

  RPC_delay_args (ptr<location> _from, ptr<location> _l, const rpc_program &_prog, int _procno,
		  ptr<void> _in, void *_out, aclnt_cb _cb) :
    l (_l), from (_from), prog (_prog), procno (_procno), in (_in), 
    out (_out), cb (_cb), now (getusec ()) {};

  RPC_delay_args (const rpc_program &_prog, int _procno,
		  ptr<void> _in, void *_out, aclnt_cb _cb) :
    l (NULL), from (NULL), prog (_prog), procno (_procno), in (_in), 
    out (_out), cb (_cb), now (getusec ()) {};
};

struct rpc_state {
  chordID ID;
  ref<location> loc;
  ptr<location> from;
  aclnt_cb cb;
  int progno;
  int procno;
  long seqno;
  long rexmit_ID;

  rpccb_chord *b;
  int rexmits;
  ihash_entry <rpc_state> h_link;

  void *out;

  rpc_state (ptr<location> from, ref<location> l, aclnt_cb c,  long s, int p, void *out);
};

// store latency information about a host.
struct hostinfo {
  chord_hostname host;
  str key;
  u_int64_t nrpc;
  u_int64_t maxdelay;
  float a_lat;
  float a_var;
  int fd;
  ptr<axprt_stream> xp;
  vec<RPC_delay_args *> connect_waiters;
  unsigned orpc; // tcp debugging (benjie)

  ihash_entry<hostinfo> hlink_;
  tailq_entry<hostinfo> lrulink_;

  hostinfo (const net_address &r);
};

// Default implementation, udp aclnt
class rpc_manager {
  static const float GAIN = 0.2;
  virtual void doRPCcb (aclnt_cb realcb, ptr<location> l, u_int64_t sent,
			clnt_stat err);
  
 protected:
  // statistics
  float a_lat;
  float a_var;
  float avg_lat;
  vec<float> lat_history;
  float c_err;
  float c_var;

  // counters
  u_int64_t nrpc;
  u_int64_t nrpcfailed;
  u_int64_t nsent;
  u_int64_t npending;
  ptr<u_int32_t> nrcv;

  ptr<axprt_dgram> dgram_xprt;

  ihash<str, hostinfo, &hostinfo::key, &hostinfo::hlink_> hosts;
  tailq<hostinfo, &hostinfo::lrulink_> hostlru;

  hostinfo *lookup_host (const net_address &r);
  virtual void remove_host (hostinfo *h);
  void update_latency (ptr<location> from, ptr<location> l, u_int64_t lat);

 public:
  virtual void rexmit (long seqno) {};
  virtual void stats ();
  virtual long doRPC (ptr<location> from, ptr<location> l, const rpc_program &prog, int procno,
		 ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);
  virtual long doRPC_dead (ptr<location> l, const rpc_program &prog, int procno,
			   ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);
  // the following may not necessarily make sense for all implementations.
  virtual float get_a_lat (ptr<location> l);
  virtual float get_a_var (ptr<location> l); 
  virtual float get_avg_lat ();
  virtual float get_avg_var ();
  rpc_manager (ptr<u_int32_t> _nrcv);
  virtual ~rpc_manager () {};
};

// "dhashtcp" implementation.
class tcp_manager : public rpc_manager {
  void doRPC_tcp_connect_cb (RPC_delay_args *args, int fd);
  void doRPC_tcp_cleanup (ptr<aclnt> c, RPC_delay_args *args, clnt_stat err);
  void send_RPC (RPC_delay_args *args);
  void send_RPC_ateofcb (RPC_delay_args *args);
  void remove_host (hostinfo *h);

 public:
  void rexmit (long seqno) {};
  void stats ();
  long doRPC (ptr<location> from, ptr<location> l, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);
  long doRPC_dead (ptr<location> l, const rpc_program &prog, int procno,
		   ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);
  tcp_manager (ptr<u_int32_t> _nrcv) : rpc_manager (_nrcv) {}
  ~tcp_manager () {}
};

// congestion control udp implementation.
#define MAX_REXMIT 4
#define MIN_RPC_FAILURE_TIMER 2
class stp_manager : public rpc_manager {
  // state
  vec<float> timers;
  vec<float> cwind_time;
  vec<float> cwind_cwind;
  vec<long> acked_seq;
  vec<float> acked_time;
  vec<long> qued_hist;
  vec<long> lat_inq;

  int seqno;
  float cwind;
  float cwind_ewma;
  float ssthresh;
  int left;
  float cwind_cum;
  int num_cwind_samples;
  int num_qed;
  
  int inflight;

  long fake_seqno;

  u_int64_t st;

  tailq<RPC_delay_args, &RPC_delay_args::q_link> Q;
  tailq<sent_elm, &sent_elm::q_link> sent_Q;
  ihash<long, rpc_state, &rpc_state::rexmit_ID, &rpc_state::h_link> user_rexmit_table;

  timecb_t *idle_timer;

  // methods
  void doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err);
  
  void ratecb ();
  void remove_from_sentq (long acked_seqno);
  void update_cwind (int acked);
  void timeout (rpc_state *s);
  void enqueue_rpc (RPC_delay_args *args);
  void rpc_done (long seqno);
  void reset_idle_timer ();
  void idle ();
  void setup_rexmit_timer (ptr<location> from, ptr<location> l, long *sec, long *nsec);
  void timeout_cb (rpc_state *C);
  bool room_in_window ();
 public:
  void stats ();

  void rexmit (long seqno);
  long doRPC (ptr<location> from, ptr<location> l, const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);
  long doRPC_dead (ptr<location> l, const rpc_program &prog, int procno,
		   ptr<void> in, void *out, aclnt_cb cb, long fake_seqno = 0);

  stp_manager (ptr<u_int32_t> _nrcv);
  ~stp_manager ();
};

extern int chord_rpc_style;
extern const int CHORD_RPC_STP;  // our own rpc style
extern const int CHORD_RPC_SFSU; // libarpc over UDP
extern const int CHORD_RPC_SFST; // libarpc over TCP
extern const int CHORD_RPC_SFSBT; // libarpc over TCP, no caching

#endif /* _SFSNET_COMM_H_ */
