#ifndef _SFSNET_COMM_H_
#define _SFSNET_COMM_H_

#include "async.h"

// store latency information about a host.
struct hostinfo {
  sfs_hostname host;
  u_int64_t rpcdelay;
  u_int64_t nrpc;
  u_int64_t maxdelay;
  float a_lat;
  float a_var;

  ihash_entry<hostinfo> hlink_;
  tailq_entry<hostinfo> lrulink_;

  hostinfo (const net_address &r);
};

struct sent_elm {
  tailq_entry<sent_elm> q_link;
  long seqno;
  
  sent_elm (long s) : seqno (s) {};
};

struct RPC_delay_args {
  ptr<location> l;
  rpc_program prog;
  int procno;
  ptr<void> in;
  void *out;
  aclnt_cb cb;

  tailq_entry<RPC_delay_args> q_link;

  RPC_delay_args (ptr<location> _l, rpc_program _prog, int _procno,
		  ptr<void> _in, void *_out, aclnt_cb _cb) :
    l (_l), prog (_prog), procno (_procno), in (_in), out (_out), cb (_cb) {};
};

struct rpc_state {
  chordID ID;
  ref<location> loc;
  aclnt_cb cb;
  u_int64_t s;
  int progno;
  long seqno;

  rpccb_chord *b;
  int rexmits;
  
  rpc_state (ref<location> l, aclnt_cb c, u_int64_t S, long s, int p)
    : loc (l), cb (c),  s (S), progno (p), seqno (s),
      b (NULL), rexmits (0)
  {
    ID = l->n;
  };
};

// Default implementation, udp aclnt
class rpc_manager {
  virtual void doRPCcb (aclnt_cb realcb, clnt_stat err);
 protected:
  // counters
  u_int64_t nrpc;
  u_int64_t nrpcfailed;
  u_int64_t nsent;
  u_int64_t npending;

  ptr<axprt_dgram> dgram_xprt;
  ptr<chord> chordnode;
  
 public:
  virtual void stats ();
  virtual void doRPC (ptr<location> l, rpc_program prog, int procno,
		 ptr<void> in, void *out, aclnt_cb cb);
  // the following may not necessarily make sense for all implementations.
  virtual float get_a_lat (ptr<location> l);
  virtual float get_a_var (ptr<location> l); 
  virtual float get_avg_lat ();
  virtual float get_avg_var ();
  rpc_manager (ptr<chord> c);
  virtual ~rpc_manager () {};
};

// "dhashtcp" implementation.
class tcp_manager : public rpc_manager {
  void doRPC_tcp (ptr<location> l, rpc_program progno, 
		  int procno, ptr<void> in, 
		  void *out, aclnt_cb cb);

  void doRPC_tcp_connect_cb (RPC_delay_args *args, int fd);
  void doRPC_tcp_cleanup (RPC_delay_args *args, int fd, clnt_stat err);
 public:
  void doRPC (ptr<location> l, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb);
  tcp_manager (ptr<chord> c) : rpc_manager (c) {}
  ~tcp_manager () {}
};

// congestion control udp implementation.
#define MAX_REXMIT 4
#define MIN_RPC_FAILURE_TIMER 2
class stp_manager : public rpc_manager {
  static const float GAIN = 0.2;
  static const size_t max_host_cache = 100; 

  // statistics
  float a_lat;
  float a_var;
  float avg_lat;
  float bf_lat;
  float bf_var;
  u_int64_t rpcdelay;

  // state
  vec<float> timers;
  vec<float> lat_little;
  vec<float> lat_big;
  vec<float> cwind_time;
  vec<float> cwind_cwind;
  vec<long> acked_seq;
  vec<float> acked_time;

  int seqno;
  float cwind;
  float cwind_ewma;
  float ssthresh;
  int left;
  float cwind_cum;
  int num_cwind_samples;
  int num_qed;

  u_int64_t st;

  tailq<RPC_delay_args, &RPC_delay_args::q_link> Q;
  tailq<sent_elm, &sent_elm::q_link> sent_Q;

  timecb_t *idle_timer;

  tailq<hostinfo, &hostinfo::lrulink_> hostlru;
  ihash<sfs_hostname, hostinfo, &hostinfo::host, &hostinfo::hlink_> hosts;

  hostinfo *lookup_host (const net_address &r);

  // methods
  void doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err);
  
  void update_latency (ptr<location> l, u_int64_t lat, bool bf);
  void ratecb ();
  void update_cwind (int acked);
  void timeout (rpc_state *s);
  void enqueue_rpc (RPC_delay_args *args);
  void rpc_done (long seqno);
  void reset_idle_timer ();
  void idle ();
  void setup_rexmit_timer (hostinfo *h, long *sec, long *nsec);
  void timeout_cb (rpc_state *C);
 public:
  void stats ();
		   
  void doRPC (ptr<location> l, rpc_program prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb);
  float get_a_lat (ptr<location> l);
  float get_a_var (ptr<location> l);
  float get_avg_lat ();
  float get_avg_var ();

  stp_manager (ptr<chord> c);
  ~stp_manager ();
};

#endif /* _SFSNET_COMM_H_ */
