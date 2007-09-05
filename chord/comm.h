#ifndef _SFSNET_COMM_H_
#define _SFSNET_COMM_H_

#include "chord_types.h"
#include "chord.h"
#include "ihash.h"
#include "arpc.h"
#include <misc_utils.h>
#include "aclnt_chord.h"

#define max_host_cache 64

class location;

/* Maintain statistics about outbound bandwidth usage */
struct rpcstats {
  str key;
  size_t call_bytes;
  size_t ncall;
  size_t rexmit_bytes;
  size_t nrexmit;
  size_t reply_bytes;
  size_t nreply;
  u_int64_t latency_ewma; 
  ihash_entry<rpcstats> h_link;

  rpcstats (str k) :
    key (k),
    call_bytes (0), ncall (0),
    rexmit_bytes (0), nrexmit (0),
    reply_bytes (0), nreply (0),
    latency_ewma (0)
  {}
};

void track_call (const rpc_program &prog, int procno, size_t b);
void track_rexmit (const rpc_program &prog, int procno, size_t b);
void track_rexmit (int progno, int procno, size_t b);
void track_reply (const rpc_program &prog, int procno, size_t b);
void track_proctime (const rpc_program &prog, int procno, u_int64_t l);

extern ihash<str, rpcstats, &rpcstats::key, &rpcstats::h_link> rpc_stats_tab;
extern u_int64_t rpc_stats_lastclear;


struct RPC_delay_args {
  ptr<location> l;
  ptr<location> from;
  const rpc_program &prog;
  int procno;
  ptr<void> in;
  void *out;
  aclnt_cb cb;
  cbtmo_t cb_tmo;
  u_int64_t now;

  tailq_entry<RPC_delay_args> q_link;

  RPC_delay_args (ptr<location> _from, ptr<location> _l, 
		  const rpc_program &_prog, int _procno,
		  ptr<void> _in, void *_out, aclnt_cb _cb,
		  cbtmo_t _cb_tmo) :
    l (_l), from (_from), prog (_prog), procno (_procno), in (_in), 
    out (_out), cb (_cb), cb_tmo (_cb_tmo), now (getusec ()) {};

  RPC_delay_args (const rpc_program &_prog, int _procno,
		  ptr<void> _in, void *_out, aclnt_cb _cb,
		  cbtmo_t _cb_tmo) :
    l (NULL), from (NULL), prog (_prog), procno (_procno), in (_in), 
    out (_out), cb (_cb), cb_tmo (_cb_tmo), now (getusec ()) {};
};

struct rpc_state {
  chordID ID;
  ref<location> loc;
  ptr<location> from;
  aclnt_cb cb;
  int progno;
  int procno;
  long seqno;
  bool in_window;

  rpccb_chord *b;
  int rexmits;
  u_int64_t sendtime;
  cbtmo_t cb_tmo;

  tailq_entry <rpc_state> q_link;

  void *out;

  rpc_state (ptr<location> from, ref<location> l, aclnt_cb c,  
	     cbtmo_t cb_tmo,
	     long s, int p, void *out);
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

  u_int64_t connect_time;
  u_int64_t last_time;
  u_int64_t last_sent;
  u_int64_t last_bw;
  timecb_t *bwcb;
  void update_bw ();

  ihash_entry<hostinfo> hlink_;
  tailq_entry<hostinfo> lrulink_;

  hostinfo (const net_address &r);
  ~hostinfo ();
};

// Default implementation, udp aclnt
class rpc_manager {
  static const float GAIN;
  virtual void doRPCcb (aclnt_cb realcb, ptr<location> l, u_int64_t sent,
			clnt_stat err);
  
 protected:
  // statistics
  float a_lat;
  float a_var;
  float c_err;
  float c_err_rel;
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
  void update_latency (ptr<location> from, ptr<location> l, u_int64_t senttime);
  void count_rpc (ptr<location> l, hostinfo *h = NULL);

 public:
  virtual void stats (const strbuf &o);
  virtual long doRPC (ptr<location> from, ptr<location> l, 
		      const rpc_program &prog, int procno,
		      ptr<void> in, void *out, aclnt_cb cb, 
		      cbtmo_t cb_tmo = NULL);
  virtual long doRPC_dead (ptr<location> l, 
			   const rpc_program &prog, 
			   int procno,
			   ptr<void> in, 
			   void *out, 
			   aclnt_cb cb);

  virtual long doRPC_stream (ptr<location> from,
			     ptr<location> l, 
			     const rpc_program &prog, 
			     int procno,
			     ptr<void> in, 
			     void *out, 
			     aclnt_cb cb)
  { return doRPC (from, l, prog, procno, in, out, cb, NULL); }

  // the following may not necessarily make sense for all implementations.
  virtual float get_a_lat (ptr<location> l);
  virtual float get_a_var (ptr<location> l); 
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
  long doRPC (ptr<location> from, ptr<location> l, 
	      const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, 
	      cbtmo_t cb_tmo = NULL);

  long doRPC_stream (ptr<location> from, ptr<location> l, 
		     const rpc_program &prog, 
		     int procno,
		     ptr<void> in, 
		     void *out, 
		     aclnt_cb cb)
  { return doRPC (from, l, prog, procno, in, out, cb, NULL); }
  
  long doRPC_dead (ptr<location> l, const rpc_program &prog, int procno,
		   ptr<void> in, void *out, aclnt_cb cb);
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
  vec<long> qued_hist;
  vec<long> lat_inq;

  int seqno;
  float cwind;
  float cwind_ewma;
  float ssthresh;
  float cwind_cum;
  int num_cwind_samples;
  int num_qed;
  
  int inflight;

  u_int64_t st;

  tailq<RPC_delay_args, &RPC_delay_args::q_link> Q;
  tailq<rpc_state, &rpc_state::q_link> pending;

  timecb_t *idle_timer;

  ptr<tcp_manager> stream_rpcm;

  // methods
  void doRPCcb (ref<aclnt> c, rpc_state *C, clnt_stat err);
  
  void ratecb ();
  void remove_from_sentq (long acked_seqno);
  void update_cwind (int acked);
  bool timeout (rpc_state *s);
  void enqueue_rpc (RPC_delay_args *args);
  void rpc_done (long seqno);
  void reset_idle_timer ();
  void idle ();
  void setup_rexmit_timer (ptr<location> from, ptr<location> l, long *sec, long *nsec);
  void timeout_cb (rpc_state *C);
  bool room_in_window ();
 public:
  void stats (const strbuf &o);

  long doRPC (ptr<location> from, ptr<location> l, 
	      const rpc_program &prog, int procno,
	      ptr<void> in, void *out, aclnt_cb cb, 
	      cbtmo_t cb_tmo = NULL);
  long doRPC_dead (ptr<location> l, const rpc_program &prog, int procno,
		   ptr<void> in, void *out, aclnt_cb cb);
  long doRPC_stream (ptr<location> from, ptr<location> l, 
		     const rpc_program &prog, 
		     int procno,
		     ptr<void> in, 
		     void *out, 
		     aclnt_cb cb);

  stp_manager (ptr<u_int32_t> _nrcv);
  ~stp_manager ();
};

enum chord_rpc_style_t {
  CHORD_RPC_STP   = 0, // our own rpc style
  CHORD_RPC_SFSU  = 1, // libarpc over UDP
  CHORD_RPC_SFST  = 2, // libarpc over TCP
  CHORD_RPC_SFSBT = 3  // libarpc over TCP, no caching
};
extern chord_rpc_style_t chord_rpc_style;

#endif /* _SFSNET_COMM_H_ */
