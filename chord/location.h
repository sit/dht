#ifndef _LOCATION_H_
#define _LOCATION_H_

/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2, or (at
 * your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
 * USA
 *
 */

class chord;

// the identifier for the ihash class
struct hashID {
  hashID () {}
  hash_t operator() (const chordID &ID) const {
    return ID.getui ();
  }
};

struct rpc_args {
  chordID ID;
  rpc_program progno;
  int procno;
  const void *in;
  void *out;
  aclnt_cb cb;
  rpc_args(chordID id, rpc_program progn, int proc, const void *i, void *o, 
	   aclnt_cb c) : ID (id), progno (progn), procno (proc),in (i),
    out (o), cb (c){};
};

struct location;

struct doRPC_cbstate {
  rpc_program progno;
  int procno;
  ptr<void> in;
  void *out;
  aclnt_cb cb;
  chordID ID;
  tailq_entry<doRPC_cbstate> connectlink;

  doRPC_cbstate (rpc_program ro, int pi, ptr<void> ini, void *outi,
		 aclnt_cb cbi, chordID id) : progno (ro), procno (pi), 
		   in (ini), out (outi), cb (cbi), ID (id) {};
};


struct frpc_state {
  chord_RPC_res *res;
  void *out;
  rpc_program prog;
  int procno;
  aclnt_cb cb;
  location *l;
  u_int64_t s;

  frpc_state (chord_RPC_res *r, void *o, rpc_program p, int pr, aclnt_cb c,
		location *L, u_int64_t S) : res (r), out (o), prog (p), 
    procno (pr), cb (c), l (L), s (S) {};
};

struct location {
  int refcnt;	// locs w. refcnt == 0 are in the cache; refcnt > 0 are fingers
  chordID n;
  net_address addr;
  ptr<axprt_stream> x;
  tailq<doRPC_cbstate, &doRPC_cbstate::connectlink> connectlist;
  ihash_entry<location> fhlink;
  tailq_entry<location> cachelink;
  tailq_entry<location> connlink;
  tailq_entry<location> delaylink;

  u_int64_t rpcdelay;
  u_int64_t nrpc;
  u_int64_t maxdelay;

  location (chordID &_n, net_address &_r) : n (_n), addr (_r) {
    x = NULL;
    refcnt = 0;
    rpcdelay = 0;
    nrpc = 0;
    maxdelay = 0;
  };
  location (chordID &_n, sfs_hostname _s, int _p) : n (_n) {
    addr.hostname = _s;
    addr.port = _p;
    x = NULL;
    refcnt = 0;
    rpcdelay = 0;
    nrpc = 0;
    maxdelay = 0;
  };
  ~location () {
    warnx << "~location: delete " << n << "\n";
  }
};

struct node {
  chordID n;
  bool alive;
};

class locationtable : public virtual refcount {
  static const int delayed_timer = 1;  // seconds

  ptr<chord> chordnode;
  ihash<chordID,location,&location::n,&location::fhlink,hashID> locs;
  tailq<location, &location::cachelink> cachedlocs;  // the cached location
  tailq<location, &location::connlink> connections;  // active connections
  tailq<location, &location::delaylink> delayedconnections;
  timecb_t *delayed_tmo;
  int size_cachedlocs;
  int max_cachedlocs;
  int size_connections;
  int max_connections;
  u_int64_t rpcdelay;
  u_int64_t nrpc;
  u_int64_t nrpcfailed;
  unsigned nconnections;
  unsigned ndelayedconnections;

  u_long nnodessum;
  u_long nnodes;
  unsigned nvnodes;

  qhash<long, svccb *> octbl;
  unsigned long last_xid;
  
  locationtable () : last_xid (0) {};

  void connect_cb (location *l, callback<void, ptr<axprt_stream> >::ref cb, 
		   int fd);
  void doRPCcb (aclnt_cb cb, location *l, u_int64_t s, clnt_stat err);
  void dorpc_connect_cb(location *l, ptr<axprt_stream> x);
  void chord_connect(chordID ID, callback<void, ptr<axprt_stream> >::ref cb);
  void decrefcnt (location *l);
  void touch_cachedlocs (location *l);
  void add_cachedlocs (location *l);
  void delete_cachedlocs (void);
  void remove_cachedlocs (location *l);
  void touch_connections (location *l);
  void delete_connections (location *l);
  void add_connections (location *l);
  void delay_connections (location *l);
  void cleanup_connections ();
  void remove_connections (location *l);
  bool present_connections(location *l);
 public:
  locationtable (ptr<chord> _chordnode, int set_rpcdelay, int _max_cache,
		 int _max_connections);
  bool betterpred1 (chordID current, chordID target, chordID newpred);
  bool betterpred2 (chordID myID, chordID current, chordID target, 
		    chordID newpred);
  void incvnodes () { nvnodes++; };
  void replace_estimate (u_long o, u_long n);
  void insert (chordID &_n, sfs_hostname _s, int _p);
  location *getlocation (chordID &x);
  void deleteloc (chordID &n);
  void cacheloc (chordID &x, net_address &r);
  void updateloc (chordID &x, net_address &r);
  void increfcnt (chordID &n);
  bool lookup_anyloc (chordID &n, chordID *r);
  chordID findsuccloc (chordID x);
  chordID findpredloc (chordID x);
  net_address & getaddress (chordID &x);
  chordID query_location_table (chordID x);
  void changenode (node *n, chordID &n, net_address &r);
  void checkrefcnt (int i);
  void doRPC (chordID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  void stats ();

  long new_xid (svccb *sbp);
  void reply (long xid, void *out, long outlen);
  bool doForeignRPC (rpc_program prog, 
		     unsigned long procno,
		     void *in, 
		     void *out,
		     chordID ID,
		     aclnt_cb cb);
  void doForeignRPC_cb (frpc_state *C, clnt_stat err);
};

#endif _LOCATION_H_
