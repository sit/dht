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
  tailq_entry<doRPC_cbstate> connectlink;
  
  doRPC_cbstate (rpc_program ro, int pi, ptr<void> ini, void *outi,
		 aclnt_cb cbi) : progno (ro), procno (pi), in (ini),  
    out (outi), cb (cbi) {};
  
};

struct location {
  int refcnt;
  chordID n;
  net_address addr;
  chordID source;
  ptr<axprt_stream> x;
  bool connecting;
  tailq<doRPC_cbstate, &doRPC_cbstate::connectlink> connectlist;
  ihash_entry<location> fhlink;
  bool alive;
  u_int64_t rpcdelay;
  u_int64_t nrpc;
  u_int64_t maxdelay;
  int nout;
  timecb_t *timeout_cb;
  
  location (chordID &_n, net_address &_r, chordID _source) : 
    n (_n), addr (_r), source (_source) {
    connecting = false; 
    x = NULL;
    nout = 0;
    timeout_cb = NULL;
    refcnt = 0;
    rpcdelay = 0;
    nrpc = 0;
    maxdelay = 0;
  };
  location (chordID &_n, sfs_hostname _s, int _p, chordID &_source) : n (_n) {
    addr.hostname = _s;
    addr.port = _p;
    source = _source;
    connecting = false;
    x = NULL;
    nout = 0;
    timeout_cb = NULL;
    refcnt = 0;
    rpcdelay = 0;
    nrpc = 0;
    maxdelay = 0;
  }

  ~location () {
    warnx << "~location: delete " << n << "\n";
  }
};

struct node {
  chordID n;
  bool alive;
};

class locationtable : public virtual refcount {
  ihash<chordID,location,&location::n,&location::fhlink,hashID> locs;
  ptr<chord> chordnode;
  u_int64_t rpcdelay;
  u_int64_t nrpc;
  u_int64_t nrpcfailed;

  void connect_cb (callback<void, ptr<axprt_stream> >::ref cb, int fd);
  void doRPCcb (aclnt_cb cb, location *l, u_int64_t s, clnt_stat err);
  void dorpc_connect_cb(location *l, ptr<axprt_stream> x);
  void chord_connect(chordID ID, callback<void, ptr<axprt_stream> >::ref cb);
  void timeout(location *l);
  void decrefcnt (location *l);
 public:
  locationtable (ptr<chord> _chordnode, int set_rpcdelay);
  void insert (chordID &_n, sfs_hostname _s, int _p, chordID &_source);
  location *getlocation (chordID &x);
  void deleteloc (chordID &n);
  void cacheloc (chordID &x, net_address &r, chordID &source);
  void updateloc (chordID &x, net_address &r, chordID &source);
  void increfcnt (chordID &n);
  bool lookup_anyloc (chordID &n, chordID *r);
  chordID findsuccloc (chordID x);
  chordID findpredloc (chordID x);
  net_address & getaddress (chordID &x);
  chordID query_location_table (chordID x);
  void changenode (node *n, chordID &n, net_address &r);
  void replacenode (node *n);
  void checkrefcnt (int i);
  void doRPC (chordID &n, rpc_program progno, int procno, ptr<void> in, 
	      void *out, aclnt_cb cb);
  void stats ();
};

#endif _LOCATION_H_
