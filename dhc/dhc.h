#ifndef _DHC_H_
#define _DHC_H_

#include <dbfe.h>
#include <arpc.h>
#include <chord.h>
#include <chord_types.h>
#include <dhash_types.h>
#include <location.h>
#include <dhc_prot.h>
#include <modlogger.h>

#define ID_size sha1::hashsize
#define dhc_trace modlogger ("dhct")

extern int dhc_debug;
extern int RECON_TM;
extern char *host;

struct dhash_block;

extern void set_locations (vec<ptr<location> > *, ptr<vnode>, vec<chordID>);
extern void ID_put (char *, chordID);
extern void ID_get (chordID *, char *);
extern int tag_cmp (tag_t, tag_t);

// PK blocks data structure for maintaining consistency.

struct replica_t {
  u_int64_t seqnum;
  vec<chordID> nodes;
  char *buf;
  
  replica_t () : seqnum (0), buf (NULL) { };

  replica_t (char *bytes) : seqnum (0), buf (NULL)
  {
    uint offst = sizeof (uint);
    bcopy (bytes + offst, &seqnum, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    uint nreplica; 
    bcopy (bytes + offst, &nreplica, sizeof (uint));
    offst += sizeof (uint);
    chordID id;
    for (uint i=0; i<nreplica; i++) {
      //warnx << "replica_t node[" << i << "]: ";
      ID_get (&id, bytes + offst);
      nodes.push_back (id);
      offst += ID_size;
      //warnx << nodes[i] << "\n";
    }
  }

  ~replica_t () { if (buf) free (buf); nodes.clear (); }

  uint size ()
  {
    return (sizeof (uint) + sizeof (u_int64_t) + sizeof (uint) + 
	    nodes.size () * ID_size);
  }
  
  char *bytes ()
  {
    if (buf) free (buf);
    uint offst = 0;
    uint sz = size ();
    buf = (char *) malloc (sz);
    bcopy (&sz, buf, sizeof (uint));
    offst += sizeof (uint);
    bcopy (&seqnum, buf + offst, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    uint nreplica = nodes.size ();
    bcopy (&nreplica, buf + offst, sizeof (uint));
    offst += sizeof (uint);
    for (uint i=0; i<nreplica; i++) {
      ID_put (buf + offst, nodes[i]);
      offst += ID_size;
    }
	     
    return buf;
  }
};

struct keyhash_meta {
  replica_t config;
  paxos_seqnum_t accepted;
  char *buf;
  
  keyhash_meta () : buf (NULL)
  {
    accepted.seqnum = 0;
    bzero (&accepted.proposer, sizeof (accepted.proposer));
  }

  keyhash_meta (char *bytes) : buf (NULL)
  {
    uint offst = sizeof (uint);
    uint csize;
    bcopy (bytes + offst, &csize, sizeof (uint));
    config = replica_t (bytes + offst);
    offst += csize;
    bcopy (bytes + offst, &accepted.seqnum, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    ID_get (&accepted.proposer, bytes + offst);
  }

  ~keyhash_meta () { if (buf) free (buf); }

  uint size ()
  {
    return (sizeof (uint) + config.size ()
	    + sizeof (u_int64_t) + ID_size);
  }

  char *bytes ()
  {
    if (buf) free (buf);
    
    uint offst = 0;
    uint sz = size ();
    buf = (char *) malloc (sz);

    bcopy (&sz, buf, sizeof (uint));
    offst += sizeof (uint);
    bcopy (config.bytes (), buf + offst, config.size ());
    offst += config.size ();
    bcopy (&accepted.seqnum, buf + offst, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    ID_put (buf + offst, accepted.proposer);

    return buf;
  }

  str to_str ()
  {
    strbuf ret;
    ret << "\n     config seqnum: " << config.seqnum 
	<< "\n     config IDs: ";
    for (uint i=0; i<config.nodes.size (); i++)
      ret << config.nodes[i] << " ";
    ret << "\n     accepted proposal number: " 
	<< "<" << accepted.seqnum << "," << accepted.proposer << ">";

    return str (ret);
  }

};

struct dhc_block {
  chordID id;
  chordID masterID;   //ID of object that decides which node is the recon leader.
  keyhash_meta *meta;
  keyhash_data *data;
  char *buf;

  dhc_block (chordID ID, chordID mID) : id (ID), masterID (mID), buf (NULL)
  {
    meta = New keyhash_meta;
    data = New keyhash_data;
  }

  dhc_block (char *bytes, int sz) : buf (NULL)
  {
    //warnx << "dhc_block: " << sz << "\n";
    uint msize, offst = 0;
    ID_get (&id, bytes);
    offst += ID_size;
    ID_get (&masterID, bytes);

    offst += ID_size;
    bcopy (bytes + offst, &msize, sizeof (uint));
    //warnx << "dhc_block: msize " << msize << "\n";
    //warnx << "dhc_block: create meta at offst: " << offst << "\n";
    meta = New keyhash_meta (bytes + offst);
    offst += msize;
    data = New keyhash_data;
    bcopy (bytes + offst, &data->tag.ver, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    ID_get (&data->tag.writer, bytes + offst);
    offst += ID_size;
    int data_size = sz - offst;
    if (data_size >= 0)
      data->data.set (bytes + offst, data_size);
    else {
      warn << "dhc_block: Fatal exceeded end of pointer!!!\n";
      exit (-1);
    }
  }

  ~dhc_block () 
  {
    if (buf) free (buf);
    delete meta;
    data->data.clear ();
    delete data;
  }

  uint size ()
  {
    return (ID_size + ID_size + meta->size () + sizeof (u_int64_t) + 
	    ID_size + data->data.size ());
  }
  
  char *bytes ()
  {
    if (buf) free (buf);

    buf = (char *) malloc (size ());

    uint offst = 0;
    ID_put (buf, id);
    offst += ID_size;
    ID_put (buf, masterID);
    offst += ID_size;

    bcopy (meta->bytes (), buf + offst, meta->size ());
    offst += meta->size ();
    bcopy (&data->tag.ver, buf + offst, sizeof (u_int64_t));
    offst += sizeof (u_int64_t);
    ID_put (buf + offst, data->tag.writer);
    offst += ID_size;
    bcopy (data->data.base (), buf + offst, data->data.size ());

    return buf;
  }

  str to_str ()
  {
    strbuf ret;
    ret << "\n*************** DHC block *****************\n" 
	<< "\n id: " << id
	<< "\n master ID " << masterID 
	<< "\n meta data: " << meta->to_str ()
	<< "\n data tag ver " << data->tag.ver 
	<< "\n data tag writer " << data->tag.writer
	<< "\n data size: " << data->data.size ()
      //<< "\n data data: " << str (data->data.base (), data->data.size ())
	<< "\n";
    return str(ret);
  }
};

struct paxos_state_t {
  bool proposed;
  bool sent_newconfig;
  bool sent_reply;
  uint promise_recvd;
  uint accept_recvd;
  uint newconfig_ack_recvd;
  vec<chordID> acc_conf;
  
  paxos_state_t () : proposed(false), sent_newconfig(false), sent_reply(false), 
    promise_recvd(0), accept_recvd(0), newconfig_ack_recvd (0) {}
  
  ~paxos_state_t () { acc_conf.clear (); }

  void init () 
  {
    proposed = false;
    sent_newconfig = false;
    sent_reply = false;
    promise_recvd = 0;
    accept_recvd = 0;
    newconfig_ack_recvd = 0;
  }

  str to_str ()
  {
    strbuf ret;
    ret << "\n proposed: " << proposed
	<< "\n sent_newconfig: " << sent_newconfig
	<< "\n promise msgs received: " << promise_recvd 
	<< "\n accept msgs received: " << accept_recvd
	<< "\n newconfig acks received: " << newconfig_ack_recvd
	<< "\n accepted config: ";
    for (uint i=0; i<acc_conf.size (); i++)
      ret << acc_conf[i] << " ";
    return str (ret);
  }
};

enum stat_t {
  IDLE = 0,
  RECON_INPROG = 1
};

struct dhc_soft {
  chordID id;
  stat_t status;
  u_int64_t config_seqnum;
  vec<ptr<location> > config;
  vec<ptr<location> > new_config;   //next accepted config. used during recon
  paxos_seqnum_t proposal;
  paxos_seqnum_t promised;

  ptr<paxos_state_t> pstat;
  
  ihash_entry <dhc_soft> link;

  dhc_soft (ptr<vnode> myNode, ptr<dhc_block> kb)
  {
    id = kb->id;
    status = IDLE;
    config_seqnum = kb->meta->config.seqnum;
    set_locations (&config, myNode, kb->meta->config.nodes);
    proposal.seqnum = 0;
    bzero (&proposal.proposer, sizeof (chordID));    
    promised.seqnum = kb->meta->accepted.seqnum;
    promised.proposer = kb->meta->accepted.proposer;

    
    pstat = New refcounted<paxos_state_t>;
  }
  
  ~dhc_soft () 
  {
    config.clear ();
    new_config.clear ();
  }

  str to_str () 
  {
    strbuf ret;
    ret << "\n************ dhc_soft stat *************\n";
    ret << "Block ID:" << id
	<< "\n status: " << status
	<< "\n config seqnum: " << config_seqnum 
	<< "\n config IDs: ";
    for (uint i=0; i<config.size (); i++) 
      ret << config[i]->id () << " ";
    ret << "\n new config IDs: ";
    for (uint i=0; i<new_config.size (); i++)
      ret << new_config[i]->id () << " ";
    ret << "\n proposal number: <" << proposal.seqnum << "," << proposal.proposer << ">"
	<< "\n promised number: <" << promised.seqnum << "," << promised.proposer << ">";
    ret << pstat->to_str () << "\n";
    
    return str (ret);
  }
};

struct read_state {
  bool done;
  uint count;
  vec<keyhash_data> blocks;
  vec<uint> bcount;

  read_state () : done (false), count (0) {}

  ~read_state () 
  {
    blocks.clear ();
    bcount.clear ();
  }

  void add () { count++; }

  void add (keyhash_data kd)
  {
    bool found = false;
    for (uint i=0; i<blocks.size (); i++)
      if (tag_cmp (blocks[i].tag, kd.tag) == 0 &&
	  bcmp (blocks[i].data.base (), kd.data.base (), kd.data.size ()) == 0) {
	found = true;
	bcount[i]++;
	break;
      }
    if (!found) {
      keyhash_data d;
      d.tag.ver = kd.tag.ver;
      d.tag.writer = kd.tag.writer;
      d.data.setsize (kd.data.size ());
      memmove (d.data.base (), kd.data.base (), kd.data.size ());
      blocks.push_back (d);
      bcount.push_back (1);
    }
  }

};

struct write_state {
  bool done;
  uint bcount;
  user_args *sbp;
  u_int64_t start;
  ptr<keyhash_data> new_data;
  
  write_state (user_args *s) : done (false), bcount (0), sbp (s) 
  {
    new_data = New refcounted<keyhash_data>;
  }
};

typedef callback<void, dhc_stat, clnt_stat>::ref dhc_cb_t;
typedef callback<void, ptr<dhash_block> >::ref dhc_getcb_t;

struct put_args {
  chordID bID;
  chordID writer;
  ptr<dhash_value> value;
   
  put_args (chordID b, chordID w, ref<dhash_value> v) : 
    bID (b), writer (w)
  {
    value = New refcounted<dhash_value>;
    value->set (v->base (), v->size ());
  }
};

class dhc /*: public virtual refcount*/ {
  
  ptr<vnode> myNode;
  ptr<dbfe> db;
  ihash<chordID, dhc_soft, &dhc_soft::id, &dhc_soft::link, hashID> dhcs;

  uint n_replica;
  uint recon_tm_rpcs;
  timecb_t *recon_tm;
  u_int64_t start_recon, end_recon;

  void recon_timer ();
  void recon_tm_lookup (ref<dhc_block>, bool, vec<chord_node>, route, chordstat);
  void recon_tm_done (dhc_stat, clnt_stat);
  void recon (chordID, dhc_cb_t);
  void master_recon (ptr<dhc_block>, dhc_cb_t);
  void ask_master (ptr<dhc_block>, dhc_cb_t);

  void recv_prepare (user_args *);
  void recv_promise (chordID, dhc_cb_t, ref<dhc_prepare_res>, clnt_stat);
  void recv_propose (user_args *);
  void recv_accept (chordID, dhc_cb_t, ref<dhc_propose_res>, clnt_stat);
  void recv_newconfig (user_args *);
  void recv_newconfig_ack (chordID, dhc_cb_t, ref<dhc_newconfig_res>, clnt_stat);

  void recv_permission (chordID, dhc_cb_t, ref<dhc_prepare_res>, clnt_stat);

  void recv_get (user_args *);
  void getblock_cb (user_args *, ptr<location>, ptr<read_state>, 
		    ref<dhc_get_res>, clnt_stat);
  void getblock_retry_cb (user_args *, ptr<location>, ptr<read_state>);
  void recv_getblock (user_args *);

  void recv_put (user_args *);
  void recv_putblock (user_args *);
  void putblock_cb (ptr<location>, ptr<write_state>, 
		    ref<dhc_put_res>, clnt_stat);
  void putblock_retry_cb (ptr<location>, ptr<write_state>);

  void recv_newblock (user_args *);
  void recv_newblock_ack (user_args *, ptr<uint>, 
			  ref<dhc_newconfig_res>, clnt_stat);
 public:

  dhc (ptr<vnode>, str, uint);
  ~dhc () {};
  
  void init ();
  void dispatch (user_args *);
  
};

#endif /*_DHC_H_*/















