/* $Id: tapestry.h,v 1.42 2004/06/21 02:32:15 strib Exp $ */

#ifndef __TAPESTRY_H
#define __TAPESTRY_H

#include "chord.h"
#include "p2psim/p2psim.h"
#include "p2psim/condvar.h"
#include "p2psim/p2protocol.h"
#include "p2psim/bighashmap.hh"
#include <map>
using namespace std;

class NodeInfo;
class RouteEntry;
class RoutingTable;

class Tapestry : public P2Protocol {
  friend class RoutingTable;
public:

  typedef unsigned long long GUID;
  // The base of each digit in the id
  const unsigned _base;
  // how many bits are there for each digit? (must be log base 2 of _base)
  const unsigned _bits_per_digit;
  // how many digits of base _b are in each id?
  const unsigned _digits_per_id;

  // types of statistics we can record
  //  enum stat_type
  //{
  const static stat_type STAT_JOIN = 1;
  const static stat_type STAT_NODELIST = 2;
  const static stat_type STAT_MC = 3;
  const static stat_type STAT_PING = 4;
  const static stat_type STAT_BACKPOINTER = 5;
  const static stat_type STAT_MCNOTIFY = 6;
  const static stat_type STAT_NN = 7;
  const static stat_type STAT_REPAIR = 8;
  const static stat_type STAT_SIZE = 9;

  Tapestry(IPAddress i, Args a);
  virtual ~Tapestry();
  string proto_name() { return "Tapestry"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*);
  virtual void initstate();
  virtual void nodeevent (Args *) {};

  // print it to stdout
  string print_guid( GUID id );
  // print it in the stream
  bool is_joined() { return joined; };
  void print_guid( GUID id, ostream &s );
  uint get_digit( GUID id, uint digit );
  GUID id() { return _my_id; };
  uint* id_digits() { return _my_id_digits; };
  IPAddress ip() { return P2Protocol::ip(); };
  void add_to_rt( IPAddress new_ip, GUID new_id );
  // how many digits do these keys share
  // returns -1 if they are the same
  int guid_compare( GUID key1, GUID key2 ); 
  int guid_compare( GUID key1, uint *key2_digits ); 
  int guid_compare( uint *key1_digits, uint *key2_digits ); 

  bool stabilized(vector<GUID> lid);

  void oracle_node_died( IPAddress ip, GUID id, const set<Node*>* );
  void oracle_node_joined( Tapestry *t );

  template<class BT, class AT, class RT>
    bool Tapestry::retryRPC(IPAddress dst, void (BT::* fn)(AT *, RT *), 
			    AT *args, RT *ret, uint type, uint num_args_id = 0,
			    uint num_args_else = 0);

  void check_rt(void *x);

  struct check_node_args {
    IPAddress ip;
    GUID id;
  };

  void check_node_loop(void *args);

  struct join_args {
    IPAddress ip;
    GUID id;
  };

  struct join_return {
    GUID surr_id;
    bool failed;
  };

  void handle_join(join_args *args, join_return *ret);

  struct lookup_args {
    GUID key;
    IPAddress looker;
    // none of the below are counted in bw stats -- they are hacks
    IPAddress lasthop;
    Time lastrtt; // an estimate of the lastguy's RTT to you
    Time starttime; 
  };

  struct lookup_return {
    IPAddress owner_ip;
    GUID owner_id;
    bool failed;
    int hopcount;
    GUID real_owner_id;
    Time time_done;
    uint num_timeouts;
    Time time_timeouts;
  };

  void handle_lookup(lookup_args *args, lookup_return *ret);
  void handle_lookup_done(lookup_args *args, lookup_return *ret);

  struct wrap_lookup_args {
    GUID key;
    Time starttime;
    uint num_tries;
    uint num_timeouts;
    Time time_timeouts;
    uint hopcount;
  };

  void lookup_wrapper(wrap_lookup_args *args);

  struct nodelist_args {
    vector<NodeInfo *> nodelist;
  };

  struct nodelist_return {
    int dummy;
  };

  void handle_nodelist(nodelist_args *args, nodelist_return *ret);

  struct mc_args {
    IPAddress new_ip;
    GUID new_id;
    uint alpha;
    vector<bool *> watchlist;
    bool from_lock;
  };

  struct mc_return {
    int dummy;
  };

  void handle_mc(mc_args *args, mc_return *ret);

  struct ping_args {
    int dummy;
  };

  struct ping_return {
    int dummy;
  };

  void handle_ping(ping_args *args, ping_return *ret);

  struct backpointer_args {
    IPAddress ip;
    GUID id;
    int level;
    bool remove;
  };

  struct backpointer_return {
    int dummy;
  };

  void handle_backpointer(backpointer_args *args, backpointer_return *ret);
  void got_backpointer(IPAddress bpip, GUID bpid, uint level, bool remove);
  void place_backpointer( RPCSet *bp_rpcset, 
			  HashMap<unsigned,backpointer_args*> *bp_resultmap, 
			  IPAddress bpip, 
			  int level, bool remove );
  void place_backpointer_end( RPCSet *bp_rpcset, 
			      HashMap<unsigned,backpointer_args*> *bp_resultmap);

  struct mcnotify_args {
    IPAddress ip;
    GUID id;
    vector<NodeInfo *> nodelist;
  };

  struct mcnotify_return {
    int dummy;
  };

  void handle_mcnotify(mcnotify_args *args, mcnotify_return *ret);

  struct nn_args {
    IPAddress ip;
    GUID id;
    int alpha;
  };

  struct nn_return {
    vector<NodeInfo *> nodelist;
  };

  void handle_nn(nn_args *args, nn_return *ret);

  struct repair_args {
    vector<GUID> *bad_ids;
    vector<uint> *levels;
    vector<uint> *digits;
  };

  struct repair_return {
    vector<NodeInfo> nodelist;
  };

  void handle_repair(repair_args *args, repair_return *ret);

  void init_state( const set<Node*>* );

private:

#define TapDEBUG(x) DEBUG(x) << now() << ": (" << ip() << "/" << print_guid(id()) << ") "
  
  static const Time MAXTIME = 1000;

  // stabilization timer
  uint _stabtimer;

  // how many nodes to lookup at a time (at most)
  uint _redundant_lookup_num;
  uint _redundancy;

  GUID _my_id;
  uint *_my_id_digits;

  // threads waiting for join to finish
  ConditionVar *_waiting_for_join;

  // have we finished our join yet?
  bool joined;

  // monitor how many backups for failures
  uint _repair_backups;

  // how many times have we tried to join?
  uint _join_num;

  // how else are we gonna route?
  RoutingTable *_rt;

  // used during join to keep track of the next nodes to ping
  // during nearest neighbor
  vector<NodeInfo *> initlist;

  // how many nearest neighbors do we keep at every step?
  static const uint _k = 16;

  // when's the last time I heard from this person?
  map<IPAddress, Time> _last_heard_map;

  // guids to find replacements for in the next stabilization round
  vector<GUID> _recently_dead;
  bool _lookup_learn;

  // with recursive routing, we probably want to reply directly to the source
  bool _direct_reply;

  // bother looking up global correctness
  bool _lookup_cheat;

  // should we check the liveness of backpointers?
  bool _check_backpointers;

  // should we send back all the backpointers on a handle_nn, or
  // just a random _k?
  bool _nn_random;

  // statistics per message
  vector<uint> stat;
  vector<uint> num_msgs;

  // timeout on lookups
  Time _max_lookup_time;

  // timeout on declaring nodes dead
  Time _declare_dead_time;
  uint _declare_dead_num;
  // factor above rtt for timeouts
  uint _rtt_timeout_factor;

  // nodes that we learned about from lookups
  RoutingTable *_cachebag;

  // how many people do we ask about for each node?
  uint _max_repair_num;

  // nodes to check (in check_node_loop)
  vector<check_node_args *> *_check_nodes;
  ConditionVar *_check_nodes_waiting;

  // overall stats
  static unsigned long long _num_lookups;
  static unsigned long long _num_succ_lookups;
  static unsigned long long _num_inc_lookups;
  static unsigned long long _num_fail_lookups;
  static unsigned long long _num_hops;
  static unsigned long long _total_latency;
  bool _verbose;


  bool _joining;
  bool _stab_scheduled;

  /**
   * Convert a given IP address to an id in the Tapestry namespace
   */
  GUID get_id_from_ip( IPAddress addr ) {
    // we can get more creative later if need be . . .
    return ConsistentHash::ip2chid( addr );
  }

  // finds the next hop toward the given key
  // returns ip() if we are the root
  IPAddress next_hop( GUID key );
  void next_hop( GUID key, IPAddress** ips, uint size );
  void next_hop( GUID key, IPAddress** ips, GUID** ids, uint size );
  Time ping( IPAddress other_node, GUID other_id, bool &ok );
  GUID lookup_cheat( GUID key );
  void record_stat( stat_type type, uint num_ids, uint num_else );
  void print_stats();

  class mc_callinfo { public:
    mc_callinfo(IPAddress xip, mc_args *mca, mc_return *mcr)
      : ip(xip), ma(mca), mr(mcr) {}
    ~mc_callinfo() { delete ma; delete mr; }
    IPAddress ip;
    mc_args *ma;
    mc_return *mr;
  };

  class ping_callinfo { public:
    ping_callinfo(IPAddress xip, GUID xid, Time xps)
      : ip(xip), id(xid), pingstart(xps), rtt(87654), failed(false), 
      times_tried(0) {}
    ~ping_callinfo() {}
    IPAddress ip;
    GUID id;
    Time pingstart;
    Time rtt;
    bool failed;
    Time last_timeout;
    uint times_tried;
  };

  class nn_callinfo { public:
    nn_callinfo(IPAddress xip, nn_args *nna, nn_return *nnr)
      : ip(xip), na(nna), nr(nnr) {}
    ~nn_callinfo() { delete na; /*delete nr;*/ }
    IPAddress ip;
    nn_args *na;
    nn_return *nr;
  };

  class repair_callinfo { public:
    repair_callinfo(repair_args *rra, repair_return *rrr)
      : ra(rra), rr(rrr) {}
    ~repair_callinfo() { delete ra; }
    repair_args *ra;
    repair_return *rr;
  };

  void multi_add_to_rt(	vector<NodeInfo *> *nodes, 
			map<IPAddress, Time> *timing );

  void multi_add_to_rt_start( RPCSet *ping_rpcset, 
			      HashMap<unsigned, ping_callinfo*> *ping_resultmap,
			      vector<NodeInfo *> *nodes, 
			      map<IPAddress, Time> *timing, 
			      bool check_exists );

  void multi_add_to_rt_end( RPCSet *ping_rpcset,
			    HashMap<unsigned, ping_callinfo*> *ping_resultmap,
			    Time before_ping, map<IPAddress, Time> *timing,
			    bool repair );
  void have_joined();

};

//////// NodeInfo ////////////////
// Just a collection of IP, GUID, distance, etc. per node
class NodeInfo {
 public:
  typedef Tapestry::GUID GUID;
  NodeInfo( IPAddress addr, GUID id, Time distance = 1000, 
	    bool timeout = false ) {
    _id = id;
    _addr = addr;
    _distance = distance;
    _timeout = timeout;
  };
  ~NodeInfo() {};
  
  GUID _id;
  IPAddress _addr;
  Time _distance;
  bool _timeout;

};

//////// Routing Table Entry ///////////////
class RouteEntry {
  
 public:
  RouteEntry( uint redundancy );
  RouteEntry( NodeInfo *first_node, uint redundancy );
  ~RouteEntry();
  
  /**
   * Return the primary (closest) node
   */
  NodeInfo *get_first();
  /**
   * Get the node at a specific position
   */
  NodeInfo *get_at( uint pos );
  /**
   * How many nodes is it storing?
   */
  uint size();
  
 private:

  friend class RoutingTable;

  // How many nodes can we keep in each entry (c in the JSAC paper)
  // must be at least 1
  uint NODES_PER_ENTRY;

  /**
   * Add a new node.  Indicate the node that's kicked out (if any).
   * Return true if the node was added to the entry
   */
  bool add( NodeInfo *new_node, NodeInfo **kicked_out );
  void remove_at( uint pos );
  
  NodeInfo **_nodes;
  uint _size;
  
};

//////// Routing Table ///////////////
class RoutingTable {

 public:
  typedef Tapestry::GUID GUID;
  RoutingTable( Tapestry *node, uint redundancy );
  ~RoutingTable();
  
  /**
   * Add a new node to the table.  Return all the nodes that were kicked out
   * as a result.
   */
  bool add( IPAddress ip, GUID id, Time distance );
  bool add( IPAddress ip, GUID id, Time distance, bool sendbp );
  void remove( GUID id );
  void remove( GUID id, bool sendbp );
  /**
   * Read the primary neighbor at this position.
   */
  NodeInfo *read( uint i, uint j );
  RouteEntry *get_entry( uint i, uint j );

  bool contains( GUID id );
  Time get_time( GUID id );
  void set_timeout( GUID id, bool timeout );
  bool get_timeout( GUID id );

  ostream& insertor( ostream &s ) const;

  void add_backpointer( IPAddress ip, GUID id, uint level );
  void remove_backpointer( IPAddress ip, GUID id );
  void remove_backpointer( IPAddress ip, GUID id, uint level );
  vector<NodeInfo *> *get_backpointers( uint level );

  void set_lock( IPAddress ip, GUID id );
  void remove_lock( IPAddress ip, GUID id );
  // get the locked nodes that are associated with this node's id
  vector<NodeInfo *> *get_locks( GUID id );

  uint redundancy() { return _redundancy; };

 private:

#define TapRTDEBUG(x) DEBUG(x) << now() << ": (" << _node->ip() << "/" << _node->print_guid(_node->id()) << ") "

  static const Time MAXTIME = 1000;

  RouteEntry ***_table;
  Tapestry *_node;
  vector<NodeInfo *> **_backpointers;
  vector<NodeInfo *> ***_locks;
  uint _redundancy;

};

// operator overloading

ostream& operator<< (ostream &s, RoutingTable const &rt);

bool operator== ( const NodeInfo & one, const NodeInfo & two );

#endif // __TAPESTRY_H


