/* $Id: tapestry.h,v 1.5 2003/10/09 23:49:08 strib Exp $ */

#ifndef __TAPESTRY_H
#define __TAPESTRY_H

#include "chord.h"
#include "p2psim/p2psim.h"
#include "p2psim/condvar.h"
#include <map.h>

class NodeInfo;
class RouteEntry;
class RoutingTable;

class Tapestry : public DHTProtocol {
public:

  typedef unsigned long long GUID;
  // The base of each digit in the id
  const unsigned _base;
  // how many bits are there for each digit? (must be log base 2 of _base)
  const unsigned _bits_per_digit;
  // how many digits of base _b are in each id?
  const unsigned _digits_per_id;


  Tapestry(Node *n, Args a);
  virtual ~Tapestry();
  string proto_name() { return "Tapestry"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*);

  // print it to stdout
  string print_guid( GUID id );
  // print it in the stream
  void print_guid( GUID id, ostream &s );
  uint get_digit( GUID id, uint digit );
  GUID id() { return _my_id; };
  IPAddress ip() { return DHTProtocol::ip(); };
  void add_to_rt( IPAddress new_ip, GUID new_id );
  // how many digits do these keys share
  // returns -1 if they are the same
  int guid_compare( GUID key1, GUID key2 ); 

  bool stabilized(vector<GUID> lid);

  void check_rt(void *x);

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
  };

  struct lookup_return {
    IPAddress owner_ip;
    GUID owner_id;
    bool failed;
    int hopcount;
  };

  void handle_lookup(lookup_args *args, lookup_return *ret);

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
			  map<unsigned,backpointer_args*> *bp_resultmap, 
			  IPAddress bpip, 
			  int level, bool remove );
  void place_backpointer_end( RPCSet *bp_rpcset, 
			      map<unsigned,backpointer_args*> *bp_resultmap);

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
    vector<NodeInfo> nodelist;
  };

  void handle_nn(nn_args *args, nn_return *ret);

  void init_state( list<Protocol*> );

private:

#define TapDEBUG(x) DEBUG(x) << now() << ": (" << ip() << "/" << print_guid(id()) << ") "
  
  // stabilization timer
  uint _stabtimer;

  // how many nodes to lookup at a time (at most)
  uint _redundant_lookup_num;

  GUID _my_id;

  // threads waiting for join to finish
  ConditionVar *_waiting_for_join;

  // have we finished our join yet?
  bool joined;

  // how else are we gonna route?
  RoutingTable *_rt;

  // used during join to keep track of the next nodes to ping
  // during nearest neighbor
  vector<NodeInfo *> initlist;

  // how many nearest neighbors do we keep at every step?
  static const uint _k = 16;

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
  Time ping( IPAddress other_node, GUID other_id, bool &ok );
  GUID lookup_cheat( GUID key );

  class mc_callinfo { public:
    mc_callinfo(IPAddress xip, mc_args *mca, mc_return *mcr)
      : ip(xip), ma(mca), mr(mcr) {}
    ~mc_callinfo() { delete ma; delete mr; }
    IPAddress ip;
    mc_args *ma;
    mc_return *mr;
  };

  class ping_callinfo { public:
    ping_callinfo(IPAddress xip, GUID xid)
      : ip(xip), id(xid), rtt(87654), failed(false) {}
    ~ping_callinfo() {}
    IPAddress ip;
    GUID id;
    Time rtt;
    bool failed;
  };

  class nn_callinfo { public:
    nn_callinfo(IPAddress xip, nn_args *nna, nn_return *nnr)
      : ip(xip), na(nna), nr(nnr) {}
    ~nn_callinfo() { delete na; /*delete nr;*/ }
    IPAddress ip;
    nn_args *na;
    nn_return *nr;
  };

  void multi_add_to_rt(	vector<NodeInfo *> *nodes );

  void multi_add_to_rt_start( RPCSet *ping_rpcset, 
			      map<unsigned, ping_callinfo*> *ping_resultmap,
			      vector<NodeInfo *> *nodes, bool check_exists );

  void multi_add_to_rt_end( RPCSet *ping_rpcset,
			    map<unsigned, ping_callinfo*> *ping_resultmap,
			    Time before_ping );
  void have_joined();

};

//////// NodeInfo ////////////////
// Just a collection of IP, GUID, distance, etc. per node
class NodeInfo {
 public:
  typedef Tapestry::GUID GUID;
  NodeInfo( IPAddress addr, GUID id, Time distance = 1000 ) {
    _id = id;
    _addr = addr;
    _distance = distance;
  };
  ~NodeInfo() {};
  
  GUID _id;
  IPAddress _addr;
  Time _distance;

};

//////// Routing Table Entry ///////////////
class RouteEntry {
  
 public:
  RouteEntry();
  RouteEntry( NodeInfo *first_node );
  ~RouteEntry();
  // How many nodes can we keep in each entry (c in the JSAC paper)
  // must be at least 1
  static const uint NODES_PER_ENTRY = 3;
  
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
  RoutingTable( Tapestry *node );
  ~RoutingTable();
  
  /**
   * Add a new node to the table.  Return all the nodes that were kicked out
   * as a result.
   */
  bool add( IPAddress ip, GUID id, Time distance );
  bool add( IPAddress ip, GUID id, Time distance, bool sendbp );
  void remove( GUID id );
  /**
   * Read the primary neighbor at this position.
   */
  NodeInfo *read( uint i, uint j );
  RouteEntry *get_entry( uint i, uint j );

  bool contains( GUID id );
  Time get_time( GUID id );

  ostream& insertor( ostream &s ) const;

  void add_backpointer( IPAddress ip, GUID id, uint level );
  void remove_backpointer( IPAddress ip, GUID id, uint level );
  vector<NodeInfo *> *get_backpointers( uint level );

  void set_lock( IPAddress ip, GUID id );
  void remove_lock( IPAddress ip, GUID id );
  // get the locked nodes that are associated with this node's id
  vector<NodeInfo *> *get_locks( GUID id );

 private:

#define TapRTDEBUG(x) DEBUG(x) << now() << ": (" << _node->ip() << "/" << _node->print_guid(_node->id()) << ") "

  static const Time MAXTIME = 1000;

  RouteEntry ***_table;
  Tapestry *_node;
  vector<NodeInfo *> **_backpointers;
  vector<NodeInfo *> ***_locks;

};

// operator overloading

ostream& operator<< (ostream &s, RoutingTable const &rt);

bool operator== ( const NodeInfo & one, const NodeInfo & two );

#endif // __TAPESTRY_H


