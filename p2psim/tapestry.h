/* $Id: tapestry.h,v 1.1 2003/07/18 06:25:20 strib Exp $ */

#ifndef __TAPESTRY_H
#define __TAPESTRY_H

#include "dhtprotocol.h"
#include "consistenthash.h"
#include "p2psim.h"
#include <math.h>
#include <stdio.h>
#include <vector>

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


  Tapestry(Node *n);
  virtual ~Tapestry();
  string proto_name() { return "Tapestry"; }

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void lookup(Args*);
  virtual void insert(Args*);

  // print it to stdout
  void print_guid( GUID id );
  // print it in the stream
  void print_guid( GUID id, ostream &s );
  uint get_digit( GUID id, uint digit );
  GUID id() { return _my_id; };
  IPAddress ip() { return DHTProtocol::ip(); };

  struct join_args {
    IPAddress ip;
    GUID id;
  };

  struct join_return {
    int dummy;
  };

  void handle_join(join_args *args, join_return *ret);

private:

  GUID _my_id;

  // how else are we gonna route?
  RoutingTable *_rt;

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
  // how many digits do these keys share
  // returns -1 if they are the same
  int guid_compare( GUID key1, GUID key2 ); 

};

//////// NodeInfo ////////////////
// Just a collection of IP, GUID, distance, etc. per node
class NodeInfo {
 public:
  typedef Tapestry::GUID GUID;
  NodeInfo( IPAddress addr, GUID id, uint distance = 1000 ) {
    _id = id;
    _addr = addr;
    _distance = distance;
  };
  ~NodeInfo() {};
  
  GUID _id;
  IPAddress _addr;
  uint _distance;

  
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
  /**
   * Add a new node.  Indicate the node that's kicked out (if any).
   * Return true if the node was added to the entry
   */
  bool add( NodeInfo *new_node, NodeInfo **kicked_out );
  
 private:
  
  NodeInfo *_nodes[NODES_PER_ENTRY];
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
  vector<IPAddress> add( IPAddress ip, GUID id, uint distance, bool *added );
  /**
   * Read the primary neighbor at this position.
   */
  NodeInfo *read( uint i, uint j );

  ostream& insertor( ostream &s ) const;

 private:

  RouteEntry ***_table;
  Tapestry *_node;

};

// operator overloading

ostream& operator<< (ostream &s, RoutingTable const &rt);

bool operator== ( const NodeInfo & one, const NodeInfo & two );

#endif // __TAPESTRY_H

