/* $Id: tapestry.C,v 1.1 2003/07/18 06:25:20 strib Exp $ */

#include "tapestry.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
#include <math.h>

using namespace std;

Tapestry::Tapestry(Node *n)
  : DHTProtocol(n),
    _base(16),
    _bits_per_digit(log10(_base)/log10(2)),
    _digits_per_id((uint)8*sizeof(GUID)/_bits_per_digit)
{
  _my_id = get_id_from_ip(ip());
  cout << "Tapestry Constructor for " << ip() << " and id: ";
  print_guid(_my_id);
  cout << endl;
  _rt = new RoutingTable(this);
  cout << *_rt << endl;
}

Tapestry::~Tapestry()
{
  cout << "Tapestry Destructor" << endl;
  delete _rt;
}

void
Tapestry::lookup(Args *args) 
{
  cout << "Tapestry Lookup" << endl;
}

void
Tapestry::insert(Args *args) 
{
  cout << "Tapestry Insert" << endl;
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Tapestry::join(Args *args)
{
  cout << "Tapestry join" << endl;

  IPAddress wellknown_ip = args->nget<IPAddress>("wellknown");
  cout << ip() << " Wellknown: " << wellknown_ip << endl;

  // if we're the well known node, we're done
  if( ip() == wellknown_ip ) {
    return;
  }

  // contact the well known machine, and have it start routing to the surrogate
  join_args ja;
  ja.ip = ip();
  ja.id = id();
  join_return jr;

  bool ok = doRPC( wellknown_ip, &Tapestry::handle_join, &ja, &jr );

  cout << ip() << " join done: " << ok << endl;

}

void
Tapestry::handle_join(join_args *args, join_return *ret)
{
  cout << ip() << " got a join message from " << args->ip << "/";
  print_guid(args->id);
  cout << endl;

  // route toward the root
  IPAddress next = next_hop( args->id );
  if( next == ip() ) {
    // we are the surrogate root for this node, start the integration
    
  } else {
    // recursive routing yo
    doRPC( next, &Tapestry::handle_join, args, ret );
  }

}

IPAddress
Tapestry::next_hop( GUID key )
{
  // first, figure out how many digits we share, and start looking at 
  // that level
  int level = guid_compare( id(), key );
  if( level == -1 ) {
    // it's us!
    return ip();
  }

  NodeInfo *ni = _rt->read( level, get_digit( key, level ) );

  // if it has an entry, use it
  if( ni != NULL ) {
    // this cannot be us, since this should be the entry where we differ
    // from the key, but just to be sure . . .
     assert( ni->_id != id() );
     return ni->_addr;
  } else {
    // if there's no such entry, it's time to surrogate route.  yeah baby.
    // keep adding 1 to the digit until you find a match.  If it's us,
    // go up to the next level and keep on trying till the end
    // (From Fig. 3 in the JSAC paper)

    for( uint i = level; i < _digits_per_id; i++ ) {

      uint j = get_digit( key, i );
      while( ni == NULL ) {

	// NOTE: we can't start with looking at the j++ entry, since
	// this might be the next level up, and we'd want to start at j
	ni = _rt->read( i, j );

	j++;
	if( j == _base ) {
	  // think circularly
	  j = 0;
	}
      }

      // if it is us, go around another time
      // otherwise, we've found the next hop
      if( ni->_addr != ip() ) {
	return ni->_addr;
      }

    }
    
  }

  // didn't find a better surrogate, so it must be us
  return ip();

}

int
Tapestry::guid_compare( GUID key1, GUID key2 )
{

  // if they're the same, return -1
  if( key1 == key2 ) {
    return -1;
  }
  uint i = 0;
  // otherwise, figure out where they differ
  for( ; i < _digits_per_id; i++ ) {
    if( get_digit( key1, i ) != get_digit( key2, i ) ) {
      break;
    }
  }

  return (int) i;

}

void
Tapestry::leave(Args *args)
{
  crash (args);
}

void
Tapestry::crash(Args *args)
{
  cout << "Tapestry crash" << endl;
}

void
Tapestry::print_guid( GUID id )
{
  //printf( "initial guid: %16qx\n", id );
  // print it out, digit by digit
  // (in order to get leading zeros)
  for( uint i = 0; i < _digits_per_id; i++ ) {
    printf( "%x", get_digit(id, i) );
  }

}

void
Tapestry::print_guid( GUID id, ostream &s )
{
  for( uint i = 0; i < _digits_per_id; i++ ) {
    char digit_string[2];
    sprintf( digit_string, "%x", get_digit(id, i) );
    s << digit_string;
  }
}

uint
Tapestry::get_digit( GUID id, uint digit )
{

  // can't request a digit higher than what we have, right?
  // 0-based . . .
  assert( digit < _digits_per_id );

  // shift left to get rid of leading zeros
  GUID shifted_id = id << (digit*_bits_per_digit);
  // shift right to get rid of the others
  shifted_id = shifted_id >> ((_digits_per_id-1)*_bits_per_digit);

  return shifted_id;
}

//////////  RouteEntry  ///////////

RouteEntry::RouteEntry()
{
  _size = 0;
}

RouteEntry::RouteEntry( NodeInfo *first_node )
{
  assert( first_node );
  _nodes[0] = first_node;
  _size = 1;
}

RouteEntry::~RouteEntry()
{
  for( uint i = 0; i < _size; i++ ) {
    delete _nodes[i];
    _nodes[i] = NULL;
  }
}

NodeInfo *
RouteEntry::get_first()
{
  return _nodes[0];
}

NodeInfo *
RouteEntry::get_at( uint pos )
{
  assert( pos > 0 );
  assert( pos < NODES_PER_ENTRY );
  return _nodes[pos];
}

uint
RouteEntry::size()
{
  return _size;
}

bool
RouteEntry::add( NodeInfo *new_node, NodeInfo **kicked_out )
{
  
  assert( new_node );
  // if this node is already in the entry, just remove it so that it can
  // be resorted in with the new distance
  for( uint i = 0; i < _size; i++ ) {
    if( *new_node == *(_nodes[i]) ) {
      // if the distance is the same, don't do anything
      if( new_node->_distance != _nodes[i]->_distance ) {
	// positive if distance went up
	int diff = new_node->_distance - _nodes[i]->_distance;
	_nodes[i]->_distance = new_node->_distance;
	if( diff > 0 ) {
	  // distance went up, make sure it's still smaller than what 
	  // comes next
	  while( i+1 < _size && 
		 _nodes[i]->_distance > _nodes[i+1]->_distance ) {
	    NodeInfo * tmp = _nodes[i];
	    _nodes[i] = _nodes[i+1];
	    _nodes[i+1] = tmp;
	    i++;
	  }
	} else {
	  // distance went down, make sure it's still bigger than what 
	  // comes before
	  while( i-1 > 0 && 
		 _nodes[i]->_distance < _nodes[i-1]->_distance ) {
	    NodeInfo * tmp = _nodes[i];
	    _nodes[i] = _nodes[i-1];
	    _nodes[i-1] = tmp;
	    i--;
	  }
	}
      }
      // didn't actually add in the same node twice, so its false
      return false;
    }
  }

  // see if its closer than any other entry so far
  for( uint i = 0; i < _size; i++ ) {
    if( new_node->_distance < _nodes[i]->_distance ) {
      NodeInfo *replacement = new_node;
      // insert the new node and shift the others down
      for( uint j = i; j < _size; j++ ) {
	NodeInfo *tmp = _nodes[j];
	_nodes[j] = replacement;
	replacement = tmp;
      }
      // did we add a new one?
      if( _size < NODES_PER_ENTRY ) {
	_size++;
      }
      // this is the last one that got kicked out (may be NULL)
      *kicked_out = replacement;
      return true;
    }
  }

  // if it wasn't put in by now, and there's still space in the entry,
  // it must belong at the end
  if( _size < NODES_PER_ENTRY ) {
    _nodes[_size] = new_node;
    _size++;
    return true;
  }

  return false;
}

////////  Routing Table ////////////

RoutingTable::RoutingTable( Tapestry *node )
{
  cout << "Routing Table constructor" << endl;
  assert(node);
  _node = node;
  _table = new RouteEntry **[_node->_digits_per_id];
  // initialize all the rows
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    _table[i] = new RouteEntry *[_node->_base];
  }

  // now we add ourselves to the table
  bool a;
  add( _node->ip(), _node->id(), 0, &a );

}

RoutingTable::~RoutingTable()
{
  // delete all route entries
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    delete [] _table[i];
    _table[i] = NULL;
  }
  delete [] _table;
  
}

vector<IPAddress>
RoutingTable::add( IPAddress ip, GUID id, uint distance, bool* added )
{
  vector<IPAddress> kicked_out;
  bool in_added = false;

  // find the spots where it fits and add it
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    uint j = _node->get_digit( id, i );
    NodeInfo *new_node = new NodeInfo( ip, id, distance );
    RouteEntry *re = _table[i][j];
    if( re == NULL ) {
      // brand new entry, huzzah
      re = new RouteEntry( new_node );
      _table[i][j] = re;
      in_added = true;
    } else {
      // add it to an existing one
      NodeInfo *kicked_out_node = NULL;
      in_added = in_added | re->add( new_node, &kicked_out_node );
      if( kicked_out_node != NULL ) {
	kicked_out.push_back(kicked_out_node->_addr);
      }
    }
    // if the last digit wasn't a match, we're done
    if( j != _node->get_digit( _node->id(), i ) ) {
      break;
    }
  }
  
  *added = in_added;
  return kicked_out;
}

NodeInfo *
RoutingTable::read( uint i, uint j )
{
  RouteEntry *re = _table[i][j];
  if( re == NULL || re->get_first() == NULL ) {
    return NULL;
  } else {
    return re->get_first();
  }
}

ostream&
RoutingTable::insertor( ostream &s ) const
{
  s<< "RoutingTable for " << _node->ip() << "/";
  _node->print_guid( _node->id(), s );
  s << endl;

  // now print out each row until you hit an empty one
  for( uint i = 0; i < _node->_digits_per_id; i++ ) {
    s << i << ": ";
    uint count = 0;
    for( uint j = 0; j < _node->_base; j++ ) {
      RouteEntry *re = _table[i][j];
      if( re == NULL || re->get_first() == NULL ) {
	s << "--- ";
      } else {
	NodeInfo *ni = re->get_first();
	s << ni->_addr << "/";
	_node->print_guid( ni->_id, s );
	s << " ";
	count++;
      }
    }
    s << endl;
    // if we only found ourselves in this row, get out
    if( count <= 1 ) {
      break;
    }
  }

  return s;
}


///// operator overloading ///////////

ostream& 
operator<< (ostream &s, RoutingTable const &rt)
{
  return rt.insertor(s);
}


bool 
operator== ( const NodeInfo & one, const NodeInfo & two )
{
  return one._addr == two._addr && one._id == two._id;
} 
