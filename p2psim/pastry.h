#ifndef __PASTRY_H
#define __PASTRY_H

#include "protocol.h"
#include "node.h"
#include <vector>
using namespace std;

class Pastry : public Protocol {
public:
  const unsigned idlength;
  typedef long long NodeID;

  Pastry(Node*);
  ~Pastry();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert(Args*);
  virtual void lookup(Args*);

private:
  NodeID _id;
  const unsigned _b; // config. parameter b from paper, base of NodeID
  const unsigned _L; // config. parameter L from paper, 
  const unsigned _M; // config. parameter M from paper, size of neighborhood

  void route(NodeID*, void*);
  unsigned shared_prefix_len(NodeID, NodeID);
  unsigned get_digit(NodeID, unsigned);


  //
  // ROUTING TABLE
  //
  // routing table entry
  typedef pair<NodeID, IPAddress> RTEntry;

  // single row in a routing table
  typedef vector<RTEntry> RTRow;

  // whole routing table.  is array since
  vector<RTRow> _rtable;

  //
  // NEIGHBORHOOD SET
  //
  

  //
  // LEAF SET
  //
};

#endif // __PASTRY_H
