#ifndef __VIVALDITEST_H
#define __VIVALDITEST_H

#include "dhtprotocol.h"
#include "node.h"
#include "vivaldi.h"
#include <vector>

class VivaldiTest : public DHTProtocol {
public:
  VivaldiTest(Node*);
  ~VivaldiTest();
  string proto_name() { return "VivaldiTest"; }

  virtual void join(Args*);
  virtual void leave(Args*) { }
  virtual void crash(Args*) { }
  virtual void insert(Args*) { }
  virtual void lookup(Args*) { }

  void status();
  Vivaldi::Coord real();
  double error();
  void total_error(double &x05, double &x50, double &x95);
  int samples () { return _vivaldi->nsamples ();};
 private:
  Vivaldi *_vivaldi;
  static vector<VivaldiTest*> _all;

  int _next_neighbor;
  int _neighbors; // if > 0, fix the number of neighbors
  uint _old_all_size;
  vector<IPAddress> _nip; // our fixed neigbhors

  void tick(void *);
  char *ts();
  void handler(void *, Vivaldi::Coord *);

  void addNeighbors ();
  void print_all_loc();
};

#endif // __VIVALDITEST_H
