#ifndef __VIVALDI_H
#define __VIVALDI_H

#include <map>
#include "p2psim.h"
#include "protocol.h"
using namespace std;

// Compute Vivaldi synthetic coordinates.
// Protocol-independent: doesn't care where the measurements
// come from.
// Indexed by IPAddress.
// Anyone can create a Vivaldi, call sample() after each
// RPC, and then call my_location().
// Or you can use Vivaldi::RPC.

class Vivaldi {
 public:
  Vivaldi(IPAddress me);
  virtual ~Vivaldi();

  // Same as Euclidean::Coord
  // But unsigned may be bad for us...
  typedef pair<unsigned,unsigned> Coord;

  void sample(IPAddress who, Coord c, unsigned latency);
  Coord my_location();

  // Anyone can use this to make an RPC and have Vivaldi time it.
  template<class BT, class AT, class RT>
    bool RPC(IPAddress a, void (BT::* fn)(AT *, RT *),
             AT *args, RT *ret);

 private:
  IPAddress _me;
  double _x;
  double _y;

  typedef void (Threaded::* member_f)(void *, void *);

  struct rpc_args {
    member_f fn;
    void *args;
    void *ret;
  };
  struct rpc_ret {
    Vivaldi::Coord c;
  };
  void rpc_handler(rpc_args *, rpc_ret *);

  void run() { };
};

// Make an RPC call, but time it and tell Vivaldi.
// Basically wraps the RPC in an RPC to rpc_handler.
// Use this only for simple RPCs: don't use it for
// recursive RPCs.
template<class BT, class AT, class RT>
bool Vivaldi::RPC(IPAddress a,
                  void (BT::* fn)(AT *, RT *),
                  AT *args,
                  RT *ret)
{
  rpc_args ta;
  rpc_ret tr;
  ta.fn = (member_f) fn;
  ta.args = (void *) args;
  ta.ret = (void *) ret;
  //  Time before = now();
  bool ok = doRPC(a, &Vivaldi::rpc_handler, &ta, &tr);
  if(ok){
    //Time after = now();
    //sample(a, tr.c, (after - before) / 2);
    // Coord c = my_location();
    //    printf("%d vivaldi %d %d\n", ts(), _me, c.first, c.second);
  }
  return ok;
}

#endif
