/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Robert Morris (rtm@csail.mit.edu).
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __PROTOCOL_H
#define __PROTOCOL_H

#include "eventqueue.h"
#include "rpchandle.h"
#include "observed.h"
#include "args.h"
#include "bighashmap.hh"
#include <assert.h>
#include <stdio.h>

// A Node is the superclass of
// The point is, for example, to help the Chord object on
// one node find the Chord on another node by calling
// getpeer(IPAddress). P2Node has the DHT-specific
// abstract methods.
class Node : public Observed {
public:
  Node(IPAddress);
  virtual ~Node();
  static void Node::parse(char*);
  virtual void initstate() {};
  virtual string proto_name() { return "Node";}

  IPAddress ip() { return _ip; }
  unsigned long long id() { return _id;}
  string header();
  IPAddress set_alive(bool a);
  bool alive () { return _alive; }
  static bool init_state() { return (_args.nget<unsigned>("initstate",0,10) != 0); }
  static bool collect_stat();
  uint total_outbytes() { return node_live_outbytes; }
  static void set_collect_stat_time(Time u) { _collect_stat_time = u;}
  void packet_handler(Packet *);
  static void Receive(void*);

  // the One Node that we're running and its arguments
  static string protocol() { return _protocol; }
  static Args args() { return _args; }
  static void set_args (Args a) {_args = a; }

  // statistic collection
  typedef uint stat_type;
  const static stat_type STAT_LOOKUP = 0;
  unsigned long get_out_bw_stat() { return node_live_outbytes;}
  void record_bw_stat(stat_type type, uint num_ids, uint num_else);
  static void record_inout_bw_stat(IPAddress src, IPAddress dst, uint num_ids, uint num_else);
  void record_in_bytes(uint b);
  void record_out_bytes(uint b); 
  static void record_lookup_stat(IPAddress src, IPAddress dst, Time interval, 
				 bool complete, bool correct, 
				 uint num_hops = 0, uint num_timeouts = 0, 
				 Time time_timeouts = 0);

  void record_join();
  void record_crash();
  static void print_stats();
  static void print_dist_stats(vector<double> v);

  void calculate_conncomp(void *);
  virtual void add_edge(int *matrix, int sz);

  int queue_delay () { return _queue_len; };
  void queue_delay (int q) { _queue_len = q; };

  // whether nodes should be replace when they die
  static bool _replace_on_death;

  IPAddress first_ip() { return _first_ip; }

protected:
  typedef set<unsigned> RPCSet;

  // stats
  uint _track_conncomp_timer;
  static vector<unsigned long> _bw_stats;
  static vector<uint> _bw_counts;
  static vector<Time> _correct_lookups;
  static vector<Time> _incorrect_lookups;
  static vector<Time> _failed_lookups;
  static vector<double> _correct_stretch;
  static vector<double> _incorrect_stretch;
  static vector<double> _failed_stretch;
  static vector<uint> _correct_hops;
  static vector<uint> _incorrect_hops;
  static vector<uint> _failed_hops;
  static vector<double> _num_timeouts;
  static vector<Time> _time_timeouts;
  static vector<uint> _num_joins;
  static vector<Time> _last_joins;
  static vector<Time> _time_sessions;
  static vector<double> _per_node_avg;
  static vector<double> _per_node_in;
  static vector<double> _per_node_out;
  static vector< vector<double> > _special_node_out;
  static vector< vector<double> > _special_node_in;
  uint _special;
  static uint totalin;
  static uint totalout;
  int _num_joins_pos;
  static void print_lookup_stat_helper( vector<Time> times, 
					vector<double> stretch,
					vector<uint> hops,
					bool timeouts = false );
  void check_num_joins_pos();
  int _queue_len;
  Time join_time;
  //uint node_live_bytes;
  uint node_live_outbytes;
  uint node_live_inbytes;
  Time node_last_inburstime;
  Time node_last_outburstime;
  uint node_lastburst_live_inbytes;
  uint node_lastburst_live_outbytes;
  static double maxinburstrate;
  static double maxoutburstrate;

  // find peer protocol of my sub-type on a distant node.
  Node *getpeer(IPAddress);

  // Why are we forbidding non-Nodes from using delaycb()?
  // Use of a template allows us to type-check the argument
  // to fn(), and to check fn() is a member
  // of the same sub-class of Node as the caller.
  template<class BT, class AT>
    void delaycb(int d, void (BT::*fn)(AT), AT args, BT *target=NULL) {
    // Compile-time check: does BT inherit from Node?
    //Node *dummy = (BT *) 0; dummy = dummy;

    class XEvent : public Event {
    public:
      XEvent() : Event( "XEvent" ) {};
      BT *_target;
      void (BT::*_fn)(AT);
      AT _args;
    private:
      void execute() {
        (_target->*_fn)(_args);
      };
    };

    XEvent *e = New XEvent;
    e->ts = now() + d;
    if (target)
      e->_target = dynamic_cast<BT*>(target);
    else
      e->_target = dynamic_cast<BT*>(this);
    assert(e->_target);
    e->_fn = fn;
    e->_args = args;

    EventQueue::Instance()->add_event(e);
  }


  // Send an RPC from a Node on one Node to a method
  // of the same Node sub-class with a different ip
  template<class BT, class AT, class RT> 
  bool doRPC(IPAddress dst, void (BT::* fn)(AT *, RT *), AT *args, RT *ret, Time timeout = 0)
  {
    assert(dst > 0);
    Thunk<BT, AT, RT> *t = _makeThunk(dst, dynamic_cast<BT*>(getpeer(dst)), fn, args, ret);
    bool ok = _doRPC(dst, Thunk<BT, AT, RT>::thunk, (void *) t, timeout);
    delete t;
    return ok;
  }

  // Same as doRPC, but this one is asynchronous
  template<class BT, class AT, class RT>
  unsigned asyncRPC(IPAddress dst,
      void (BT::* fn)(AT *, RT *), AT *args, RT *ret, Time timeout = 0, unsigned token = 0)
  {
    assert(dst);
    while(!token || _rpcmap[token])
      token = _token++;

    Thunk<BT, AT, RT> *t = _makeThunk(dst, dynamic_cast<BT*>(getpeer(dst)), fn, args, ret);
    RPCHandle *rpch = _doRPC_send(dst, Thunk<BT, AT, RT>::thunk, Thunk<BT, AT, RT>::killme, (void *) t, timeout);

    if(!rpch)
      return 0;

    _rpcmap.insert(token, rpch);
    return token;
  }

  // returns one of the RPCHandle's for which a reply has arrived. BLOCKING.
  unsigned rcvRPC(RPCSet*, bool&);

  IPAddress _ip;
  unsigned long long _id;
  bool _alive;

  HashMap<unsigned, RPCHandle*> _rpcmap;
  unsigned _token;
  void _deleteRPC(unsigned);


  //
  // RPC machinery
  //
  template<class BT, class AT, class RT>
  class Thunk {
  public:
    BT *_target;
    void (BT::*_fn)(AT *, RT *);
    AT *_args;
    RT *_ret;
    static void thunk(void *xa) {
      Thunk *t = (Thunk *) xa;
      (t->_target->*(t->_fn))(t->_args, t->_ret);
      t->_target->notifyObservers();
    }

    static void killme(void *xa) {
      delete (Thunk*) xa;
    }
  };

  // implements _doRPC
  friend class Vivaldi;
  bool _doRPC(IPAddress, void (*fn)(void *), void *args, Time timeout = 0);
  RPCHandle* _doRPC_send(IPAddress, void (*)(void *), void (*)(void*), void *, Time = 0);
  bool _doRPC_receive(RPCHandle*);

  // creates a Thunk object with the necessary croft for an RPC
  template<class BT, class AT, class RT>
  Node::Thunk<BT, AT, RT> *
  Node::_makeThunk(IPAddress dst, BT *target, void (BT::*fn)(AT*, RT*),
      AT *args, RT *ret)
  {
    // target is probably the result of a dynamic_cast<BT*>...
    assert(target);

    Thunk<BT, AT, RT>  *t = New Thunk<BT, AT, RT>;
    t->_target = target;
    t->_fn = fn;
    t->_args = args;
    t->_ret = ret;

    return t;
  }

  // The One Protocol and Its Arguments
  static string _protocol;
  static Args _args;
  static Time _collect_stat_time;
  static bool _collect_stat;

  IPAddress _first_ip;
  IPAddress _prev_ip;
};

#define ADEBUG(x) if(p2psim_verbose >= (x)) cout << header() 

#endif // __PROTOCOL_H
