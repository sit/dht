/*
 * Copyright (c) 2003 [Anjali Gupta]
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

#ifndef __ONEHOP_H
#define __ONEHOP_H

#include "p2psim/p2protocol.h"
#include "consistenthash.h"
#include "chord.h"
#include "p2psim/skiplist.h"
#include <iostream>
#include <stdio.h>
#include <assert.h>
//#include <observers/onehopobserver.h>
#include <p2psim/network.h>

#define ALIVE 1
#define DEAD 0

#define ONEHOP_USER_LOOKUP 0
#define ONEHOP_PING 1
#define ONEHOP_LEADER_STAB 2
#define ONEHOP_NOTIFY 3
#define ONEHOP_INFORMDEAD 4

typedef unsigned long long bw;

class OneHop;

class OneHopLocTable : public LocTable {
  protected:
    int _k;
    int _u;
    ConsistentHash::CHID slice_size;
  public:    
    typedef Chord::IDMap IDMap;
    ConsistentHash::CHID unit_size;
    typedef ConsistentHash::CHID CHID;
    OneHopLocTable(uint k, uint u) : LocTable() {
    _k = k; //number of slices
    _u = u; //number of units per slice
    //slice_size = 0xffffffffffffffff / _k; 
    slice_size = ((ConsistentHash::CHID)-1)/ _k; 
    unit_size = slice_size / _u;
  };
  ~OneHopLocTable() {};
  CHID print_slice_size () {return slice_size;}
  IDMap slice_leader(CHID);
  IDMap unit_leader(CHID);
  vector<IDMap> unit_leaders(CHID);
  CHID slice (CHID node) {return node/slice_size;}  
  CHID unit (CHID node) {return (node % slice_size)/unit_size;}    
  bool is_slice_leader(CHID node, CHID exp_leader);
  bool is_unit_leader(CHID node, CHID exp_leader);
  bool is_empty (CHID node);
  bool is_empty_unit (CHID node);
  void print();
  IDMap succ (CHID id);
  IDMap pred (CHID id);
};



class OneHop : public P2Protocol {
public:
  static long lookups;
  static long failed;
  static long num_nodes;
  static long joins;
  static long crashes;
  static long same_lookups;
  static long same_failed;
  static long old_failed;
  static long two_failed;
  static long old_two_failed;
  static long old_lookups;
  static Time tot_interslice;
  static Time act_interslice;
  static Time tot_intraslice;
  static Time act_intraslice;
  static long nonempty_outers;
  static long nonempty_leaders;
  static Time total_empty;
  static Time total_count;
  static Time exp_intraslice;
  static bw bandwidth;
  static bw leader_bandwidth;
  static bw messages;
  static bw leader_messages;
  static bw old_bandwidth;
  static bw old_leader_bandwidth;
  static bw old_lookup_bandwidth;
  static bw lookup_bandwidth;
  static unsigned start;
  static Time old_time;
  static int _publish_time;
  
  typedef Chord::IDMap IDMap;
  typedef ConsistentHash::CHID CHID;
  typedef LocTable::idmapwrap idmapwrap;
  typedef LocTable::idmapcompare idmapcompare;
  int _k;
  int _u;
  
    int retries;
  class LogEntry {
    public:
    IDMap _node;
    int _state; //dead or alive?
    Time _tstamp; //loose synchronization
    LogEntry (IDMap node, int state, Time tstamp) {
      _node = node; _state = state; _tstamp = tstamp;
    }
  };
  
  class EventLog {
    vector<LogEntry> events;
    CHID generator_id;
    string log_id;
  };

  struct join_leader_args {
    CHID key;
    IPAddress ip;
  };

  struct general_ret {
    bool has_joined;
    bool correct;
    IDMap act_sliceleader;
    IDMap act_neighbor;
  };
  class join_leader_ret {
    public:
    bool is_slice_leader;
    bool is_join_complete;
    IDMap leader;
    bool exists;
    //skiplist<idmapwrap, ConsistentHash::CHID, &idmapwrap::id, &idmapwrap::sortlink_, idmapcompare> ring;
    vector<IDMap> table;
    join_leader_ret(int _k, int _u) {
      exists = false;
      is_slice_leader = false;
    }
  };

  typedef join_leader_args inform_dead_args;
  
  struct notifyevent_args {
    vector<LogEntry> log;
    IDMap sender;
    unsigned int up;
  };

  struct lookup_args {
    CHID key;
    IDMap sender;
  };

  struct lookup_ret {
    bool is_owner;
    IDMap correct_owner;
  };

  OneHop(IPAddress i, Args& a);
  ~OneHop();
  string proto_name() { return "OneHop";}
  Chord::IDMap idmap() { return me;}
  void record_stat(uint type, uint num_ids = 0, uint num_else = 0);

  // Functions callable from events file.
  virtual void join(Args*);
  virtual void leave(Args *args) { crash(args);};
  virtual void crash(Args*); 
  virtual void lookup(Args*); 
  virtual void insert(Args*) {};
  virtual void nodeevent (Args *) {};

  bool check_correctness(CHID k, IDMap n);
  void join_leader (IDMap la, IDMap sender, Args *args);
  void join_handler (join_leader_args *args, join_leader_ret *ret);
  void initstate ();
  //stabilize is called when a log message is received or after
  //period_keepalive which comes first
  void stabilize (void *x);
  void leader_stabilize (void *x);
  int num_nbrs;
  int* nbr_log_ptrs;
  vector<EventLog> log;  
  Time countertime;

  CHID id () { return ConsistentHash::ip2chid(ip());}
  IDMap me;

  bool is_slice_leader(CHID, CHID);
  bool is_unit_leader(CHID, CHID);
  Chord::IDMap OneHop::slice_leader(CHID node);
  CHID slice (CHID node);
  Chord::IDMap OneHop::unit_leader(CHID node);
  CHID unit (CHID node);

  bool inform_dead (IDMap dead, IDMap recv);
  void inform_dead_handler (inform_dead_args *ia, void *ir);

  template<class AT, class RT>
    bool xRPC(IPAddress dst, bw data, void (OneHop::* fn)(AT *, RT *),
              AT *args, RT *ret ){
      
    bool ok = doRPC(dst, fn, args, ret, TIMEOUT(me.ip,dst));
    if (!alive()) return ok;
    if (is_slice_leader(me.id, me.id)) {
      leader_bandwidth += 20 + data;
      leader_messages++;
    }
    else { 
      bandwidth += 20 + data;
      messages++;
    }
    
    return ok;
  };


  
    /*
  void reschedule_stabilizer(void *x);
  void stabilize();
  void notifyleaders(vector<IDMap> leaders, vector<deadalive_event> es);
*/
  //RPC handlers
  void ping_handler(notifyevent_args *args, general_ret *ret);
  void notifyevent_handler(notifyevent_args *args, general_ret *ret);
  void notify_rec_handler(notifyevent_args *args, general_ret *ret);
  void notify_other_leaders(notifyevent_args *args, general_ret *ret);
  void notify_unit_leaders(notifyevent_args *args, general_ret *ret);
  void lookup_handler(lookup_args *a, lookup_ret *r);
  void publish(void *v);

  //void notifyfromslice_handler(notifyevent_args *args, void *ret);

protected:
  OneHopLocTable *loctable;
  uint _to_multiplier; //jy: doRPC suffer from timeout if the dst is dead, the min of this value = 3
  uint _join_scheduled;
  uint _stab_timer;
  uint _retry_timer;
  uint _unit_timer;
  list<LogEntry> leader_log;
  list<LogEntry> outer_log;
  list<LogEntry> inner_log;
  list<LogEntry> high_to_low;
  list<LogEntry> low_to_high;
  bool _join_complete;
  bool prev_slice_leader;
  bool _leaderstab_running;
  bool sent_low;
  bool sent_high;
  ConsistentHash::CHID slice_size;
  IDMap _wkn;
};

#endif /* __ONEHOP_H */


