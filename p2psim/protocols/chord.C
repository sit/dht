/*
 * Copyright (c) 2003-2005 Jinyang Li
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

#include "observers/chordobserver.h"
#include "accordion.h"
#include <iostream>
#include <stdio.h>
#include <assert.h>
#include <math.h>

using namespace std;

extern bool vis;
bool static_sim;
unsigned int joins = 0;

vector<uint> Chord::rtable_sz;
#ifdef RECORD_FETCH_LATENCY
double _allfetchlat = 0.0;
double _allfetchsz = 0.0;
unsigned int _allfetchnum = 0;
#endif


Chord::Chord(IPAddress i, Args& a, LocTable *l, const char *name) : P2Protocol(i), _isstable (false)
{

  if(a.find("static_sim") != a.end())
    static_sim = true;
  else
    static_sim = false;

  //stabilization timer
  _stab_basic_timer = a.nget<uint>("basictimer", 10000, 10);
  _stab_succlist_timer = a.nget<uint>("succlisttimer",_stab_basic_timer,10);

  //location table timeout values
  //_timeout = a.nget<uint>("timeout", 5*_stab_succlist_timer, 10);

  //successors
  _nsucc = a.nget<uint>("successors", 16, 10);

  //fragments
  _frag = a.nget<uint>("m",1,10);

  //how many successors are fragments on?
  _allfrag = a.nget<uint>("allfrag",1,10);
  assert(_allfrag <= _nsucc);

  //recursive routing?
  _recurs = a.nget<uint>("recurs",1,10);
  _recurs_direct = a.nget<uint>("recurs_direct",1,10);
  _stopearly_overshoot = a.nget<uint>("stopearlyovershoot",0,10);

  //parallel lookup? parallelism only works in iterative lookup now
  _parallel = a.nget<uint>("parallelism",1,10);
  _alpha = a.nget<uint>("alpha",1,10);

  //lookup using ipkey?
  _ipkey = a.nget<uint>("ipkey",0,10);

  _asap = a.nget<uint>("asap",_frag,10);

  _max_lookup_time = a.nget<uint>("maxlookuptime",4000,10);

  _to_multiplier = a.nget<uint>("timeout_multiplier", 3, 10);
  _random_id = a.nget<uint>("randid",0,10);

  _wkn.ip = 0;

  _learn = a.nget<uint>("learn",0,10);
  learntable = NULL;

  assert(_frag <= _nsucc);

  me.ip = ip();
  assert(me.ip>0);
  if (_random_id)
    me.id = ConsistentHash::getRandID();
  else {
    if (name) 
      me.id = ConsistentHash::ipname2chid(name);
    else
      me.id = ConsistentHash::ip2chid(me.ip);
  }

  if (l) 
    loctable = l;
  else
    loctable = New LocTable();

  loctable->set_timeout(0);

  loctable->set_evict(false);

  loctable->init(me);
  
  if (vis) {
    printf ("vis %llu node %16qx\n", now (), me.id);
  }

  _stab_basic_running = false;

  _stab_basic_outstanding = 0;
  _join_scheduled = 0;
  _last_succlist_stabilized = 0;

}

void
Chord::record_stat(IPAddress src, IPAddress dst, uint type, uint num_ids, uint num_else)
{
  Node::record_bw_stat(type,num_ids,num_else);
  Node::record_inout_bw_stat(src,dst,num_ids,num_else);
}

Chord::~Chord()
{
  if (me.ip == 1) { //same hack as tapestry.C so statistics only gets printed once

    Node::print_stats();
    printf("<-----STATS----->\n");
    sort(rtable_sz.begin(),rtable_sz.end());
    uint totalrtable = 0;
    uint rsz = rtable_sz.size();
    for (uint i = 0; i < rsz; i++) 
      totalrtable += rtable_sz[i];
    printf("RTABLE:: 10p:%u 50p:%u 90p:%u avg:%.2f\n", rtable_sz[(uint)0.1*rsz], rtable_sz[(uint)0.5*rsz],
	rtable_sz[(uint)0.9*rsz], (double)totalrtable/(double)rsz);
    printf("<-----ENDSTATS----->\n");

#ifdef RECORD_FETCH_LATENCY
    printf("fetch lat: %.3f %.3f %u\n", _allfetchlat/_allfetchnum, _allfetchsz/_allfetchnum, _allfetchnum); 
#endif
    printf("total joins seen %u\n",joins);
  }
  delete loctable; 
}

char *
Chord::ts()
{
  static char buf[50];
  sprintf(buf, "%llu %s(%u,%qx)", now(), proto_name().c_str(), me.ip, me.id);
  return buf;
}

string
Chord::header()
{
  char buf[128];
  sprintf(buf, "%llu %s(%u,%qx,%u) ", now(),proto_name().c_str(),me.ip,me.id,first_ip());
  return string(buf);
}

string
Chord::printID(CHID id)
{
  char buf[128];
  sprintf(buf,"%qx ",id);
  return string(buf);
}

void
Chord::add_edge(int *matrix, int sz)
{
  assert(ip() <= (uint)sz);
  vector<IDMap> v = loctable->get_all();
  for (uint i = 0; i < v.size(); i++) {
    if (v[i].ip != me.ip && Network::Instance()->getnode(v[i].ip)->alive()) {
      assert(v[i].ip <= (uint)sz);
      matrix[(me.ip-1)*sz + v[i].ip -1] = 1;
    }
  }
}

void
Chord::check_static_init()
{
  if ((!static_sim) || (!alive())) return;
  if (!_inited) {
    _inited = true;
    this->initstate();
  }
}

Chord::IDMap
Chord::next_hop(ConsistentHash::CHID k) 
{
  return loctable->next_hop(k-1);
}
bool 
Chord::check_correctness(CHID k, vector<IDMap> v)
{
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  IDMap tmp;
  tmp.id = k;
  uint idsz = ids.size();
  uint pos = upper_bound(ids.begin(), ids.end(), tmp, Chord::IDMap::cmp) - ids.begin();
  uint iter = 0;

  while (iter<idsz) {
    if (pos >= idsz) pos = 0;
    Chord *node = (Chord *)Network::Instance()->getnode(ids[pos].ip);
    if (Network::Instance()->alive(ids[pos].ip)
	&& node->inited())
      break;
    pos++;
    iter++;
  }

  for (uint i = 0; i < v.size(); i++) {
    if (ids[pos].ip == v[i].ip) {
      pos = (pos+1) % idsz;
    } else if (ConsistentHash::betweenrightincl(k,ids[pos].id,v[i].id)) {
      CDEBUG(2) << "lookup incorrect(?) key" << printID(k) << " succ should be "
	<< ids[pos].ip << ","<< printID(ids[pos].id) << "instead of " <<
	v[i].ip << "," << printID(v[i].id) << endl;
      return false; 
    } else {
      CDEBUG(2) << "lookup incorrect(?) key" << printID(k) << " succ should be "
	<< ids[pos].ip << ","<< printID(ids[pos].id) << "instead of " <<
	v[i].ip << "," << printID(v[i].id) << endl;
      return false;
    }
  }
  return true;
}

//XXX Currently, it does not handle losses
//Recursive lookup canot deal with duplicate packets
//also, add_node() does not reset status flag yet
template<class BT, class AT, class RT>
bool Chord::failure_detect(IDMap dst, void (BT::* fn)(AT *, RT *), AT *args, RT *ret, 
    uint type, uint num_args_id, uint num_args_else, int num_retry)
{
  bool r;
  Time retry_to = TIMEOUT(me.ip,dst.ip);
  int checks;
  //checks = loctable->add_check(dst,0);
  checks = 2;
  //if (checks == -1) return false;
  //int tmp;
  while (checks < num_retry) {
    record_stat(me.ip, dst.ip, type,num_args_id,num_args_else);
    r = doRPC(dst.ip, fn, args, ret, retry_to);
    if (!alive()) 
      return false;
    if (r) {
      return true;
    }
    checks++;
    retry_to = retry_to * 2;
  }
  return false;
}
void
Chord::learn_info(IDMap n)
{
  //do nothing;
}

bool 
Chord::replace_node(IDMap n, IDMap &replacement)
{
  return false;
}

void
Chord::lookup(Args *args) 
{
  check_static_init();
  lookup_args *a = New lookup_args;

  if (!_ipkey) {
    a->key = args->nget<CHID>("key");
    a->ipkey = 0;
  } else {
    a->ipkey = args->nget<IPAddress>("key");
    if (!Network::Instance()->alive(a->ipkey)) {
      delete a;
      return;
    }
    a->key = dynamic_cast<Chord *>(Network::Instance()->getnode(a->ipkey))->id() + 1;
  }
  CDEBUG(1) << "start looking up key " << printID(a->key) << "ipkey " 
    << a->ipkey << endl;
  assert(a->key);
  a->start = now();
  a->latency = 0;
  a->num_to = 0;
  a->total_to = 0;
  a->retrytimes = 0;
  a->hops = 0;
  lookup_internal(a); //lookup internal deletes a
}

void
Chord::lookup_internal(lookup_args *a)
{
  vector<IDMap> v;
  IDMap lasthop;
  lasthop.ip = 0;

  a->retrytimes++;

  if (_recurs) {
    v = find_successors_recurs(a->key, _frag, TYPE_USER_LOOKUP, &lasthop,a);
  } else {
    v = find_successors(a->key, _frag, TYPE_USER_LOOKUP, &lasthop,a);
  }

  if (!alive()) {
    delete a;
    return;
  }

  if (_learn) {
    if (lasthop.ip) 
      learn_info(lasthop);
    for (uint i = 0; i < v.size(); i++) 
      learn_info(v[i]);
  }

  if (a->latency >= _max_lookup_time) {
    record_lookup_stat(me.ip, lasthop.ip, a->latency, false, false, a->hops, a->num_to, a->total_to);
  }else if ((!_ipkey  && v.size() > 0) || (_ipkey && lasthop.ip == a->ipkey)) {
    record_lookup_stat(me.ip, lasthop.ip, a->latency, true, true, a->hops, a->num_to, a->total_to);
#ifdef RECORD_FETCH_LATENCY
    assert(static_sim);
    assert(v.size() >= _frag);
    vector<uint> tmplat;
    tmplat.clear();
    Topology *t = Network::Instance()->gettopology();
    for (uint i = 0; i < v.size(); i++) {
      if (tmplat.size() < _allfrag)
	tmplat.push_back(2*t->latency(me.ip, v[i].ip));
    }
    sort(tmplat.begin(),tmplat.end());
    _allfetchlat += (double)tmplat[6]; //XXX this is a hack
    _allfetchsz += (double)tmplat.size();
    _allfetchnum++;
#endif
    CDEBUG(1) << "lookup correct key " << printID(a->key) << "interval " 
      << a->latency << endl;
  }else{
    if (_ipkey && a->retrytimes<=1 && (!Network::Instance()->alive(a->ipkey))) {
    }else if (_ipkey && (a->retrytimes >2 || Network::Instance()->alive(a->ipkey))) {
      record_lookup_stat(me.ip, lasthop.ip, a->latency, false, false, a->hops, a->num_to, a->total_to);
    }else{
      CDEBUG(1) << "lookup incorrect key " << printID(a->key)
	<< "lastnode " << lasthop.ip << "," << printID(lasthop.id)
	<< "latency " << a->latency << " start " << a->start << endl;
      a->latency += 100;
      delaycb(100, &Chord::lookup_internal, a);
      return;
    }
  }
  delete a;
}

void
Chord::find_successors_handler(find_successors_args *args, 
			       find_successors_ret *ret)
{
  check_static_init();
  if (_recurs)
    ret->v = find_successors_recurs(args->key, args->m, TYPE_JOIN_LOOKUP, &(ret->last));
  else
    ret->v = find_successors(args->key, args->m, TYPE_JOIN_LOOKUP, &(ret->last));

  if (ret->v.size() > 0) 
    CDEBUG(3) <<" find_successors_handler key "<< printID(args->key) <<"succ " 
      << ret->v[0].ip << "," << printID(ret->v[0].id) << endl;
  ret->dst = me;
}

vector<Chord::IDMap>
Chord::find_successors(CHID key, uint m, uint type, IDMap *lasthop, lookup_args *a)
{
  //parallelism controls how many queries are inflight
  Time before;
  next_args na;
  hash_map<IPAddress, bool> asked;
  hop_info lastfinished;
  list<hop_info> tasks;
  list<hop_info> savefinished;
  bool ok;
  unsigned rpc, donerpc;
  unsigned totalrpc = 0;
  nextretinfo *reuse = NULL;
  nextretinfo *p;
  RPCSet rpcset;
  RPCSet alertset;
  hash_map<unsigned, unsigned> resultmap; //result to rpc number mapping
  hash_map<unsigned, alert_args*> alertmap;
  hop_info h;
  vector<IDMap> results;
  vector<IDMap> to_be_replaced;
  to_be_replaced.clear();
  uint outstanding, parallel, alertoutstanding;
  uint rpc_i;

  h.from = me;
  h.to = me;
  h.hop = 1;

  na.src = me;
  na.type = type;
  na.key = key;
  na.m = m;
  if (type == TYPE_USER_LOOKUP) {
    parallel = _parallel;
    na.alpha = _alpha;
  } else {
    na.alpha = 1;
    parallel = 1;
  }

  vector<nextretinfo*> rpcslots;
  for(uint i = 0; i < parallel; i++) {
    reuse = New nextretinfo;
    reuse->free = true;
    rpcslots.push_back(reuse);
  }

  //init my timeout calculations
  vector<uint> num_timeouts;
  vector<uint> time_timeouts;
  vector<uint> num_hops;
  for (uint i = 0; i < parallel; i++) {
    num_timeouts.push_back(0);
    time_timeouts.push_back(0);
    num_hops.push_back(0);
  }

  na.deadnodes.clear();
  na.retry = false;

  savefinished.clear();
  lastfinished.from.ip = 0;
  lastfinished.to.ip = me.ip;
  lastfinished.to.id = me.id;
  lastfinished.hop = 0;

  //put myself in the task queue
  tasks.push_back(h);
  asked[me.ip] = true;

  results.clear();
  outstanding = alertoutstanding = 0;
 
  /*
  vector<hop_info> recorded;
  recorded.clear();
  */
  CDEBUG(2) << "start lookup key " << printID(key) << "type " 
    << type << endl;

  while (1) {
    assert(totalrpc < 100 && (na.deadnodes.size()<20));

    if ((tasks.size() == 0) && (outstanding == 0))
      tasks.push_back(lastfinished);

    while ((outstanding < parallel) && (tasks.size() > 0)) {
      h = tasks.front();
      if (ConsistentHash::betweenrightincl(h.to.id, key, lastfinished.to.id) && (outstanding > 0))
	break;
      else if ((h.to.ip == lastfinished.to.ip) && (totalrpc > 0)) {
	assert(outstanding == 0);
	na.retry = true;
	tasks.pop_front();
      }else if (ConsistentHash::betweenrightincl(lastfinished.to.id, key, h.to.id) || (totalrpc == 0)) {
	na.retry = false;
	tasks.pop_front();
      }else{
	CDEBUG(2) << "key " << printID(key) << "timeout resume to node " << 
	  lastfinished.to.ip << "," << printID(lastfinished.to.id) << endl;
	na.retry = true;
	h = lastfinished;
      }

      //find a free rpc slot
      uint ii = 0;
      for (ii = 0; ii < parallel; ii++) {
	if (rpcslots[(rpc_i+ii)%parallel]->free) {
	  rpc_i = (rpc_i+ii)%parallel;
	  p = rpcslots[rpc_i];
	  p->free = false;
	  break;
	}
      }
      assert(ii<parallel);

      p->link = h;
      totalrpc++;
      assert(h.to.ip > 0 && h.to.ip < 3000);
      //recorded.push_back(h);

      CDEBUG(2) << "key " << printID(key) << "sending to next hop node "<< h.to.ip
	<< "," << printID(h.to.id) << endl;

      record_stat(me.ip,h.to.ip,type,1+na.deadnodes.size(),0);
      assert((h.to.ip == me.ip) || (h.to.ip!=h.from.ip));
      rpc = asyncRPC(h.to.ip, &Chord::next_handler, &na, &(p->ret), TIMEOUT(me.ip,h.to.ip));
      rpcset.insert(rpc);
      resultmap[rpc] = rpc_i;
      outstanding++;
    }
    
    assert(outstanding > 0);

    //fill out all my allowed parallel connections, wait for response
    before = now();
    donerpc = rcvRPC(&rpcset, ok);
    rpc_i = resultmap[donerpc];
    assert(rpc_i>=0 && rpc_i<parallel);

    if (a) 
      a->latency += (now()-before);

    outstanding--;
    reuse = rpcslots[rpc_i];
    assert(!reuse->free);
    reuse->free = true;

    //count statistics about hop count and timeouts
    num_hops[rpc_i]++;
    if (!ok) {
      num_timeouts[rpc_i]++;
      time_timeouts[rpc_i]+=TIMEOUT(me.ip,reuse->link.to.ip);
    }

    if (!alive()) goto DONE;

    if (ok) {
      record_stat(reuse->link.to.ip,me.ip,type,reuse->ret.done?reuse->ret.next.size():reuse->ret.v.size()); 
      if (reuse->link.from.ip == me.ip) {
	assert(reuse->ret.dst.ip == reuse->link.to.ip);
	loctable->update_ifexists(reuse->ret.dst); 
      }

      CDEBUG(2) << "key " << printID(key) << "outstanding " << outstanding 
	<< " deadsz " << na.deadnodes.size() << " from " << reuse->link.to.ip 
	<< "," << printID(reuse->link.to.id) << "done? " << (reuse->ret.done?1:0)
	<< " nextsz " << reuse->ret.next.size() << " savefinishedsz " 
	<< savefinished.size() << endl;

      if ((reuse->link.from.ip == me.ip) && (!static_sim))
	loctable->update_ifexists(reuse->link.to); 

      if (reuse->ret.done) {
	lastfinished = reuse->link;
	goto DONE;//success
      }

      if (a && a->ipkey && reuse->link.to.ip == a->ipkey) {
	lastfinished = reuse->link;
	goto DONE;
      }

      //XXX: to be fixed, does not look like it will work for parallel lookup
      if ((reuse->ret.next.size() == 0 ) && (outstanding == 0)) {
//	  && reuse->link.to.ip == lastfinished.to.ip)  //this will fail coz i cannot mark this node as dead
	lastfinished = reuse->link;
	goto DONE;//failed
      }
      
      list<hop_info>::iterator iter = tasks.begin();
      IDMap tmptmp;
      for (uint i = 0; i < reuse->ret.next.size(); i++) {

	tmptmp = reuse->ret.next[i];
	if (asked.find(reuse->ret.next[i].ip) != asked.end()) 
	  continue;

	if (_learn)
	  learn_info(reuse->ret.next[i]);

	if (ConsistentHash::betweenrightincl(reuse->ret.next[i].id, key, lastfinished.to.id))
	  continue;

	h.from = reuse->link.to;
	h.to = reuse->ret.next[i];
	h.hop = reuse->link.hop + 1;

	while (iter != tasks.end()) {
	  if (ConsistentHash::between(iter->to.id, key, reuse->ret.next[i].id))
	    break;
	  iter++;
	}
	assert(h.to.ip != me.ip);
	tasks.insert(iter, h);
	asked[h.to.ip] = true;
      }

      // any improvement?
      if (na.retry && lastfinished.to.ip == reuse->link.to.ip) 
	if ((tasks.size() == 0) || ConsistentHash::betweenrightincl(tasks.front().to.id, key, lastfinished.to.id))
	  goto DONE;

      //insert into the history of finished rpcs
      iter = savefinished.begin();
      while (iter != savefinished.end()) {
	if (ConsistentHash::betweenrightincl(me.id, iter->to.id, reuse->link.to.id)) 
	  break;
	iter++;
      }
      if (iter == savefinished.end() || reuse->link.to.ip != iter->to.ip) 
	savefinished.insert(iter, reuse->link);
      lastfinished = savefinished.back();

      CDEBUG(2) << "key " << printID(key) << "from " << reuse->link.to.ip
	<< "," << printID(reuse->link.to.id) << "next? " << 
	(reuse->ret.next.size()>0?reuse->ret.next[0].ip:0) << ","
	<< (reuse->ret.next.size()>0?printID(reuse->ret.next[0].id):0)
	<< " task top " << (tasks.size()>0?tasks.front().to.ip:0)
	<< "," << (tasks.size()>0?printID(tasks.front().to.id):0)
	<< " tasksz " << tasks.size() << " lastfinished " 
	<< lastfinished.to.ip << "," << printID(lastfinished.to.id) << endl;

    } else {

      if (reuse->link.to.ip == me.ip) {
	//a very special wierd case, the node has gone down and up during the RPC
	goto DONE;
      }

      CDEBUG(2) << "key " << printID(key) << "outstanding " << outstanding
	<< " deadsz " << na.deadnodes.size() << " from " << reuse->link.to.ip
	<< "," << printID(reuse->link.to.id) << "DEAD savefinishedsz " 
	<< savefinished.size() << " lastfinished " << lastfinished.to.ip << endl;
  
      if (reuse->link.from.ip == me.ip) {
	if (_learn)
	  to_be_replaced.push_back(reuse->link.to);
      }

      //notify
      alert_args *aa = New alert_args;
      aa->n = reuse->link.to;
      record_stat(me.ip,reuse->link.from.ip,type,1);
      assert(reuse->link.from.ip > 0 && reuse->link.from.ip < 3000);
      rpc = asyncRPC(reuse->link.from.ip, &Chord::alert_handler, aa,(void *)NULL);
      alertset.insert(rpc);
      alertmap[rpc] = aa;
      alertoutstanding++;
      if (ConsistentHash::betweenleftincl(lastfinished.to.id, key, reuse->link.to.id)) {
	na.deadnodes.push_back(reuse->link.to);
      }
      if (lastfinished.to.ip ==  reuse->link.to.ip) {
	assert(savefinished.size()>0);
	savefinished.pop_back();

	CDEBUG(2) << "key " << printID(key) << "lastfinished " << lastfinished.to.ip
	<< "," << printID(lastfinished.to.id) << "dead new last finished " << 
	savefinished.back().to.ip << endl;

	lastfinished = savefinished.back();
      }
    }

    if (a && a->latency >= _max_lookup_time) {
      goto DONE;
    }
  }
DONE:

  //get rid the crap nodes that i've learned
  if (learntable) {
    for (uint i = 0; i < na.deadnodes.size(); i++) 
      learntable->del_node(na.deadnodes[i],true); 
    IDMap replacement;
    for (uint i = 0; i < to_be_replaced.size();i++) 
      replace_node(to_be_replaced[i],replacement);
  }

  //jesus christ i'm done, however, i need to clean up my shit
  assert(reuse);
  assert(rpc_i >=0 && rpc_i<parallel);
  if (a) {
    a->num_to += num_timeouts[rpc_i];
    a->total_to += time_timeouts[rpc_i];
    a->hops += num_hops[rpc_i];
  }

  if ((type == TYPE_USER_LOOKUP) && (alive())) {
    /*
    Topology *t = Network::Instance()->gettopology();
    printf("%s lookup key %qx, hops %d totalrpc %d (lookup info hop %u, num_to %u, retry %u)\n", ts(), key, lastfinished.hop, totalrpc,a->hops,a->num_to,a->retrytimes);
    printf("%s key %qx route: ", ts(), key);
    IDMap last;
    last = lastfinished.to;
    for (uint i = recorded.size()-1; i >= 0; i--) {
      if (last.ip == me.ip) break;
      if (recorded[i].to.ip == last.ip) {
	printf("(%u,%qx %llu) ", recorded[i].to.ip, recorded[i].to.id, (uint)2*t->latency(me.ip, last.ip));
	last = recorded[i].from;
      }
    }
    printf("\n");
    */
    if ((reuse->ret.done) && (reuse->ret.correct))
      results = reuse->ret.v;

  }else {
    if (reuse->ret.done) 
      results = reuse->ret.v;
  }

  if (lasthop) {
    *lasthop = reuse->link.to;
  }

  for (uint i = 0; i < outstanding; i++) {
    donerpc = rcvRPC(&rpcset, ok);
    rpc_i = resultmap[donerpc];
    assert(rpc_i>=0 && rpc_i < parallel);
    reuse = rpcslots[rpc_i];
    if (ok) 
      record_stat(reuse->link.to.ip,me.ip,type,reuse->ret.done?reuse->ret.next.size():reuse->ret.v.size());
  }

  for (uint i = 0; i < parallel; i++) {
    delete rpcslots[i];
  }

  for (uint i = 0;i < alertoutstanding; i++) {
    donerpc = rcvRPC(&alertset, ok);
    alert_args *aa = alertmap[donerpc];
    if (ok)
      record_stat(aa->n.ip,me.ip,type,0);
    delete alertmap[donerpc];
  }
  return results;
}


/* the recursive query goes directly back to the sender. */
void
Chord::final_recurs_hop(next_recurs_args *args, next_recurs_ret *ret)
{
  lookup_path tmp;
  tmp.n = me;
  tmp.tout = 0;
  ret->path.push_back(tmp);
  ret->finish_time = now();
}

vector<Chord::IDMap>
Chord::find_successors_recurs(CHID key, uint m, uint type, IDMap *lasthop, lookup_args *a) 
{
  next_recurs_args fa;
  fa.key = key;
  fa.type = type;
  fa.m = m;
  fa.src = me;

  CDEBUG(3) << "find_successors_recurs start key " << printID(key) << endl;
  //do the parallel recursive lookup thing
  //doRPC(me.ip, &Chord::next_recurs_handler, args, ret);
  hash_map<unsigned, next_recurs_ret*> resultmap;
  next_recurs_ret *reuse = NULL;
  next_recurs_ret *p = NULL;
  uint outstanding, parallel;
  bool ok;
  Time before = now();
  vector<IDMap> results;
  results.clear();
  lookup_path tmp;
  
  unsigned rpc, donerpc;
  RPCSet rpcset;

  if (a) {
    fa.ipkey = a->ipkey;
    parallel = _parallel;
  } else {
    fa.ipkey = 0;
    parallel = 1;
  }
  fa.src = me;

  IDMap nexthop;
  nexthop.id = key;
  nexthop.ip = 0;
  outstanding = 0;

  assert(!_stopearly_overshoot || _parallel == 1); //i do not know how to do parallel lookup with stopearly and overshoot

  while (1) {

    if (!alive()) return results;
    IDMap succ = loctable->succ(me.id+1,LOC_HEALTHY);
    if (succ.ip == 0) {
      if (!_join_scheduled) {
	_join_scheduled++;
	delaycb(0, &Chord::join, (Args *)0);

	CDEBUG(2) << " find_successors_recurs key " << printID(key) 
	  << "no succ " << endl;
      }
      if (lasthop) *lasthop = me;
      return results;
    }

    while (outstanding < parallel) {
      if (_stopearly_overshoot) 
	nexthop = me;
      else {
	nexthop = loctable->next_hop(nexthop.id-1);
	if(nexthop.ip == 0) {
	  if (reuse) {
	    delete reuse;
	    reuse = NULL;
	  }
	  break;
	}
      }
      if (reuse) {
	p = reuse;
	reuse = NULL;
      }else {
	p = New next_recurs_ret;
	p->path.clear();
      }

      p->correct = false;
      p->lasthop = me;
      p->v.clear();
      p->finish_time = 0;

      if ((a && a->ipkey && me.ip == a->ipkey) || (ConsistentHash::between(me.id,succ.id,key))) {
	if (fa.m == 1) {
	  p->v.push_back(succ);
	}else{
	  p->v = loctable->succs(key,fa.m,LOC_HEALTHY);
	}
	p->correct = check_correctness(key,p->v);
	p->finish_time = now();
	reuse = p;
	goto RECURS_DONE;
      }

      if (nexthop.ip!=me.ip) {
	tmp.n = nexthop;
	tmp.tout = 0;
	p->path.push_back(tmp);
      }

      CDEBUG(2) << " key " << (a?a->ipkey:0) << "," << printID(key) 
	<< "via nexthop " << nexthop.ip << "," << printID(nexthop.id) 
	<< "outstanding " << outstanding << " parallel " << parallel << endl;

      record_stat(me.ip,nexthop.ip,type,1);
      p->nexthop = nexthop;
      p->prevhop = me;
      assert(!reuse);
      rpc = asyncRPC(nexthop.ip, &Chord::next_recurs_handler, &fa, p, TIMEOUT(me.ip,nexthop.ip));
      rpcset.insert(rpc);
      resultmap[rpc] = p;
      outstanding++;
    }
    assert(outstanding>0);
    donerpc = rcvRPC(&rpcset, ok);
    outstanding--;
    reuse = resultmap[donerpc];
    if (ok) {
      loctable->update_ifexists(reuse->nexthop);
      record_stat(reuse->nexthop.ip,me.ip,type,resultmap[donerpc]->v.size());
      goto RECURS_DONE;
    }else{
      //do a long check to see if next hop is really dead
      assert(reuse->path.size()>0);
      IDMap n = reuse->path[reuse->path.size()-1].n;

      CDEBUG(2) << " key " << (a?a->ipkey:0) << "," << printID(key) << "nexthop "
	<< n.ip << "," << printID(n.id) << "failed outstanding " << outstanding <<endl;

      reuse->path[reuse->path.size()-1].tout=1;
      IDMap replacement;
      //if ((!_learn) || (!replace_node(n,replacement))) {
      int check = loctable->add_check(n);
      if (check == LOC_ONCHECK) {
	alert_args *tmp = New alert_args;
	tmp->n = n;
	tmp->dst = me.ip;
	delaycb(1, &Chord::alert_delete, tmp);
      }
      replace_node(n,replacement);
      //} 
    }
  }

RECURS_DONE:
  assert(reuse);
  if (lasthop) {
    *lasthop = (reuse->lasthop);
  }

  CDEBUG(1)<<" key " << printID(key) <<"finished lasthop "<< reuse->lasthop.ip 
    <<","<< printID(reuse->lasthop.id) << "vsize " << reuse->v.size() 
    <<" correct? "<< (reuse->correct?1:0) <<" hops "<<reuse->path.size()<<endl;

  if (a) {

    IDMap prev = me;
    if (_recurs_direct) {
      assert(reuse->finish_time > 0);
      a->latency += (reuse->finish_time-before);
    }else{
      a->latency += (now()-before);
    }
    a->hops += reuse->path.size();
    for (uint j= 0; j< reuse->path.size(); j++) {
      if (reuse->path[j].tout>0) {
	a->num_to++;
	a->total_to += TIMEOUT(prev.ip,reuse->path[j].n.ip);
      }else{
	prev = reuse->path[j].n;
      }
    }
  }

  if (!a || reuse->correct) {
    results = reuse->v;
  }else{
    results.clear();
  }

  if (reuse->lasthop.ip && _learn) 
    learn_info(reuse->lasthop);

  delete reuse;

  //garbage collection
  for (uint i = 0; i < outstanding; i++) {
    donerpc = rcvRPC(&rpcset, ok);
    if (ok) {
      loctable->update_ifexists(resultmap[donerpc]->nexthop);
      record_stat(resultmap[donerpc]->nexthop.ip,me.ip,type,resultmap[donerpc]->v.size());
    }
    if ((_learn) && (resultmap[donerpc]->lasthop.ip))
      learn_info(resultmap[donerpc]->lasthop);
    delete resultmap[donerpc];
  }

  return results;
}

char *
Chord::print_path(vector<lookup_path> &p, char *tmp)
{
  char *begin = tmp;
  tmp += sprintf(tmp,"<");
  for (uint i = 0; i < p.size(); i++) {
    tmp += sprintf(tmp, " %u",p[i].n.ip);
  }
  tmp += sprintf(tmp," >");
  return begin;
}

//XXX: in a dynamic environment, the current implementation has a 
//wierd mechanism for implementing timeout.
//it sends an empty reply to the sender to indicate that 
//some node currently holding the packet has died
//in a real implemenation, the sender should implement some kind of 
//timeout mechanism.
void
Chord::next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  vector<IDMap> succs;
  IDMap succ;
  bool r;
  lookup_path tmp;
  IDMap next;
  uint sz, i;

  check_static_init();
  assert(alive());

  if (_learn) {
    assert(args->src.ip>0 && args->src.ip < 100000);
    learn_info(args->src);
    if (ret->prevhop.ip!=args->src.ip) 
      learn_info(ret->prevhop);
  }

  Topology *t = Network::Instance()->gettopology();

  CDEBUG(2) << " next_recurs key " << args->ipkey << "," << printID(args->key)
    << "arrived pathsz " << ret->path.size() <<" src "<< args->src.ip << endl;

  while (1) {

    if (!alive()) {
      ret->v.clear();
      ret->correct = false;
      ret->finish_time = now();
      ret->lasthop.ip = 0;
      ret->nexthop = me;
      return;
    }
    succs = loctable->succs(me.id+1,_nsucc,LOC_HEALTHY);
    if (succs.size() == 0) {
      //lookup failed
      //XXX do i need to backtrack?
     ret->v.clear();

     vector<IDMap> tmp = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
      //rejoin baby 
      if ((!_join_scheduled) && (tmp.size()==0)) {
	_join_scheduled++;
	delaycb(0, &Chord::join, (Args *)0);

	CDEBUG(2) <<" next_recurs key "<< printID(args->key)
	  << "no succ rejoin "<< endl;
      }
      ret->correct = false;
      ret->lasthop = me;
      if (args->type==TYPE_USER_LOOKUP && _recurs_direct) {
	record_stat(me.ip,args->src.ip,args->type,ret->v.size(),0);
	r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	if (r) 
	  record_stat(args->src.ip,me.ip,args->type,0);
	else
	  ret->finish_time=now(); //who cares about finishtime now
      }
      ret->nexthop = me;
      return;
    }

    succ = succs[0];
    next = loctable->next_hop(args->key);

    if ((_ipkey && me.ip == args->ipkey) || 
	(ConsistentHash::betweenrightincl(me.id, succs[0].id, args->key))) {
      ret->v.clear();
      sz = args->m < succs.size()? args->m : succs.size();
      for (i = 0; i < sz; i++) {
	ret->v.push_back(succs[i]);
	if (ret->v.size() >= args->m) break;
      }
      if (args->type == TYPE_USER_LOOKUP) {
	ret->correct = check_correctness(args->key,ret->v);
	if (!ret->correct) 
	  CDEBUG(3) << "next_recurs_handler key " << printID(args->key) 
	    << " incorrect succ " << succs[0].ip << "," 
	    << printID(succs[0].id) << endl;
      } else
	ret->correct = true;

      ret->lasthop = me;
      if (_recurs_direct) {
	record_stat(me.ip,args->src.ip,args->type,ret->v.size(),0);
	r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	if (r) 
	  record_stat(args->src.ip,me.ip,args->type,0);
	else
	  ret->finish_time=now(); //who cares about finishtime now
      }
      ret->nexthop = me; //the return path
      return;
    }else if (_stopearly_overshoot && ret->v.size()==0 && ConsistentHash::betweenrightincl(me.id, succs[succs.size()-1].id, args->key)) {
      assert(static_sim);
      uint start = 0;
      for (i = 0; i < succs.size(); i++) {
	if ( ConsistentHash::betweenrightincl(me.id, succs[i].id, args->key)) {
	  ret->v.push_back(succs[i]);
	  if (ret->v.size()>1)
	    assert(ConsistentHash::between(args->key,ret->v[ret->v.size()-1].id,ret->v[0].id));
	  if (!start) start = i;
	}
      }
      if (ret->v.size() >= args->m) {
	assert(ret->v.size() <= _nsucc);
	ret->correct = check_correctness(args->key,ret->v);
	assert(ret->correct);
	if (_recurs_direct) {
	  record_stat(me.ip,args->src.ip,args->type,1,0); 
	  assert(ret->v.size() >= args->m && args->m >= 7);
	  r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	  if (r) 
	    record_stat(args->src.ip,me.ip,args->type,0);
	}
	ret->nexthop = me;
	return;
      }

      int currsz = ret->v.size();
      Time min_lat = 1000000;
      Time lat;
      for (i = 0; i < succs.size(); i++) {
	if ((currsz + i + 1)>= args->m) {
	  lat = t->latency(me.ip, succs[i].ip);
	  if (min_lat > lat) {
	    min_lat = lat;
	    next = succs[i];
	  }
	}
      }
      assert(min_lat < 1000000);
    }else if (_stopearly_overshoot && ret->v.size() > 0) {
      assert(_nsucc == succs.size());
      assert(static_sim);
      //handling overshoot, just append my succ list
      uint start = 0;

      if (me.ip != ret->v[ret->v.size()-1].ip) {
	for (start = 0; start < succs.size(); start++) {
	  if (succs[start].ip == ret->v[ret->v.size()-1].ip) {
	    break;
	  }
	}
	start++;
      }
      if (start > succs.size()) {
	start = 0;
	while (start < succs.size()) {
	  if (ConsistentHash::between(me.id,succs[start].id,args->key) && ConsistentHash::between(args->key,succs[start].id, ret->v[ret->v.size()-1].id)) 
	    break;
	  start++;
	}
      }
      IDMap tmp,tmp2,tmp3,tmp4;
      for (i = start; i < succs.size(); i++) {
	tmp = succs[i];
	if (i>1) {
	  tmp2 = succs[i-1];
	}
	tmp3 = ret->v[ret->v.size()-1];
	tmp4 = ret->v[0];

	ret->v.push_back(succs[i]);
        if (ret->v.size()>1)
	  assert(ConsistentHash::between(args->key,ret->v[ret->v.size()-1].id,ret->v[0].id));
	if (ret->v.size() >= args->m) {
	  assert(ret->v.size() <= _nsucc);
	  ret->correct = check_correctness(args->key,ret->v);
	  assert(ret->correct);
	  if (_recurs_direct) {
	    record_stat(me.ip,args->src.ip,args->type,1,0);
	    assert(ret->v.size() >= args->m);
	    r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	    if (r)
	      record_stat(args->src.ip,me.ip,args->type,0);
	  }
	  ret->nexthop = me;
	  return;
	}
      }
      assert(0);
    } else {
      if (ret->path.size() >= 30) {
	printf("WARNING: path too long for key %qx m %d %s: ", args->key, args->m, ts());
	for (uint i = 0; i < ret->path.size(); i++) {
	  printf("(%u,%qx,%u) ", ret->path[i].n.ip, ret->path[i].n.id, ret->path[i].tout);
	}
	printf("\n");
	if (_recurs_direct) {
	  ret->finish_time = now(); //not correct, but what the heck
	}
	ret->correct = false;
	ret->v.clear();
	ret->nexthop = me;
	return;
      } 
    }

    assert(_stopearly_overshoot|| (next.ip != me.ip && ConsistentHash::between(me.id, args->key, next.id)));

    tmp.n = next;
    tmp.tout = 0;
    ret->path.push_back(tmp);

    record_stat(me.ip,next.ip,args->type,1);
    ret->nexthop = next;
    ret->prevhop = me;
    r = doRPC(next.ip, &Chord::next_recurs_handler, args, ret, TIMEOUT(me.ip,next.ip));

    if (_learn && !_recurs_direct) {
      if (ret->lasthop.ip) 
	learn_info(ret->lasthop);
      for (uint x = 0; x < ret->v.size(); x++) 
	learn_info(ret->v[x]);
    }

    if (!_recurs_direct && (!alive())) {
      CDEBUG(3) << " next_recurs_handler lost key " << printID(args->key)<<endl;
      ret->lasthop.ip = 0;
      ret->v.clear();
      ret->nexthop = me;
      ret->correct = false;
      if (!ret->finish_time)
	ret->finish_time = now();
      return;
    }

    if (r) {
      if (!_recurs_direct) {
	record_stat(next.ip,me.ip,args->type,ret->v.size());
      }else{
	record_stat(next.ip,me.ip,args->type,0);
      }
      if (!static_sim) loctable->update_ifexists(ret->nexthop); 
      ret->nexthop = me;
      return;
    }else{
      CDEBUG(3) << "next_recurs_handler key " << printID(args->key) 
	<< " nexthop " << next.ip << "," << printID(next.id) << endl;
      if (vis) 
	printf ("vis %llu delete %16qx %16qx succ %u,%qx\n", now (), me.id, next.id, succ.ip,succ.id);

      ret->path[ret->path.size()-1].tout = 1;
      
      //do a long check to see if next hop is really dead
      IDMap replacement;
      if ((!_learn) || (!replace_node(next,replacement))) {
	int check = loctable->add_check(next);
	if (check == LOC_ONCHECK) {
	  alert_args *tmp = New alert_args;
	  tmp->n = next;
	  tmp->dst = me.ip;
	  delaycb(1, &Chord::alert_delete, tmp);
	}
      }
    }
  }
}

void
Chord::null_handler (void *args, IDMap *ret) 
{
  return;
}

void
Chord::alert_delete(alert_args *aa)
{
  if (aa->dst != me.ip) 
    record_stat(me.ip,aa->dst,TYPE_MISC,1);
  doRPC(aa->dst, &Chord::alert_handler, aa, (void *)NULL, TIMEOUT(me.ip,aa->dst));
  delete aa;
}

// Always attempt to go to the predecessor for the key regardless of m
void
Chord::next_handler(next_args *args, next_ret *ret)
{
  if (_learn) 
    learn_info(args->src);

  ret->dst = me;
  check_static_init();

  /* don't trust other people's information about my dead neighbors coz of non-transitivity
     however, in case this is a retried query, take it more seriously*/
  /*
  if ((args->retry) && (s >= 0)) {
    for (s = 0; s < (int)succs.size(); s++) {
      for (j = 0; j < (int)args->deadnodes.size(); j++)  {
	if (succs[s].ip == args->deadnodes[j].ip) {
	  alert_args *tmp = New alert_args;
	  tmp->n = succs[s];
	  tmp->dst = me.ip;
	  delaycb(1, &Chord::alert_delete, tmp);
	  break;
	}
      }
      if (j >= (int) args->deadnodes.size()) {
	break;
      }
    }
  }
  */
  if (args->deadnodes.size() > 0) {
    for (uint i = 0; i < args->deadnodes.size(); i++) {
      int check = loctable->add_check(args->deadnodes[i]);
      if (check == -1 || check == LOC_DEAD) {
      } else {
	alert_args *tmp = New alert_args;
	tmp->n = args->deadnodes[i];
	tmp->dst = me.ip;
	delaycb(1, &Chord::alert_delete, tmp);
      }
    }
  }

  int s;
  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc, LOC_HEALTHY);
  if (succs.size() > 0) 
    s = 0;
  else
    s = -1;

  assert((!static_sim) || succs.size() >= _allfrag);

  ret->v.clear();
  ret->next.clear();

  if (s < 0) {
    ret->lastnode = me;
    ret->done = true;
    vector<IDMap> tmp = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
    CDEBUG(3) << "next_handler key " << printID(args->key) 
      << " failed due to newjoin deadsz " << args->deadnodes.size() 
      << endl;
    ret->correct = false;
    //rejoin baby
    if ((!_join_scheduled) && (tmp.size()==0)){
      _join_scheduled++;
      delaycb(0, &Chord::join, (Args *)0);
    }
  } else if (ConsistentHash::betweenrightincl(me.id, succs[s].id, args->key)) { 
    //XXX: need to take care of < m nodes situation in future
    for (int i = s; i < (int)succs.size(); i++) {
      ret->v.push_back(succs[i]);
      if (ret->v.size() >= args->m) break;
    }
    ret->lastnode = me;
    ret->done = true;
    if (args->type == TYPE_USER_LOOKUP) {
      ret->correct = check_correctness(args->key,ret->v);
      if (!ret->correct) 
	CDEBUG(3) << "key " << printID(args->key) << "incorrect succ " 
	  << succs[0].ip << "," << printID(succs[0].id) << endl;
    }else
      ret->correct = true; //i don't check correctness for non-user lookups
  } else {
    ret->done = false;
    ret->next.clear();
    ret->next = loctable->next_hops(args->key,args->alpha);
  }
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{

  if (static_sim) {
    if ((args) && (!_inited))
      notifyObservers((ObserverInfo *)"join");
    _inited = true;
    return;
  }

  if (args) {
    me.ip = ip();
    if (_random_id)
      me.id = ConsistentHash::getRandID();
    else 
      me.id = ConsistentHash::ip2chid(me.ip);
    me.timestamp = 0;

    loctable->init(me);
    if (_learn) 
      learntable->init(me);

    _last_join_time = now();
    ChordObserver::Instance(NULL)->addnode(me);
    notifyObservers((ObserverInfo *)"join");

    _join_scheduled++;
    // XXX: Thomer says: not necessary
    // node()->set_alive(); //if args is NULL, it's an internal join
    CDEBUG(1) << "start to join " << endl;
  }else{

    if (!alive()) {
      _join_scheduled--;
      return;
    }
    CDEBUG(2) << "rescheduled join " << endl;
  }

  if (vis) {
    printf("vis %llu join %16qx\n", now (), me.id);
  }

  if (!_wkn.ip) {
    assert(args);
    _wkn.ip = args->nget<IPAddress>("wellknown");
    assert (_wkn.ip);
    _wkn.id = dynamic_cast<Chord *>(Network::Instance()->getnode(_wkn.ip))->id();
  }

  find_successors_args fa;
  find_successors_ret fr;
  //fa.key = me.id + 1;
  fa.key = me.id - 1;
  //try to get multiple successors in case some failed
  assert(_nsucc > 3);
  fa.m = _nsucc;

  bool ok = failure_detect(_wkn, &Chord::find_successors_handler, &fa, &fr, TYPE_JOIN_LOOKUP,1,0);
  assert(_wkn.ip == fr.dst.ip);
  _wkn = fr.dst;
  joins++;
  if (ok) record_stat(_wkn.ip,me.ip,TYPE_JOIN_LOOKUP, fr.v.size());

  if (!alive()) {
    _join_scheduled--;
    return;
  }

  if (!ok || fr.v.size() < 1) {
    CDEBUG(2) << "join failed retry later" << endl;
    _join_scheduled--;
    if (!_join_scheduled) {
      delaycb(200, &Chord::join, (Args *)0);
      _join_scheduled++;
    }
    return;
  }

  assert (ok);

  for (uint i = 0; i < fr.v.size(); i++) {
    if (fr.v[i].ip != me.ip) 
      loctable->add_node(fr.v[i],true);
    _inited = true;
  }

  if (fr.v.size()>1) 
    _last_succlist_stabilized = now();

  CDEBUG(1) << "joined succ " << fr.v[0].ip << "," << printID(fr.v[0].id) << endl;
 
  if (!_stab_basic_running) {
    _stab_basic_running = true;
    delaycb(0, &Chord::reschedule_basic_stabilizer, (void *) 0);
  }else{
    Chord::stabilize();
  }

  if ((loctable->size() < 2)  && (alive())) {
    CDEBUG(2) << "after stabilize join failed" << endl;
    _join_scheduled--;
    if (!_join_scheduled) {
      delaycb(200, &Chord::join, (Args *)0);
      _join_scheduled++;
      return;
    }
  }
  _join_scheduled--;
}


void
Chord::reschedule_basic_stabilizer(void *x)
{
  assert(!static_sim);
  if (!alive()) {
    _stab_basic_running = false;
    CDEBUG(3) << "node dead cancel stabilizing " << endl;
    return;
  }

  _stab_basic_running = true;
  if (_stab_basic_outstanding > 0) {
    assert(0);
  }else{
    _stab_basic_outstanding++;
    stabilize();
    _stab_basic_outstanding--;
    assert(_stab_basic_outstanding == 0);
  }
  delaycb(_stab_basic_timer, &Chord::reschedule_basic_stabilizer, (void *) 0);
}

// Which paper is this code from? -- PODC 
// stabilization is not run in one non-interruptable piece
// after each possible yield point, check if the node is dead.
void
Chord::stabilize()
{
  assert(!static_sim);
  if (!_inited) return;

  IDMap pred1 = loctable->pred(me.id-1, LOC_ONCHECK);
  IDMap succ1 = loctable->succ(me.id+1, LOC_ONCHECK);
  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc, LOC_ONCHECK);
  if (!succ1.ip) return;

  CDEBUG(3) << "chord_stabilize start pred " << pred1.ip << "," 
    << printID(pred1.id) << "succ " << succ1.ip << "," 
    << printID(succ1.id) << endl;

  fix_successor();

  if (!alive()) return;

  succs = loctable->succs(me.id+1, _nsucc, LOC_HEALTHY);

  assert(_nsucc >= 3);
  if ((_nsucc > 1) && ((succs.size() < 0.5*_nsucc) || (now()-_last_succlist_stabilized > _stab_succlist_timer))) {
    _last_succlist_stabilized = now();
    fix_successor_list();
  }

  if (!alive()) return;

  fix_predecessor();

  if (!alive()) return;

  IDMap pred = loctable->pred(me.id-1, LOC_ONCHECK);
  IDMap succ = loctable->succ(me.id+1,LOC_ONCHECK);
  CDEBUG(3) << "chord_stabilize done pred "<< pred.ip <<","<< printID(pred.id) 
    << "succ " << succ.ip << "," << printID(succ.id) << endl;
}

bool
Chord::stabilized(vector<CHID> lid)
{
  vector<CHID>::iterator iter;
  iter = find(lid.begin(), lid.end(), me.id);
  assert(iter != lid.end());

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc, LOC_ONCHECK);

  if (succs.size() != _nsucc) 
    return false;

  for (unsigned int i = 1; i <= _nsucc; i++) {
    iter++;
    if (iter == lid.end()) iter = lid.begin();
    if (succs[i-1].id != *iter) {
      printf("%s not stabilized, %5d succ should be %16qx instead of (%u, %16qx)\n", 
	  ts(), i-1, *iter, succs[i-1].ip,  succs[i-1].id);
      return false;
    }
  }

  return true;
}

void
Chord::oracle_node_died(IDMap n)
{
  //assume a dead node will affect routing tables of those that contain it
  IDMap tmp = loctable->succ(n.id, LOC_ONCHECK);
  if (tmp.ip != n.ip) return;

  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  int my_pos = find(ids.begin(), ids.end(), me) - ids.begin();

  //lost my predecessor
  IDMap pred = loctable->pred(me.id-1);
  if (tmp.ip == pred.ip) {
    loctable->del_node(n);
    loctable->add_node(ids[(my_pos-1)%sz]);
    CDEBUG(3) << "chord_oracle_node_died pred del " << n.ip << "," 
      << printID(n.id) << " add " << ids[(my_pos-1)%sz].ip << ","
      << printID(ids[(my_pos-1)%sz].id) << endl;
    return;
  }

  //lost one of my successors
  vector<IDMap> succs = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
  IDMap last = succs[succs.size()-1];
  if (ConsistentHash::betweenrightincl(me.id, succs[succs.size()-1].id, n.id)) {
    loctable->del_node(n);
    loctable->add_node(ids[(my_pos+_nsucc)%sz],true);
    vector<IDMap> newsucc = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
    CDEBUG(3) << "chord_oracle_node_died succ del " << n.ip << ","
      << printID(n.id) << "add " << ids[(my_pos+_nsucc)%sz].ip 
      << "," << printID(ids[(my_pos+_nsucc)%sz].id) << endl;
  }
}

void
Chord::oracle_node_joined(IDMap n)
{
  IDMap tmp = loctable->succ(n.id, LOC_ONCHECK);
  if (tmp.ip == n.ip) return;

  //is the new node my predecessor?
  IDMap pred = loctable->pred(me.id-1);
  if (ConsistentHash::between(pred.id,me.id, n.id)) {
//    loctable->del_node(pred);
    loctable->add_node(n);
    CDEBUG(3) << "chord_oracle_node_joined pred del " << pred.ip << "," 
      << printID(pred.id) << "add " << n.ip << "," << printID(n.id) << endl;
    return;
  }
 
  //is the new node one of my successor?
  vector<IDMap> succs = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
  //uint ssz = succs.size();
  if (succs.size() > 0 && ConsistentHash::between(me.id,succs[succs.size()-1].id,n.id)) {
 //   loctable->del_node(succs[succs.size()-1]);
    loctable->add_node(n,true);
  //  vector<IDMap> newsucc = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
   // uint nssz = newsucc.size();
    //assert(nssz == _nsucc);
    CDEBUG(3) << "chord_oracle_node_joined succ add " << n.ip << "," 
      << printID(n.id) << endl;
  }
}

void
Chord::initstate()
{
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  uint sz = ids.size();
  uint my_pos = find(ids.begin(), ids.end(), me) - ids.begin();
  assert(ids[my_pos].id == me.id);
  //add successors and (each of the successor's predecessor)
  for (uint i = 1; i <= _nsucc; i++) {
    loctable->add_node(ids[(my_pos + i) % sz],true);
  }
  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);

  IDMap succ1 = loctable->succ(me.id+1);
  CDEBUG(3) << "chord_init_state sz " << loctable->size() << " succ " 
    <<  succ1.ip << "," << printID(succ1.id) << endl;
  _inited = true;
}

//pings predecessor and fix my predecessor pointer if 
//old predecessor's successor pointer has changed
void
Chord::fix_predecessor()
{

  //ping my predecessor
  IDMap pred = loctable->pred(me.id-1);
  if ((!pred.ip) || (pred.ip == me.ip)) return;

  bool ok;
  get_predsucc_args gpa;
  get_predsucc_ret gpr;

  gpa.pred = false;
  gpa.m = 1;
  ok = failure_detect(pred, &Chord::get_predsucc_handler, &gpa, &gpr, TYPE_FIXPRED_UP);
  if (ok) record_stat(pred.ip,me.ip,TYPE_FIXPRED_UP,gpr.v.size());

  if (!alive()) return;
  if (ok) {
    loctable->update_ifexists(gpr.dst); 
    if (gpr.v.size() > 0) {
      IDMap tmp = gpr.v[0];
      loctable->add_node(gpr.v[0]);
      if (gpr.v[0].ip == me.ip) {
	_inited = true;
      }
    }
  } else
    loctable->del_node(pred); 

}

//fix successor notify predecessor of its sucessor change
void
Chord::fix_successor(void *x)
{
  IDMap succ1, succ2,pred;
  get_predsucc_args gpa;
  get_predsucc_ret gpr;
  bool ok;

  gpa.m = 0;
  gpa.pred = true;
  alert_args aa;

  assert(alive());

  //while (1) {

    aa.n.ip = 0;

    succ1 = loctable->succ(me.id+1,LOC_ONCHECK); 
    if (succ1.ip == 0 || succ1.ip == me.ip) {
      //sth. wrong, i lost my succ, join again
      if (!_join_scheduled) {
	_join_scheduled++;
	CDEBUG(3) << "fix_successor re-join" << endl;
	delaycb(0, &Chord::join, (Args *)0);
      }
      return;
    }

    ok = failure_detect(succ1,&Chord::get_predsucc_handler, &gpa, &gpr, TYPE_FIXSUCC_UP,0);
    if (ok) record_stat(succ1.ip,me.ip,TYPE_FIXSUCC_UP,2);

    if (!alive()) return;

    if (!ok) {
      CDEBUG(3) << "fix_successor old succ " << succ1.ip << "," 
	<< printID(succ1.id) << "dead" << endl;
      loctable->del_node(succ1, true); //successor dead, force delete
      aa.n = succ1;
    } else {
      assert(gpr.dst.ip == succ1.ip);
      loctable->update_ifexists(gpr.dst); 

      CDEBUG(3) << "fix_successor succ " << succ1.ip << "," << printID(succ1.id)
	<< " his pred is " << gpr.n.ip << "," << printID(gpr.n.id) << endl;

      if (!gpr.n.ip) {
	//sigh, that person is clueless about his pred
      }else if (gpr.n.ip && gpr.n.ip == me.ip) {
	return;
      }else if ((gpr.n.ip && gpr.n.ip!= me.ip)) {
	if (ConsistentHash::between(me.id, succ1.id, gpr.n.id)) {
	  loctable->add_node(gpr.n,true); //successor has changed, i should stabilize it immeidately
	} else {
	  assert(gpr.n.ip == me.ip || ConsistentHash::between(gpr.n.id, succ1.id, me.id));
	  pred = loctable->pred(me.id-1, LOC_HEALTHY);
	  if (ConsistentHash::between(pred.id,me.id,gpr.n.id))
	    loctable->add_node(gpr.n);
	  //my successor's predecessor is behind me
	  //notify my succ of his predecessor change
	  notify_args na;
	  notify_ret nr;
	  na.me = me;

	  ok = failure_detect(succ1, &Chord::notify_handler, &na, &nr, TYPE_FIXSUCC_UP, 2, 0);
	  if (ok) record_stat(succ1.ip,me.ip,TYPE_FIXSUCC_UP,0);

	  if(!alive()) return;
      
	  if (!ok) {
	    CDEBUG(3) << "fix_successor notify succ " << succ1.ip << "," 
	      << printID(succ1.id) << "dead" << endl;
	    loctable->del_node(succ1,true); //successor dead, force delete
	    aa.n = succ1;
	  }
	}
      }
    }

    //notify my new successor of this node's death 
    if (aa.n.ip) {
      succ2 = loctable->succ(me.id+1, LOC_ONCHECK); 
      aa.n = succ1;
      if (succ2.ip) {
	ok = failure_detect(succ2, &Chord::alert_handler, &aa, (void*)NULL, TYPE_FIXSUCC_UP,1,0);
	if (ok) {
	  //i should not immediately stabilize new successor,  
	  //i should wait for this new successor to discover the failure himself
	  record_stat(succ2.ip,me.ip,TYPE_FIXSUCC_UP,0);
	} else {
	  loctable->del_node(succ2,true);
	}
	return;
      }
    }
  //}
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(me.id+1);
  if (!succ.ip) return;

  get_predsucc_args gpa;
  get_predsucc_ret gpr;
  bool ok;

  gpa.m = _nsucc;
  gpa.pred = false;
  gpr.v.clear();

  ok = failure_detect(succ, &Chord::get_predsucc_handler, &gpa, &gpr,TYPE_FIXSUCCLIST_UP);
  if (ok) record_stat(succ.ip,me.ip,TYPE_FIXSUCCLIST_UP, gpr.v.size());

  if (!alive()) return;

  if (!ok) {
    loctable->del_node(succ,true);
  }else{

    loctable->update_ifexists(gpr.dst);

    //scs[0] might not be succ anymore
    vector<IDMap> scs = loctable->succs(me.id + 1, _nsucc, LOC_DEAD);
    assert(scs.size() <= _nsucc);

    uint scs_i, gpr_i;
    gpr_i = scs_i = 0;
    while (scs_i < scs.size()) {
      if (scs[scs_i].ip == succ.ip) 
	break;
      scs_i++;
    }
    scs_i++;

    while (1) {
      if (gpr_i >= gpr.v.size()) {
	break;
      }

      if (scs_i >= scs.size()) {
	while (scs_i < _nsucc && gpr_i < gpr.v.size()) {
	  loctable->add_node(gpr.v[gpr_i],true);
	  gpr_i++;
	  scs_i++;
	}
	break;
      }
      assert(scs_i < scs.size() && scs_i < _nsucc);
      if (scs[scs_i].ip == gpr.v[gpr_i].ip) {
	loctable->add_node(scs[scs_i],true);
	gpr_i++;
	scs_i++;
      } else if (ConsistentHash::between(me.id,gpr.v[gpr_i].id,scs[scs_i].id)) {
	//delete the successors that my successor failed to pass to me
	CDEBUG(3) << "fix_successor_list del " << scs_i << " " << gpr_i << " " 
	  << scs[scs_i].ip << "," << printID(scs[scs_i].id) << gpr.v[gpr_i].ip 
	  << printID(gpr.v[gpr_i].id) << endl;
	loctable->del_node(scs[scs_i],true); //force delete
	scs_i++;
      } else {
	loctable->add_node(gpr.v[gpr_i], true);
	gpr_i++;
      }
    }

    if (vis) {
      bool change = false;

      scs = loctable->succs(me.id + 1, _nsucc);
      for (uint i = 0; i < scs.size (); i++) {
	if ((i >= lastscs.size ()) || lastscs[i].id != scs[i].id) {
	  change = true;
	}
      }
      if (change) {
	printf ( "vis %llu succlist %16qx", now (), me.id);
	for (uint i = 0; i < scs.size (); i++) {
	  printf ( " %16qx", scs[i].id);
	}
	printf ( "\n");
      }
      lastscs = scs;
    }
  }
}


void
Chord::notify_handler(notify_args *args, notify_ret *ret)
{
  assert(!static_sim);
  loctable->add_node(args->me);
}


void
Chord::alert_handler(alert_args *args, void *ret)
{
  assert(!static_sim);
  if (vis)
    printf ("vis %llu delete %16qx %16qx\n", now (), me.id, args->n.id);

  //due to non-transitivity problem and avoid long timeout during lookup
  //i check before i delete, 
  //this will make find_successor() slower, but fortunately iterative lookup is async
  bool b = failure_detect(args->n, &Chord::null_handler,(void *)NULL, &(args->n), TYPE_MISC,0,0,TIMEOUT_RETRY-1);
  if (!alive()) return;
  if (!b) {
   loctable->del_node(args->n); 
   CDEBUG(3) << "alert_handler del " << args->n.ip << "," 
     << printID(args->n.id) << endl;
  }else{
    record_stat(args->n.ip,me.ip,TYPE_MISC,0,0);
    loctable->update_ifexists(args->n);
  }
}

void
Chord::get_predsucc_handler(get_predsucc_args *args, get_predsucc_ret *ret)
{
  assert(!static_sim);
  ret->dst = me;

  if (args->pred)
    ret->n = loctable->pred(me.id-1);
  else
    ret->n.ip = 0;

  if (args->m > 0)
    ret->v = loctable->succs(me.id+1,args->m,LOC_HEALTHY);

}

void
Chord::dump()
{
  _isstable = true;

  printf("myID is %16qx %5u\n", me.id, me.ip);
  printf("===== %16qx =====\n", me.id);
  printf ("ring size %d\n", loctable->size ());

  vector<IDMap> v = loctable->succs(me.id+1, _nsucc);
  for (unsigned int i = 0; i < v.size(); i++) {
    printf("%16qx: succ %5d : %16qx\n", me.id, i, v[i].id);
  }
  IDMap p = loctable->pred(me.id-1);
  printf("pred is %5u,%16qx\n", p.ip, p.id);
}

void
Chord::leave(Args *args)
{
  assert(!static_sim);
  crash (args);
  loctable->del_all();
  ChordObserver::Instance(NULL)->delnode(me);
}

void
Chord::crash(Args *args)
{
  ChordObserver::Instance(NULL)->delnode(me);
  if (vis)
    printf ("vis %llu crash %16qx\n", now (), me.id);

  if ((Node::collect_stat()) && (now()-_last_join_time>600000)){
    uint rsz = loctable->size(LOC_HEALTHY,0.0);
    rtable_sz.push_back(rsz);
  }
  // XXX: Thomer says: not necessary
  //node()->crash ();
  _inited = false;
  loctable->del_all();
  CDEBUG(1) << "crashed " << endl;
  notifyObservers((ObserverInfo *)"crash");
  if (_learn) {
    learntable->del_all();
  }
}


/*************** LocTable ***********************/

#define MINTIMEOUT 30000

LocTable::LocTable()
{
  _evict = false;
} 

void LocTable::init(Chord::IDMap m)
{
  ring.repok();
  me = m;
  idmapwrap *elm = New idmapwrap(me);
  ring.insert(elm);
  full = me.id+1;
  lastfull = 0;
}

void
LocTable::del_all()
{
  assert (ring.repok ());
  idmapwrap *next;
  idmapwrap *cur;
  for (cur = ring.first(); cur; cur = next) {
    next = ring.next(cur);
    ring.remove(cur->id);
    bzero(cur, sizeof(*cur));
    delete cur;
  }
  assert (ring.repok ());
}

LocTable::~LocTable()
{
  del_all();
}

LocTable::idmapwrap *
LocTable::get_naked_node(ConsistentHash::CHID id)
{
  if (id == me.id) 
    return NULL;
  else
    return ring.search(id);
}

//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id, int status)
{
  vector<Chord::IDMap> v = succs(id, 1, status);
  uint vsz = v.size();
  if (vsz > 0) {
    return v[0];
  }
  Chord::IDMap tmp;
  tmp.ip = 0;
  return tmp;
}

/* returns m successors including or after the number id
 */
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m, int status)
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());

  if (m <= 0) return v;

  idmapwrap *ptr = ring.closestsucc(id);
  assert(ptr);
  idmapwrap *ptrnext;

  uint j = 0;
  while (j < m) {
    if (ptr->n.ip == me.ip) break;

    ptrnext = ring.next(ptr);
    if (!ptrnext) ptrnext = ring.first();

    if (ptr->status <= status) {

      if ((id == (me.id+1)) && (!ptr->is_succ)) return v;
      if (v.size() > 0) 
	assert(ptr->n.ip != v[v.size()-1].ip);
      v.push_back(ptr->n);
      j++;
      if (j >= ring.size()) return v;
    }
    ptr = ptrnext;
  }
  return v;
}

Chord::IDMap
LocTable::pred(Chord::CHID id, int status)
{
  vector<Chord::IDMap> v = preds(id, 1, status);
  if (v.size() > 0) {
    return v[0];
  }

  Chord::IDMap tmp;
  tmp.ip = 0;
  return tmp;
}

vector<Chord::IDMap>
LocTable::preds(Chord::CHID id, uint m, int status, double to) 
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());
  idmapwrap *elm = ring.closestpred(id);

  assert(elm);
  idmapwrap *elmprev;
  while (elm->id!=me.id) {
    if ((elm->status <= status) && 
	(to < 0.0000001 || elm->is_succ 
	 || ((now()-elm->n.timestamp) < MINTIMEOUT)
	 || ((double)elm->n.alivetime/(double)(elm->n.alivetime+now()-elm->n.timestamp)) > to)) { 
      v.push_back(elm->n);
      if (v.size() == m) break;
    }
    elm = ring.prev(elm);
    if (!elm) elm = ring.last();
  }

  if (v.size() < m && to > 0.00000001) {
    elm = ring.closestpred(id);
    while (elm->id != me.id) {
      if ((elm->status <= status) && ((double)(elm->n.alivetime+now()-elm->n.timestamp)) < to){
	v.push_back(elm->n);
	if (v.size() == m) break;
      }
      elm = ring.prev(elm);
      if (!elm) elm = ring.last();
    }
  }
  return v;
}

/*
vector<Chord::IDMap>
LocTable::preds(Chord::CHID id, uint m, int status, Time expire) 
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());
  idmapwrap *elm = ring.closestpred(id);

  assert(elm);
  idmapwrap *elmprev;
  while (elm->id!=me.id) {
    if ((elm->status <= status) && 
	(!expire || elm->is_succ || (now()-elm->n.timestamp) < expire)) { 
      v.push_back(elm->n);
      if (v.size() == m) break;
    }
    elm = ring.prev(elm);
    if (!elm) elm = ring.last();
  }

  if (v.size() < m && expire) {
    elm = ring.closestpred(id);
    while (elm->id != me.id) {
      if ((elm->status <= status) && (now()-elm->n.timestamp) > expire){
	v.push_back(elm->n);
	if (v.size() == m) break;
      }
      elm = ring.prev(elm);
      if (!elm) elm = ring.last();
    }
  }
  return v;
}
*/
void
LocTable::print ()
{
  idmapwrap *m = ring.search(me.id);
  assert(m);                         
  printf ("ring:\n");
  idmapwrap *i = m;
  do {
    printf ("  %5u,%16qx\n", i->n.ip, i->n.id);
    i = ring.next(i);
    if (!i) i = ring.first();
  }while (i != m);
}

int
LocTable::add_check(Chord::IDMap n)
{
  idmapwrap *elm = ring.search(n.id);
  
  
  if (!elm) 
    return -1;

  if (elm->status <= LOC_HEALTHY) { 
    if (n.timestamp >= elm->n.timestamp) {
      elm->n.timestamp = n.timestamp;
      elm->status = LOC_ONCHECK;
      return LOC_ONCHECK;
    } else {
      return elm->status;
    }
  }else{
    return LOC_DEAD;
  }
}

bool 
LocTable::update_ifexists(Chord::IDMap n, bool replacement)
{
  idmapwrap *ptr = ring.search(n.id);
  if (!ptr) return false;
  ptr->n = n;
  ptr->n.timestamp = now();
  if (replacement)
    ptr->status= LOC_REPLACEMENT;
  else
    ptr->status = LOC_HEALTHY;
  return true;
}

bool
LocTable::add_node(Chord::IDMap n, bool is_succ, bool assertadd, Chord::CHID fs,Chord::CHID fe, bool replacement)
{
  Chord::IDMap succ1; 
  Chord::IDMap pred1; 

  if (!n.ip) {
    if (p2psim_verbose>=4)
      cout << " add_node wierd " << n.ip << endl;
    return false;
  }

  if (n.ip == me.ip)
    return false;

  if (vis) {
    succ1 = succ(me.id+1);
    pred1 = pred(me.id-1);
  }
  idmapwrap *elm = ring.closestsucc(n.id);
  if (elm && elm->id == n.id) {
    if (replacement && elm->status == LOC_HEALTHY)
	elm->status = LOC_REPLACEMENT;
    if (n.timestamp > elm->n.timestamp) {
      elm->n = n;
      if (replacement)
	elm->status = LOC_REPLACEMENT;
      else
	elm->status = LOC_HEALTHY;
    }
    if (is_succ) {
      elm->fs = elm->fe = 0;
      elm->is_succ = is_succ;
    }
    elm->fs = fs;
    elm->fe = fe;
    return false;
  } else {
    idmapwrap *newelm = New idmapwrap(n);
    if (elm && elm->is_succ) {
      newelm->is_succ = true;
    } else {
      newelm->is_succ = is_succ;
      if (is_succ) {
	elm = ring.prev(elm);
	if (!elm) elm = ring.last();
	while (elm->n.ip!=me.ip && !elm->is_succ) {
	  elm->is_succ = true;
	  elm = ring.prev(elm);
	  if (!elm) elm = ring.last();
	}
      }
    }
    newelm->fs = fs;
    newelm->fe = fe;
    newelm->status = replacement? LOC_REPLACEMENT:LOC_HEALTHY;
    if (ring.insert(newelm)) {
    }else{
      abort();
    }
    return true;
  }
}

bool
LocTable::del_node(Chord::IDMap n, bool force)
{
  assert(n.ip != me.ip);
  idmapwrap *elm = ring.search(n.id);
  if (!elm) return false;
  if (!force) {
    if (n.timestamp > elm->n.timestamp)
      elm->n.timestamp = n.timestamp;
    elm->status = LOC_DEAD;
  } else {
    elm = ring.remove(n.id);
    bzero(elm,sizeof(*elm));
    delete elm;
  }
  assert (ring.repok ());
  return true;
}

uint
LocTable::size(uint status, double to)
{
  if (status == LOC_DEAD)
    return ring.size();
  idmapwrap *elm = ring.first();
  uint sz = 0;
  while (elm) {
    double ti = (double)elm->n.alivetime/(double)(now()-elm->n.timestamp+elm->n.alivetime);
    if ((elm->status <= status) && (to<0.0000001 
	  || elm->is_succ 
	  || (!elm->n.alivetime)
	  || ((now()-elm->n.timestamp) < MINTIMEOUT)
	  || (ti> to))) {
      sz++;
    }
    elm = ring.next(elm);
  }
  return sz;
}


vector<Chord::IDMap>
LocTable::next_hops(Chord::CHID key, uint nsz)
{
  return preds(key, nsz, LOC_HEALTHY);
}

Chord::IDMap
LocTable::next_hop(Chord::CHID key)
{
  vector<Chord::IDMap> v = next_hops(key, 1);

  if (v.size() == 0) 
    return me;

  return v[0];
}

vector<Chord::IDMap>
LocTable::get_all(uint status)
{
  vector<Chord::IDMap> v;
  v.clear();

  idmapwrap *currp; 
  currp = ring.first();
  while (currp) {
    if (currp->status <= status) {
      v.push_back(currp->n);
    }
    currp = ring.next(currp);
  }
  return v;
}

//for debugging purpose
Chord::IDMap
LocTable::first()
{
  idmapwrap *elm = ring.first();
  return elm->n;
}

Chord::IDMap
LocTable::last()
{
  idmapwrap *elm = ring.last();
  return elm->n;
}

Chord::IDMap
LocTable::search(ConsistentHash::CHID id)
{
  idmapwrap *elm = ring.search(id);
  assert(elm);
  return elm->n;
}

void
LocTable::last_succ(Chord::IDMap n)
{
  if (n.ip == me.ip)
    return;
  idmapwrap *elm = ring.closestsucc(n.id+1);
  while ((elm->is_succ) && (me.ip != elm->n.ip)){
    elm->is_succ = false;
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  if (me.ip == elm->n.ip)
    elm->is_succ = false;
}

bool
LocTable::is_succ(Chord::IDMap n)
{
  idmapwrap *elm = ring.search(n.id);
  return (elm && elm->is_succ);
}

uint
LocTable::succ_size()
{
  idmapwrap *elm = ring.first();
  uint n = 0;
  while (elm) {
    if ((elm->is_succ) && (elm->status <= LOC_HEALTHY))
      n++;
    elm = ring.next(elm);
  }
  return n;
}

uint
LocTable::live_size(double to)
{
  idmapwrap *elm = ring.first();
  uint n = 0;
  while (elm) {
    if (elm->status <= LOC_HEALTHY && elm->n.ip && Network::Instance()->alive(elm->n.ip)
	&& (to<0.0000001 
	  || elm->is_succ 
	  || !elm->n.alivetime
	  || (now()-elm->n.timestamp < MINTIMEOUT)
	  || ((double)elm->n.alivetime/(double)(now()-elm->n.timestamp+elm->n.alivetime)>to)))
      n++;
    elm = ring.next(elm);
  }
  return n;
}

int
LocTable::find_node(Chord::IDMap n)
{
  idmapwrap *elm = ring.closestsucc(n.id);
  if (elm->n.ip == n.ip && elm->n.timestamp >= n.timestamp) 
    return elm->status;
  else 
    return -1;
}

void
LocTable::dump()
{
  printf("===%u,%qx loctable dump at %llu===\n", me.ip, me.id,now());
  idmapwrap *elm = ring.closestsucc(me.id+1);
  while (elm->n.ip != me.ip) {
    printf("(%qx, %u, %d, %llu)\n", elm->n.id, elm->n.ip, elm->is_succ, elm->n.timestamp);
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  
}
void
LocTable::stat()
{
  Time t = now();
  Chord::IDMap succ;
  succ.ip = 0;
  uint num_succ = 0;
  uint num_finger = 0;
  idmapwrap *elm = ring.closestsucc(me.id+1);
  while (elm->n.ip != me.ip) {
    if (!succ.ip) succ = elm->n;
    if (elm->is_succ) 
      num_succ++;
    else
      num_finger++;
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  printf("%llu loctable stat for (%u,%qx): succ %u,%qx number of succs %u number of finger %u\n", 
      t, me.ip, me.id, succ.ip, succ.id, num_succ, num_finger);
}

void
LocTable::rand_sample(Chord::IDMap &askwhom, Chord::IDMap &start, Chord::IDMap &end)
{
  double x = random()/(double)(RAND_MAX);
  ConsistentHash::CHID samp = (ConsistentHash::CHID) (x * (ConsistentHash::CHID)(-1));
  idmapwrap *elm = ring.closestsucc(samp);
  idmapwrap *elmtmp = elm;

  while (elm->status > LOC_HEALTHY) {
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  start = elm->n;
  do {
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  } while (elm->status > LOC_HEALTHY);
  end = elm->n;

  askwhom = start;
  //XXX jinyang: this is an ugly hack, budget should be included as part of the IDMap field
  uint prev_budget = ((Accordion *)Network::Instance()->getnode(askwhom.ip))->budget();
  ConsistentHash::CHID gap = ConsistentHash::distance(start.id,end.id);
  uint i = 0;
  while (i < 10) {
    elmtmp = ring.prev(elmtmp);
    if (!elmtmp) elmtmp = ring.last();
    if (elmtmp->n.ip == me.ip)
      break;
    if (elmtmp->status <= LOC_HEALTHY) {
      double r1 = 1.0 + (double)ConsistentHash::distance(askwhom.id,elmtmp->n.id)/(double)gap; 
      double r2 = (double)prev_budget/(double)((Accordion *)Network::Instance()->getnode(elmtmp->n.ip))->budget();
      if (r1 < r2) {
	askwhom = elmtmp->n;
	prev_budget = ((Accordion *)Network::Instance()->getnode(elmtmp->n.ip))->budget();
      }
      i++;
    }
  }
  uint my_budget = ((Accordion *)Network::Instance()->getnode(me.ip))->budget();
  if (my_budget > prev_budget) {
    double rr = (double)random()/(double)RAND_MAX;
    if (rr > ((double)prev_budget/(double)my_budget)) 
      askwhom = me; //don't send this exploration packet
  }
}

uint
LocTable::sample_smallworld(uint est_n, Chord::IDMap &askwhom, Chord::IDMap &start, Chord::IDMap &end, double tt, ConsistentHash::CHID mingap)
{
  //use LOC_REPLACEMENT 
  //to denote that the finger's succeeding gap is full
  uint op = 0;
  if (lastfull && (now()-lastfull) < 2*MINTIMEOUT) {
    rand_sample(askwhom,start,end);
  }else{
    //get a good sample
    double x = random()/(double)(RAND_MAX);
    ConsistentHash::CHID samp = me.id+((ConsistentHash::CHID)(-1)/(ConsistentHash::CHID)pow(est_n,1-x));
    idmapwrap *elm = ring.closestsucc(samp);
    if (elm->n.ip == me.ip) {
      elm = ring.next(elm);
      if (!elm) elm = ring.first();
    }

    uint lsz = 0;
    double ti;
    ConsistentHash::CHID gap = 0;
    start = end = me;
    op = 1;
    askwhom = elm->n;
    uint prev_budget = ((Accordion *)Network::Instance()->getnode(elm->n.ip))->budget();

    while ((gap <= mingap) && (elm->n.ip!=me.ip)) {
      do {
	ti = (double)elm->n.alivetime/(double)(now()-elm->n.timestamp + elm->n.alivetime);
	if (elm->n.ip == me.ip) {
	  askwhom = me;
	  lastfull = now();
	  return op;
	}
	elm = ring.next(elm);
	if (!elm) elm = ring.first();

	if (elm->status <= LOC_HEALTHY) {
	  double r1 = 1.0+(double)ConsistentHash::distance(askwhom.id,elm->n.id)/(double)mingap; 
	  double r2 = (double)prev_budget/(double)((Accordion *)Network::Instance()->getnode(elm->n.ip))->budget();
	  if ((r1 >= r2) || (askwhom.alivetime < elm->n.alivetime && elm->n.alivetime > 3*MINTIMEOUT)){
	    askwhom = elm->n;
	    prev_budget = ((Accordion *)Network::Instance()->getnode(elm->n.ip))->budget();
	  }
	}

	if ((elm->status <=LOC_HEALTHY) && (ti > tt || (now()-elm->n.timestamp < MINTIMEOUT)))
	  lsz++;
	op++;
      }while (elm->is_succ || (elm->status==LOC_REPLACEMENT) || (elm->status >= LOC_DEAD));

      start = elm->n;
      while (elm->n.ip!=me.ip) {
	if ((elm->status <=LOC_HEALTHY) && (ti > tt || (now()-elm->n.timestamp < MINTIMEOUT)))
	  lsz++;
	elm = ring.next(elm);
	if (!elm) elm = ring.first();
	ti = (double)elm->n.alivetime/(double)(now()-elm->n.timestamp + elm->n.alivetime);
	if (!elm->is_succ && elm->status <= LOC_HEALTHY
	    && (ti > tt
	      || (now()-elm->n.timestamp < MINTIMEOUT))
	      || (!elm->n.alivetime))
	  break;
	op++;
      }
      end = elm->n;
      gap = ConsistentHash::distance(start.id,end.id);
    }
    if (op >= 0.5 * size() && lsz >= 0.2*est_n)
      lastfull = now();
    else
      lastfull = 0;
  }
  return op;
}

double
LocTable::pred_biggest_gap(Chord::IDMap &start, Chord::IDMap &end, Time stabtime, double to)
{
  idmapwrap *elm = ring.closestsucc(me.id+1);
  Chord::IDMap mysucc = succ(me.id+1);
  assert(elm);
  idmapwrap *prev = ring.search(me.id);
  Time prevelapsed = 0;
  Chord::IDMap maxgap_n = me;
  Chord::IDMap maxgap_end = elm->n;
  double maxgap = 0.0;
  Time t = now();
  Chord::IDMap oldn;
  Chord::IDMap myprecious;
  myprecious.ip = 0;
  double oldti = 1;
  double ti;
  idmapwrap *elmdel;
  while (1) {
    ti = ((double)elm->n.alivetime/(double)(elm->n.alivetime+t-elm->n.timestamp));
    /* i need to think more carefully about when to get rid of old nodes
    while (elm->status > LOC_HEALTHY) {
      if (ti >= 0.5)
	break; //safe to evict this node
      elmdel = elm;
      elm = ring.next(elm);
      if (!elm) elm = ring.first();
      ring.remove(elmdel->id);
      delete elmdel;
      ti = ((double)elm->n.alivetime/(double)(elm->n.alivetime+t-elm->n.timestamp));
    }
    */
    if (elm->n.ip!=me.ip && elm->status <= LOC_HEALTHY) {
      if (!oldn.ip ||  (elm->n.alivetime && ti < to && ti > oldti))
      oldti = ti;
      oldn = elm->n;
    }
    if (elm->status <= LOC_HEALTHY) {
      //if (((now()-prev->n.timestamp) < stabtime) && (prev->n.alivetime) && (prev->follower))
      if (((now()-prev->n.timestamp) < stabtime) && (prev->n.alivetime) && (prev->status!=LOC_REPLACEMENT))
	prev = elm;
    }

    if ((elm->status <= LOC_HEALTHY) && 
	(to<0.000000001 
	 || (!elm->n.alivetime && now()-elm->n.timestamp < stabtime)
	 || (now()-elm->n.timestamp < MINTIMEOUT)
	 || elm->n.ip == me.ip || ti > to)) {
      if (prev->n.ip!=me.ip && elm->n.ip!=mysucc.ip){
	ConsistentHash::CHID gap = ConsistentHash::distance(prev->n.id,elm->n.id);
	ConsistentHash::CHID dist = ConsistentHash::distance(me.id,prev->n.id);
	double scaled = (double)gap/(double)dist;
	//if ((scaled > maxgap) && (gap > prev->follower)) {
	if (scaled > maxgap) {
	  maxgap = scaled;
	  maxgap_n = prev->n;
	  maxgap_end = elm->n;
	}
      }
      prev = elm;
    }
    if (elm->n.ip == me.ip) 
      break;
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  /*
  Chord::IDMap closest_n = maxgap_n;
  elm = ring.closestsucc(maxgap_n.id+1);
  Topology *topo = Network::Instance()->gettopology();
  Time closest = topo->latency(me.ip,closest_n.ip);
  while (elm->n.ip!=maxgap_end.ip) {
    ti =  ((double)elm->n.alivetime/(double)(elm->n.alivetime+t-elm->n.timestamp));
    if (elm->status <= LOC_HEALTHY && topo->latency(me.ip,elm->n.ip) < closest && (ti > (to/2.0))) {
      closest_n = elm->n;
      closest = topo->latency(me.ip,elm->n.ip);
    }
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  start = closest_n;
  */
  start = maxgap_n;
  end = maxgap_end;
  if (start.ip==me.ip) {
    start = oldn;
    elm = ring.closestsucc(start.id+1);
    while (elm->status > LOC_HEALTHY){
      elm = ring.next(elm);
      if (!elm) elm = ring.first();
    }
    end = elm->n;
  }

  return oldti;
}

vector<Chord::IDMap>
LocTable::get_closest_in_gap(uint m, ConsistentHash::CHID start, ConsistentHash::CHID end, Chord::IDMap src, Time stabtime, double to)
{
  vector<Chord::IDMap> v;
  vector<Chord::IDMap>::iterator i;
  vector<Time> lv;
  vector<Time>::iterator j;
  idmapwrap *elm = ring.closestsucc(start+1);
  assert(elm);
  double ti;
  Time lat;
  while (elm->n.ip!=me.ip && elm->id!=end && ConsistentHash::between(me.id,end,elm->id)) {
    ti = (double)elm->n.alivetime/(double)(now() - elm->n.timestamp+elm->n.alivetime);
    if (elm->status <= LOC_HEALTHY) {
      if ((to<0.000000001)
	|| (!elm->n.alivetime && (now()-elm->n.timestamp) < stabtime)
	|| ((now()-elm->n.timestamp) < MINTIMEOUT))
	ti = 1.0;
      Time ll = Network::Instance()->gettopology()->latency(src.ip,elm->n.ip);
      lat = (Time)(ti * ll + 3 * ll * (1-ti));
      for (i = v.begin(),j=lv.begin(); i!= v.end(); ++i,++j) {
	assert((*i).ip > 0);
	if (lat >= (*j)) 
	  break;
      }
      if ( i != v.begin() || v.size() < m) {
	v.insert(i, elm->n);
	lv.insert(j,lat);
      }
      if (v.size() > m) {
	v.erase(v.begin());
	lv.erase(lv.begin());
      }
    }
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
/*
  elm = ring.closestsucc(me.id+1);
  while ((v.size() < m) && (ConsistentHash::between(me.id,end,elm->n.id))){
    if ((elm->status <= LOC_HEALTHY) && (to && (now()-elm->n.timestamp > to)))
      v.push_back(elm->n);
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  */
  return v;
}

vector<Chord::IDMap>
LocTable::next_close_hops(ConsistentHash::CHID key, uint n, Chord::IDMap src, double to)
{
  idmapwrap *elm = ring.closestpred(key);
  Chord::IDMap mysucc = succ(me.id+1);

  Time t = now();
  Topology *topo = Network::Instance()->gettopology();
  double latest = 0.0;
  Chord::IDMap latest_n = me;
  vector<Chord::IDMap> l;
  vector<Chord::IDMap>::iterator il;
  vector<double> dl;
  vector<double>::iterator idl;
  double ti;
  uint i = 0;
  l.clear();
  ConsistentHash::CHID dist,mindist;
  double ndist;
  vector<Chord::IDMap> all;
  Chord::IDMap predpred;

  while (i < 8) {
    dist = ConsistentHash::distance(elm->n.id,key);
    ConsistentHash::CHID dist2 = ConsistentHash::distance(me.id,elm->n.id);
    if (!ConsistentHash::between(me.id,key,elm->n.id)
	|| (l.size()>=n && dist > dist2))
      break;
    else if (n==1 && dist > dist2 && latest_n.ip!=me.ip) {
      l.push_back(latest_n);
      break;
    }

    double ti = (double)elm->n.alivetime/(double)(t - elm->n.timestamp+elm->n.alivetime);
    if ((ti > latest) && (elm->status <= LOC_HEALTHY)){
      latest = ti;
      latest_n = elm->n;
    }

    if (elm->status<=LOC_HEALTHY && 
	(elm->is_succ 
	 || to<0.0000000001 
	 || ti >= to 
	 || (!elm->n.alivetime && now()-elm->n.timestamp < 800000))) { 
      all.push_back(elm->n);
      i++;
      if (i == 1) {
	mindist = dist;
	ndist = 1.0;
      }else
	ndist = (double)dist/(double)mindist;

      if ((n==1) && (i == n) && topo->latency(src.ip,elm->n.ip) < 0.8*topo->median_lat()) {
	l.push_back(elm->n);
	return l;
      }

      double delay = ndist * topo->latency(src.ip,elm->n.ip) /(double)((Accordion *)Network::Instance()->getnode(elm->n.ip))->budget();
      if (dist > dist2) {
	l.push_back(elm->n);
	dl.push_back(delay);
      } else {
	for (il = l.begin(),idl=dl.begin(); il != l.end(); ++il,++idl) {
	  if (delay > (*idl))
	  //if (topo->latency(src.ip,elm->n.ip)
	   //   >= (0.5*topo->latency(src.ip,(*iter).ip)))
	    break;
	}
	if (n > 1 && l.size() == 1) { //if parallelism >1, always choose one as the closest to key
	  l.insert(l.begin(),elm->n);
	  dl.insert(dl.begin(),delay);
	}else {
	  if (il!=l.begin() || l.size() < n)  {
	    l.insert(il,elm->n);
	    dl.insert(idl,delay);
	  }
	}
	if (l.size() > n) {
	  l.erase(l.begin());
	  dl.erase(dl.begin());
	}
      }
    }
    elm = ring.prev(elm);
    if (!elm) elm = ring.last();
  }
  if (p2psim_verbose >=5) {
    printf("%llu med %llu %u,%qx key %qx all : ",now(),topo->median_lat(),me.ip,me.id,key);
    for (uint i = 0; i < all.size(); i++) 
      printf(" (%u,%qx,%llu,%llu,%llu) ",all[i].ip,all[i].id,topo->latency(me.ip,all[i].ip),now()-all[i].timestamp,all[i].alivetime);
    printf("\n");
  }
  return l;
}

vector<Chord::IDMap>
LocTable::between(ConsistentHash::CHID start, ConsistentHash::CHID end, int status)
{
  idmapwrap *elm = ring.closestsucc(start);
  vector<Chord::IDMap> v;
  v.clear();
  while (elm->status <= status &&  elm->n.id != end && 
      (elm->n.id == start || ConsistentHash::between(start,end,elm->n.id))) {
    v.push_back(elm->n);
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  return v;
}

