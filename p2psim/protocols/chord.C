/*
 * Copyright (c) 2003 [Jinyang Li]
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
#include <stdio.h>
#include <assert.h>

extern bool vis;
bool static_sim;
unsigned int joins = 0;

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
  _recurs = a.nget<uint>("recurs",0,10);
  _recurs_direct = a.nget<uint>("recurs_direct",0,10);
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

  me.heartbeat = now();

  if (l) 
    loctable = l;
  else
    loctable = New LocTable();

  loctable->set_timeout(0);

  loctable->set_evict(false);

  loctable->init (me);
  
  
  //pin down nsucc for successor list
  loctable->pin(me.id + 1, _nsucc, 0);
  //pin down 1 predecessor
  loctable->pin(me.id - 1, 0, 1);

  if (vis) {
    printf ("vis %llu node %16qx\n", now (), me.id);
  }

  _stab_basic_running = false;

  _stab_basic_outstanding = 0;
  _join_scheduled = 0;
  _last_succlist_stabilized = 0;

}

void
Chord::record_stat(uint type, uint num_ids, uint num_else)
{
  if (Node::collect_stat()) {
    if (type == TYPE_USER_LOOKUP) {
      assert(num_ids < 20);
    }
    Node::record_bw_stat(type,num_ids,num_else);
  }

}

Chord::~Chord()
{
  if (me.ip == 1) { //same hack as tapestry.C so statistics only gets printed once

    Node::print_stats();
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
  sprintf(buf, "%llu %s(%u,%qx,%u)", now(), proto_name().c_str(), me.ip, me.id, me.heartbeat);
  return buf;
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

  while (1) {
    if (pos >= idsz) pos = 0;
    Chord *node = (Chord *)Network::Instance()->getnode(ids[pos].ip);
    if (Network::Instance()->alive(ids[pos].ip)
	&& node->inited())
      break;
    pos++;
  }

  for (uint i = 0; i < v.size(); i++) {
    if (ids[pos].ip == v[i].ip) {
      pos = (pos+1) % idsz;
    } else if (ConsistentHash::betweenrightincl(k,ids[pos].id,v[i].id)) {
#ifdef CHORD_DEBUG
      printf("%s lookup correct key %16qx succ should be (%u,%qx) \
	  instead of (%u, %qx) inited? %u\n", ts(), k, ids[pos].ip, 
	  ids[pos].id, v[0].ip, v[0].id, ((Chord *)getpeer(ids[pos].ip))->inited()?1:0);
#endif
    } else {
#ifdef CHORD_DEBUG
      printf("%s lookup incorrect key %16qx succ should be (%u,%qx) \
	  instead of (%u, %qx) i is %u\n", ts(), k, ids[pos].ip, 
	  ids[pos].id, v[i].ip, v[i].id, i);
#endif
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
    record_stat(type,num_args_id,num_args_else);
    r = doRPC(dst.ip, fn, args, ret, retry_to);
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
#ifdef CHORD_DEBUG
    printf("%s start lookuping up ipkey %u,%qx\n", ts(),a->ipkey,a->key);
#endif
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
#ifdef CHORD_DEBUG
    printf("%s key %qx lookup correct interval %u\n", ts(), a->key,a->latency);
#endif

  }else{
    if (_ipkey && a->retrytimes<=1 && (!Network::Instance()->alive(a->ipkey))) {
    }else if (_ipkey && (a->retrytimes >2 || Network::Instance()->alive(a->ipkey))) {
      record_lookup_stat(me.ip, lasthop.ip, a->latency, false, false, a->hops, a->num_to, a->total_to);
    }else{
#ifdef CHORD_DEBUG
      printf("%s key %qx lookup incorrect interval ipkey %u lastnode %u,%qx a->latency %u a->start %u\n", ts(), a->key, _ipkey?a->ipkey:0, lasthop.ip, lasthop.id, a->latency,a->start); 
#endif
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

#ifdef CHORD_DEBUG
  if (ret->v.size() > 0) 
    printf("%s find_successors_handler key %qx succ %u,%qx\n",ts(),args->key,ret->v[0].ip,ret->v[0].id);
#endif
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
#ifdef CHORD_DEBUG
  
  vector<hop_info> recorded;
  recorded.clear();
  printf("%s start debug key %qx query type %d\n", ts(), key, type);
#endif

  while (1) {
#ifdef CHORD_DEBUG
    if ((totalrpc >= 100) || (na.deadnodes.size()>=20)) {
      fprintf(stderr,"%s route: key %qx\n",ts(),key);
      for (uint i = 0; i < recorded.size(); i++) 
	fprintf(stderr,"(%u,%qx,%u) ", recorded[i].to.ip, recorded[i].to.id, recorded[i].hop);
      fprintf(stderr,"\n");
      fprintf(stderr,"deadnodes: ");
      for (uint i = 0; i < na.deadnodes.size(); i++) {
	fprintf(stderr,"<%u,%qx,%u> ",na.deadnodes[i].ip,na.deadnodes[i].id,na.deadnodes[i].heartbeat);
      }
      fprintf(stderr,"\n");
      assert(0);
    }
#endif
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
#ifdef CHORD_DEBUG
	printf("%s debug key %qx timeout, resume to node %u,%qx\n",ts(), key, lastfinished.to.ip, lastfinished.to.id);
#endif
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
#ifdef CHORD_DEBUG
      recorded.push_back(h);
      printf("%s key %qx sending to next hop node (%u,%qx,%u rpc %u)\n",ts(), key, h.to.ip,h.to.id,h.to.heartbeat,rpc_i);
#endif
      record_stat(type,1+na.deadnodes.size(),0);
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
      record_stat(type,reuse->ret.done?reuse->ret.next.size():reuse->ret.v.size()); //counting for heartbeat timer
      if (reuse->link.from.ip == me.ip) {
	assert(reuse->ret.dst.ip == reuse->link.to.ip);
	loctable->update_ifexists(reuse->ret.dst); //update the timestamp if node contacted exists in my own routing table as well as heartbeat timer
      }
#ifdef CHORD_DEBUG
      printf("%s debug key %qx, outstanding %d deadsz %d from (%u,%qx,%u rpc %u) done? %d nextsz %d savefinishedsz %u\n", ts(), key, outstanding, na.deadnodes.size(),
	  reuse->link.to.ip, reuse->link.to.id, reuse->link.to.heartbeat, rpc_i,
	  reuse->ret.done? 1:0, reuse->ret.next.size(), savefinished.size());
#endif

      if ((reuse->link.from.ip == me.ip) && (!static_sim))
	loctable->update_ifexists(reuse->link.to); //update timestamp of MY neighbors

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
#ifdef CHORD_DEBUG
      printf("%s debug key %qx, from (%u,%qx,%u rpc %u) next? (%u,%qx,%u) task top (%u,%qx,%u) tasksz %d lastfinished %u,%qx\n",
	  ts(),key, reuse->link.to.ip, reuse->link.to.id, reuse->link.to.heartbeat, rpc_i,
	  reuse->ret.next.size()>0?reuse->ret.next[0].ip:0, 
	  reuse->ret.next.size()>0?reuse->ret.next[0].id:0,
	  reuse->ret.next.size()>0?reuse->ret.next[0].heartbeat:0,
	  tasks.size()>0?tasks.front().to.ip:0, tasks.size()>0?tasks.front().to.id:0,
	  tasks.size()>0?tasks.front().to.heartbeat:0,
	  tasks.size(),
	  lastfinished.to.ip, lastfinished.to.id);
#endif
    } else {

      if (reuse->link.to.ip == me.ip) {
	//a very special wierd case, the node has gone down and up during the RPC
	goto DONE;
      }

#ifdef CHORD_DEBUG
      printf("%s debug key %qx oustanding %d deadsz %d tasksz %d from (%u,%qx,%u) DEAD, savefinishedsz %u lastfinished %u\n", ts(), key, outstanding, na.deadnodes.size(), tasks.size(),
	  reuse->link.to.ip, reuse->link.to.id, reuse->link.to.heartbeat, savefinished.size(), lastfinished.to.ip);
#endif
  
      if (reuse->link.from.ip == me.ip) {
	if (_learn)
	  to_be_replaced.push_back(reuse->link.to);
      }

      //notify
      alert_args *aa = New alert_args;
      aa->n = reuse->link.to;
      record_stat(type,1);
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
#ifdef CHORD_DEBUG
	printf("%s lastfinished %u dead new last finished %u\n",ts(),lastfinished.to.ip, savefinished.back().to.ip);
#endif
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
#ifdef CHORD_DEBUG
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
#endif
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
      record_stat(type,reuse->ret.done?reuse->ret.next.size():reuse->ret.v.size());
  }

  for (uint i = 0; i < parallel; i++) {
    delete rpcslots[i];
  }

  for (uint i = 0;i < alertoutstanding; i++) {
    donerpc = rcvRPC(&alertset, ok);
    if (ok)
      record_stat(type,0);
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
  nexthop.heartbeat = 0;
  outstanding = 0;

  assert(!_stopearly_overshoot || _parallel == 1); //i do not know how to do parallel lookup with stopearly and overshoot

  while (1) {

    if (!alive()) return results;
    IDMap succ = loctable->succ(me.id+1,LOC_HEALTHY);
    if (succ.ip == 0) {
      if (!_join_scheduled) {
	_join_scheduled++;
#ifdef CHORD_DEBUG
	printf("%s joincrash rejoin incorrect key %qx schedule rejoin\n", ts(), key); 
#endif
	delaycb(0, &Chord::join, (Args *)0);
      }
      if (lasthop) *lasthop = me;
      return results;
    }

    while (outstanding < parallel) {
      if (_stopearly_overshoot) 
	nexthop = me;
      else {
	nexthop = loctable->next_hop(nexthop.id-1);
	assert(nexthop.ip >= 0 && nexthop.ip <10000);
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

#ifdef CHORD_DEBUG
      printf("%s start lookuping up key %u,%qx via nexthop <%u,%qx,%u> outstanding %d parallel %u\n",ts(),a?a->ipkey:0,key,nexthop.ip,nexthop.id,nexthop.heartbeat,outstanding,parallel);
#endif
      record_stat(type,1);
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
      record_stat(type,resultmap[donerpc]->v.size());
      goto RECURS_DONE;
    }else{
      //do a long check to see if next hop is really dead
      assert(reuse->path.size()>0);
      IDMap n = reuse->path[reuse->path.size()-1].n;
#ifdef CHORD_DEBUG
      printf("%s key ipkey %u key %qx nexthop <%u,%qx,%u> failed outstanding %d parallel %u\n",ts(),a?a->ipkey:0,key,n.ip,n.id,n.heartbeat,outstanding,parallel);
#endif
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
#ifdef CHORD_DEBUG
  printf("%s finished lookuping up key %qx via lasthop <%u,%qx,%u> v.size %u correct? %d route(%u):",ts(),key, reuse->lasthop.ip,reuse->lasthop.id, reuse->lasthop.heartbeat, reuse->v.size(), reuse->correct?1:0,reuse->path.size());
  Time x = 0;
  Topology *t = Network::Instance()->gettopology();
#endif

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
#ifdef CHORD_DEBUG
	x += TIMEOUT(prev.ip,reuse->path[j].n.ip);
#endif
      }else{
#ifdef CHORD_DEBUG
	if (_recurs_direct)
	  x += t->latency(prev.ip,reuse->path[j].n.ip);
	else
	  x += 2*t->latency(prev.ip,reuse->path[j].n.ip);
#endif
	prev = reuse->path[j].n;
      }
#ifdef CHORD_DEBUG
      printf(" (%u,%qx,%u %u %u)",reuse->path[j].n.ip,reuse->path[j].n.id,reuse->path[j].n.heartbeat,reuse->path[j].tout, x);
#endif

    }
  }
#ifdef CHORD_DEBUG
  printf("\n");
#endif

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
      record_stat(type,resultmap[donerpc]->v.size());
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

#ifdef CHORD_DEBUG
  printf("%s lookup key %u,%qx arrived with path length %u src %u\n",ts(),args->ipkey,args->key,ret->path.size(),args->src.ip);
#endif

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
#ifdef CHORD_DEBUG
      printf("%s joincrash rejoin incorrect key %qx schedule rejoin\n", ts(), args->key); 
#endif
	delaycb(0, &Chord::join, (Args *)0);
      }
      ret->correct = false;
      ret->lasthop = me;
      if (args->type==TYPE_USER_LOOKUP && _recurs_direct) {
	record_stat(args->type,ret->v.size(),0);
	assert(0);
	r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	if (r) 
	  record_stat(args->type,0);
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
#ifdef CHORD_DEBUG
	if (!ret->correct)
	  printf("%s incorrect key %qx, succ %u,%qx\n",ts(), args->key, succs[0].ip, succs[0].id);
#endif
      } else
	ret->correct = true;

      ret->lasthop = me;
      if (_recurs_direct) {
	record_stat(args->type,ret->v.size(),0);
	r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	if (r) 
	  record_stat(args->type,0);
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
	  record_stat(args->type,1,0); 
	  assert(ret->v.size() >= args->m && args->m >= 7);
	  r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	  if (r) 
	    record_stat(args->type,0);
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
	    record_stat(args->type,1,0);
	    assert(ret->v.size() >= args->m);
	    r = doRPC(args->src.ip, &Chord::final_recurs_hop, args, ret);
	    if (r)
	      record_stat(args->type,0);
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
	  printf("(%u,%qx,%u %u) ", ret->path[i].n.ip, ret->path[i].n.id, ret->path[i].n.heartbeat,ret->path[i].tout);
	}
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

    record_stat(args->type,1);
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
#ifdef CHORD_DEBUG
      printf("%s lost lookup request %qx\n", ts(), args->key);
#endif
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
	record_stat(args->type,ret->v.size());
      }else{
	record_stat(args->type,0);
      }
      assert(ret->nexthop.ip==next.ip);
      if (!static_sim) loctable->update_ifexists(ret->nexthop); //update timestamp
      ret->nexthop = me;
      return;
    }else{
      #ifdef CHORD_DEBUG
      printf ("%s %u key %qx next hop to %u,%16qx failed\n", ts(), args->type, args->key,next.ip, next.id);
#endif
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
#ifdef CHORD_DEBUG
  printf("%s null_handler my heartbeat %u\n",ts(),me.heartbeat);
#endif
  ret->heartbeat = me.heartbeat;
  return;
}

void
Chord::alert_delete(alert_args *aa)
{
  if (aa->dst != me.ip) 
    record_stat(TYPE_MISC,1);
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
#ifdef CHORD_DEBUG
    printf("%s incorrect key %qx coz of new join succsz %d oncheck succsz %d deadsz %d last join time %llu join scheduled %d\n", 
	ts(), args->key, succs.size(), tmp.size(), args->deadnodes.size(), now()-_last_join_time, _join_scheduled);
#endif
    ret->correct = false;
    //rejoin baby
    if ((!_join_scheduled) && (tmp.size()==0)){
#ifdef CHORD_DEBUG
      printf("%s during lookup key %qx rejoin (succ on check): ",ts(),args->key);
      for (uint i = 0; i < tmp.size(); i++) {
	printf("<%u,%qx,%u> ",tmp[i].ip,tmp[i].id,tmp[i].heartbeat);
      }
      printf("\n");
#endif
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
#ifdef CHORD_DEBUG
      if (!ret->correct) 
	printf("%s incorrect key %qx, succ %u,%qx\n",ts(), args->key, succs[s].ip, succs[s].id);
#endif
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
    me.heartbeat = now();

    loctable->init(me);
    if (_learn) 
      learntable->init(me);

    _last_join_time = now();
    ChordObserver::Instance(NULL)->addnode(me);
    _join_scheduled++;
    // XXX: Thomer says: not necessary
    // node()->set_alive(); //if args is NULL, it's an internal join
#ifdef CHORD_DEBUG
    if (me.ip == DNODE) {
      fprintf(stderr,"%s start to join\n", ts());
    }
    printf("%s (joincrash) start to join inited? %u basic stab running %u\n",ts(), _inited?1:0, _stab_basic_running?1:0);
#endif
  }else{

    if (!alive()) {
      _join_scheduled--;
      return;
    }
#ifdef CHORD_DEBUG
    if (me.ip == DNODE) {
      fprintf(stderr,"%s rescheduled join\n", ts());
    }
    printf("%s rescheduled join inited? %u\n", ts(), _inited?1:0);
#endif
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

#ifdef CHORD_DEBUG
  Time before = now();
#endif

  bool ok = failure_detect(_wkn, &Chord::find_successors_handler, &fa, &fr, TYPE_JOIN_LOOKUP,1,0);
  assert(_wkn.ip == fr.dst.ip);
  _wkn = fr.dst;
  joins++;
  if (ok) record_stat(TYPE_JOIN_LOOKUP, fr.v.size());

  if (!alive()) {
    _join_scheduled--;
    return;
  }

  if (!ok || fr.v.size() < 1) {
#ifdef CHORD_DEBUG
    printf("%s joincrash join failed key %qx another rejoin rescheduled? %u\n", ts(),fa.key, _join_scheduled);
#endif
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
  }

  if (fr.v.size()>1) 
    _last_succlist_stabilized = now();

#ifdef CHORD_DEBUG
  Time after = now();
  if (me.ip == DNODE) 
    fprintf(stderr,"%s joined succ %u,%16qx elapsed %llu stab running %d _inited %u\n",
         ts(), fr.v[0].ip, fr.v[0].id,
         after - before, _stab_basic_running?1:0, _inited?1:0);
  IDMap succ = loctable->succ(me.id+1,LOC_ONCHECK);
  printf("%s joincrash joined key %qx elapsed %llu stab running %d _inited %u succ <%u,%qx,%u>: ",
         ts(), fa.key, after - before, _stab_basic_running?1:0, _inited?1:0, succ.ip,succ.id,succ.heartbeat);
  for (uint i = 0; i < fr.v.size(); i++) {
    printf("(%u,%qx,%u) ",fr.v[i].ip, fr.v[i].id,fr.v[i].heartbeat);
  }
  printf("\n");
#endif
 

  if (!_stab_basic_running) {
    _stab_basic_running = true;
    delaycb(0, &Chord::reschedule_basic_stabilizer, (void *) 0);
  }else{
    Chord::stabilize();
#ifdef CHORD_DEBUG
    printf("%s stabilization already running _inited %u\n",ts(), _inited?1:0);
#endif
  }

  if ((loctable->size() < 2)  && (alive())) {
#ifdef CHORD_DEBUG
    printf("%s after stabilize join failed! return %u another rejoin scheduled? %u\n", ts(), fr.v.size(), _join_scheduled);
    fprintf(stderr,"%s after stabilize join failed! return %u\n", ts(), fr.v.size());
#endif
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
#ifdef CHORD_DEBUG
  if (me.ip == DNODE) {
    printf("%s special treatment !!\n",ts());
  }
#endif
  assert(!static_sim);
  if (!alive()) {
    _stab_basic_running = false;
#ifdef CHORD_DEBUG
    printf("%s node dead cancel stabilizing\n",ts());
#endif
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

  IDMap pred1 = loctable->pred(me.id-1, LOC_ONCHECK);
  IDMap succ1 = loctable->succ(me.id+1, LOC_ONCHECK);
  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc, LOC_ONCHECK);
  if (!succ1.ip) return;

#ifdef CHORD_DEBUG
  Time before = now();
  printf("%s Chord stabilize BEFORE pred <%u,%qx,%u> succ <%u,%qx,%u> succsz %u _inited %u\n", ts(), pred1.ip, pred1.id, pred1.heartbeat, succ1.ip, succ1.id, succ1.heartbeat,succs.size(), _inited?1:0);
#endif

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

#ifdef CHORD_DEBUG
  IDMap pred = loctable->pred(me.id-1, LOC_ONCHECK);
  succs = loctable->succs(me.id+1, _nsucc, LOC_ONCHECK);
  vector<IDMap> tmp= loctable->succs(me.id+1, _nsucc, LOC_HEALTHY);
  Time after = now();
  if (succs.size() > 0) {
    assert(before < 86400000);
    printf("%s Chord stabilize AFTER elapsed %u (started %llu) pred <%u,%qx,%u> succ <%u,%qx,%u>  _inited %u succsz %u healthy %u: ", ts(), (uint)(after-before), before, pred.ip, pred.id, pred.heartbeat, succs[0].ip, succs[0].id, succs[0].heartbeat,  _inited, succs.size(), tmp.size());
    for (uint i = 0; i < succs.size(); i++) {
      printf("<%u %u> ",succs[i].ip,succs[i].heartbeat);
    }
    printf("|||");
    for (uint i = 0; i < tmp.size(); i++) {
      printf("<%u %u> ",tmp[i].ip,tmp[i].heartbeat);
    }
    printf("\n");
  } else
    printf("%s Chord stabilize AFTER elapsed %u (started %llu) pred %u,%qx LOST SUCC!!!", ts(), now()-before, before, pred.ip, pred.id);
#endif
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
#ifdef CHORD_DEBUG
    printf("%s oracle_node_died predecessor del %u,%qx add %u,%qx\n",ts(),n.ip,n.id,ids[(my_pos-1)%sz].ip, ids[(my_pos-1)%sz].id);
#endif
    return;
  }

  //lost one of my successors
  vector<IDMap> succs = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
  uint ssz = succs.size();
  assert(ssz==_nsucc);
  IDMap last = succs[succs.size()-1];
  if (ConsistentHash::betweenrightincl(me.id, succs[succs.size()-1].id, n.id)) {
    loctable->del_node(n);
    loctable->add_node(ids[(my_pos+_nsucc)%sz],true);
    vector<IDMap> newsucc = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
    uint nssz = newsucc.size();
    assert(nssz == _nsucc);
#ifdef CHORD_DEBUG
    printf("%s oracle_node_died successors del %u,%qx add %u,%qx succsz %d\n",ts(),n.ip,n.id,
    ids[(my_pos+_nsucc)%sz].ip, ids[(my_pos+_nsucc)%sz].id, succs.size());
#endif
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
    loctable->del_node(pred);
    loctable->add_node(n);
#ifdef CHORD_DEBUG
    printf("%s oracle_node_joined predecessor del %u,%qx, add %u,%qx\n",ts(),pred.ip,pred.id,n.ip,n.id);
#endif
    return;
  }
 
  //is the new node one of my successor?
  vector<IDMap> succs = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
  uint ssz = succs.size();
  assert(ssz==_nsucc);
  if (ConsistentHash::between(me.id,succs[succs.size()-1].id,n.id)) {
    loctable->del_node(succs[succs.size()-1]);
    loctable->add_node(n,true);
    vector<IDMap> newsucc = loctable->succs(me.id+1,_nsucc,LOC_ONCHECK);
    uint nssz = newsucc.size();
    assert(nssz == _nsucc);
#ifdef CHORD_DEBUG
    printf("%s oracle_node_joined successors del %u,%qx add %u,%qx succsz %d\n",ts(),succs[succs.size()-1].ip, succs[succs.size()-1].id,n.ip,n.id, succs.size());
#endif
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
#ifdef CHORD_DEBUG
  printf("%s inited %d %d succ is %u,%qx\n", ts(), ids.size(), 
      loctable->size(), succ1.ip, succ1.id);
#endif
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
  if (ok) record_stat(TYPE_FIXPRED_UP,gpr.v.size());

  if (!alive()) return;
  if (ok) {
    loctable->update_ifexists(gpr.dst); //refresh timestamp
    if (gpr.v.size() > 0) {
      IDMap tmp = gpr.v[0];
      loctable->add_node(gpr.v[0]);
      if (gpr.v[0].ip == me.ip) {
#ifdef CHORD_DEBUG
	if (!_inited)
	  printf("%s() fix_predecessor %u,%qx _inited changed from %u to 1\n",ts(),pred.ip, pred.id, _inited?1:0);
#endif
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

  while (1) {

    aa.n.ip = 0;

    succ1 = loctable->succ(me.id+1,LOC_ONCHECK); 
    assert(succ1.ip == 0 || succ1.heartbeat < 86400000);
    if (succ1.ip == 0 || succ1.ip == me.ip) {
      //sth. wrong, i lost my succ, join again
      if (!_join_scheduled) {
	_join_scheduled++;
#ifdef CHORD_DEBUG
	printf("%s joincrash fix_successor rejoin aftering losing succ\n",ts());
#endif
	delaycb(0, &Chord::join, (Args *)0);
      }
      return;
    }
#ifdef CHORD_DEBUG
    Time before = now();
#endif

    ok = failure_detect(succ1,&Chord::get_predsucc_handler, &gpa, &gpr, TYPE_FIXSUCC_UP,0);
    if (ok) record_stat(TYPE_FIXSUCC_UP,2);

    if (!alive()) return;

    if (!ok) {
#ifdef CHORD_DEBUG
      printf("%s detect fix_successor old succcessor (%u,%qx) died in %u msec\n", 
	  ts(), succ1.ip, succ1.id, now()-before);
#endif
      loctable->del_node(succ1, true); //successor dead, force delete
      aa.n = succ1;
    } else {
      assert(gpr.dst.ip == succ1.ip);
      loctable->update_ifexists(gpr.dst); //refresh timestamp
#ifdef CHORD_DEBUG
      printf("%s fix_successor successor (%u,%qx,%u)'s predecessor is (%u, %qx,%u)\n", 
	ts(), succ1.ip, succ1.id, gpr.dst.heartbeat, gpr.n.ip, gpr.n.id,gpr.n.heartbeat);
#endif

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
	  if (ok) record_stat(TYPE_FIXSUCC_UP,0);

	  if(!alive()) return;
      
	  if (!ok) {
#ifdef CHORD_DEBUG
	    printf("%s notify succ dead %u,%qx\n",ts(), succ1.ip, succ1.id);
#endif
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
	  record_stat(TYPE_FIXSUCC_UP,0);
	} else {
	  loctable->del_node(succ2,true);
	}
	return;
      }
    }
  }
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
  if (ok) record_stat(TYPE_FIXSUCCLIST_UP, gpr.v.size());

  if (!alive()) return;

  if (!ok) {
    loctable->del_node(succ,true);
  }else{

    loctable->update_ifexists(gpr.dst);//update timestamp

    //scs[0] might not be succ anymore
    vector<IDMap> scs = loctable->succs(me.id + 1, _nsucc, LOC_DEAD);
    assert(scs.size() <= _nsucc);

#ifdef CHORD_DEBUG
  printf("%s fix_successor_list (succ %u,%qx): ",ts(), succ.ip,succ.id);
  for (uint i = 0; i < gpr.v.size(); i++) 
    printf("%u ", gpr.v[i].ip);
  printf("\n");
  printf("%s fix_successor_list succ list sz %u: ", ts(), scs.size());
  for (uint i = 0; i < scs.size(); i++) 
    printf("%u ", scs[i].ip);
  printf("\n");
#endif

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
#ifdef CHORD_DEBUG
	printf("%s del %u %u node %u,%qx (%u,%qx) from successor list\n",ts(), scs_i, gpr_i, scs[scs_i].ip, scs[scs_i].id, gpr.v[gpr_i].ip,gpr.v[gpr_i].id);
#endif
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
#ifdef CHORD_DEBUG
  printf("%s notify_handler get pred(maybe) %u,%qx\n",ts(), args->me.ip, args->me.id);
#endif
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
#ifdef CHORD_DEBUG
   printf("%s alert_handler delete %u,%qx\n", ts(), args->n.ip, args->n.id);
#endif
  }else{
    record_stat(TYPE_MISC,0,0);
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

  // XXX: Thomer says: not necessary
  //node()->crash ();
  _inited = false;
  loctable->del_all();
#ifdef CHORD_DEBUG
  if (me.ip == DNODE) 
    fprintf(stderr,"%s crashed\n", ts());
  printf("%s (joincrash) crashed! loctable sz %d\n",ts(), loctable->size());
#endif
  notifyObservers((ObserverInfo *)"crash");
  if (_learn) {
    learntable->del_all();
  }
}


/*************** LocTable ***********************/

LocTable::LocTable()
{
  _evict = false;
} 

void LocTable::init(Chord::IDMap m)
{
  ring.repok();
  me = m;
  pin(me.id, 1, 0);
  idmapwrap *elm = New idmapwrap(me, now());
  ring.insert(elm);
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
  if (size() == 1) {
    return me;
  }
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
   also returns the timestamp of the first node
 */
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m, int status)
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());
  Time t = now();

  if (m <= 0) return v;

  idmapwrap *ptr = ring.closestsucc(id);
  assert(ptr);
  idmapwrap *ptrnext;

  uint j = 0;
  while (j < m) {
    if (ptr->n.ip == me.ip) break;

    ptrnext = ring.next(ptr);
    if (!ptrnext) ptrnext = ring.first();

    if ((id == (me.id+1)) && (!ptr->is_succ)) return v;

    if ((!_timeout)||(t - ptr->timestamp) < _timeout) {
      if (ptr->status <= status) {
	if (v.size() > 0) 
	  assert(ptr->n.ip != v[v.size()-1].ip);
	v.push_back(ptr->n);
	j++;
	if (j >= ring.size()) return v;
      }
    }else{
      ring.remove(ptr->id);
      bzero(ptr, sizeof(*ptr));
      delete ptr;
      if (ring.size() <= 1) {
	return v;
      }
    }
    ptr = ptrnext;
  }
  return v;
}

Chord::IDMap
LocTable::pred(Chord::CHID id, int status)
{
  if (size() == 1) {
    return me;
  }
  vector<Chord::IDMap> v = preds(id, 1, status);
  if (v.size() > 0) {
    return v[0];
  }

  Chord::IDMap tmp;
  tmp.ip = 0;
  return tmp;
}

vector<Chord::IDMap>
LocTable::preds(Chord::CHID id, uint m, int status) 
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());
  idmapwrap *elm = ring.closestpred(id);

  assert(elm);
  idmapwrap *elmprev;

  Time t = now();
  uint deleted = 0;
  uint rsz = ring.size();
  while (1) {
    if (elm->id == me.id) break;

    elmprev = ring.prev(elm);
    if (!elmprev) elmprev = ring.last();

    if ((!_timeout) || (t - elm->timestamp < _timeout)) {
      if (elm->status <= status) { 
	v.push_back(elm->n);
	if (v.size() == m) break;
      }
    }else {
      ring.remove(elm->id);
      bzero(elm, sizeof(*elm));
      delete elm;
      deleted++;
    }
    elm = elmprev;
    assert((rsz - deleted >= 1));
  }
  return v;
}

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

/* adds a list of nodes, sorted by increasing CHIDs
 * this function will avoid regular eviction calls by adding only pinned nodes
 */
void
LocTable::add_sortednodes(vector<Chord::IDMap> l)
{
  uint sz = pinlist.size();
  int lsz = l.size();
  Chord::IDMap tmppin;
  tmppin.ip = 0;
  int pos;
  int ptr;
  Chord::IDMap n;
  pin_entry p;

  for (uint i = 0; i < sz; i++) {
    p = pinlist[i];
    tmppin.id = pinlist[i].id;
    pos = upper_bound(l.begin(),l.end(),tmppin,Chord::IDMap::cmp) - l.begin();
    if (pos >= lsz) pos = 0;
    ptr = pos;
    if (pos < lsz && (l[pos].id != tmppin.id)) {
      ptr--;
    }
    for (uint k = 0; k < pinlist[i].pin_pred; k++) {
      if (ptr< 0) ptr= (lsz-1);
      n = l[ptr];
      add_node(l[ptr]);
      ptr--;
    }
    ptr = pos; 
    for (uint k = 0; k < pinlist[i].pin_succ; k++) {
      if (ptr >= lsz) ptr= 0;
      n = l[ptr];
      add_node(l[ptr]);
      ptr++;
    }
  }
}

int
LocTable::add_check(Chord::IDMap n)
{
  idmapwrap *elm = ring.search(n.id);
  
  
  if (!elm) 
    return -1;

  if ((elm->status <= LOC_HEALTHY) && (n.heartbeat >= elm->n.heartbeat)){
#ifdef CHORD_DEBUG
    printf("%llu ChordFingerPNS(%u,%qx,%u) putting <%u,%qx,%u> ON CHECK is_succ?%d\n", now(),me.ip,me.id,me.heartbeat,elm->n.ip,elm->n.id,elm->n.heartbeat,elm->is_succ?1:0);
#endif
    elm->status = LOC_ONCHECK;
    return LOC_ONCHECK;
  }else{
    return LOC_DEAD;
  }
}

bool 
LocTable::update_ifexists(Chord::IDMap n)
{
  idmapwrap *ptr = ring.search(n.id);
  if (!ptr) return false;
  ptr->timestamp = now();
  if (n.heartbeat > ptr->n.heartbeat) {
    ptr->status = LOC_HEALTHY;
    ptr->n.heartbeat = n.heartbeat;
  }
  return true;
}
void
LocTable::add_node(Chord::IDMap n, bool is_succ, bool assertadd, Chord::CHID fs,Chord::CHID fe, bool replacement)
{
  Chord::IDMap succ1; 
  Chord::IDMap pred1; 

  if (vis) {
    succ1 = succ(me.id+1);
    pred1 = pred(me.id-1);
  }
  idmapwrap *elm = ring.closestsucc(n.id);
  if (elm->id == n.id) {
    if (n.heartbeat > elm->n.heartbeat) {
      assert(n.heartbeat < 86400000);
      elm->n.heartbeat = n.heartbeat;
      if (replacement)
	elm->status = LOC_REPLACEMENT;
      else
	elm->status = LOC_HEALTHY;
    }
    elm->timestamp = now();
    if (is_succ) {
      elm->fs = elm->fe = 0;
      elm->is_succ = is_succ;
    }
    elm->fs = fs;
    elm->fe = fe;
  } else {
    idmapwrap *newelm = New idmapwrap(n,now());
    if (elm->is_succ) 
      newelm->is_succ = true;
    else
      newelm->is_succ = is_succ;
    newelm->fs = fs;
    newelm->fe = fe;
    newelm->status = replacement? LOC_REPLACEMENT:LOC_HEALTHY;
    newelm->timestamp = now();
    if (ring.insert(newelm)) {
    }else{
      assert(0);
    }
    elm = newelm;
  }

  assert (ring.repok ());

  if (_evict && ring.size() > _max) {
    evict();
  }

  if (vis) {

    Chord::IDMap succ2 = succ(me.id + 1);
    Chord::IDMap pred2 = pred(me.id-1);

    if(succ1.id != succ2.id) {
      printf("vis %llu succ %16qx %16qx\n", now (), me.id, succ2.id);
    }

    if(pred1.id != pred2.id) {
      printf("vis %llu pred %16qx %16qx\n", now (), me.id, pred2.id);
    }
  }
  assert(!assertadd || elm->status <= LOC_HEALTHY);
}

void
LocTable::del_node(Chord::IDMap n, bool force)
{

#ifdef CHORD_DEBUG
  Chord::IDMap succ1 = succ(me.id+1);
#endif
  assert(n.ip != me.ip);
  idmapwrap *elm = ring.search(n.id);
  if (!elm) return;

  if ((elm->is_succ) && (!force)) {
    elm->status = LOC_DEAD;
  } else {
    elm = ring.remove(n.id);
    //printf("%u,%qx del %p\n", me.ip, me.id, elm);
    bzero(elm,sizeof(*elm));
    delete elm;
  }
#ifdef CHORD_DEBUG
  Chord::IDMap succ2 = succ(me.id +1,1);
  if (succ2.ip != succ1.ip) {
    idmapwrap *ptr = ring.closestsucc(me.id+1);
    printf("%llu (%u,%qx) del caused succ change from %u,%qx to %u,%qx what? %u,%qx\n", 
	now(), me.ip, me.id, succ1.ip,succ1.id, succ2.ip, succ2.id, ptr->n.ip, ptr->n.id);
  }
#endif
  assert (ring.repok ());
}

void
LocTable::clear_pins()
{
  uint sz = pinlist.size();
  for (uint i = 0; i < (sz -1); i++) {
    pinlist.pop_back();
  }
}

// pin maintains sorted list of id's that must be pinned
void
LocTable::pin(Chord::CHID x, uint pin_succ, uint pin_pred)
{
  pin_entry pe;

  pe.id = x;
  pe.pin_succ = pin_succ;
  pe.pin_pred = pin_pred;

  _max += (pin_succ + pin_pred);

  if (pinlist.size () == 0) {
    pinlist.push_back(pe);
    return;
  }

  uint i = 0;
  for (; i < pinlist.size(); i++) {
    if (ConsistentHash::betweenrightincl(pinlist[0].id, pinlist[i].id, x)) {
      break;
    }
  }

  if (i < pinlist.size() && pinlist[i].id == x) {
    if (pin_pred > pinlist[i].pin_pred) 
      pinlist[i].pin_pred = pin_pred;
    if (pin_succ > pinlist[i].pin_succ)
      pinlist[i].pin_succ = pin_succ;
  } else {
    if (pinlist.size () == i) {
      pinlist.push_back(pe);
    } else {
      assert(pinlist[0].id != pe.id);
      pinlist.insert(pinlist.begin() + i, pe);
    }
  }
  assert(pinlist.size() <= _max);
}

uint
LocTable::size()
{
  return ring.size();
}

void
LocTable::evict() // all unnecessary(unpinned) nodes 
{
  assert(0);
  assert(pinlist.size() <= _max);
  assert(pinlist.size() > 0);


  idmapwrap *ptr;
  idmapwrap *elm = ring.first();
  while (elm) {
    elm->pinned = false;
    elm = ring.next(elm);
  }
  
  uint i = 0; // index into pinlist

  elm = ring.first();
  while (i < pinlist.size ()) {

    // find successor of pinlist[i]. 
    //XXX don't start at j, but where we left off
    while (elm && elm->id < pinlist[i].id) {
      elm = ring.next(elm);
    }
    if (elm && elm->id > pinlist[i].id ) {
      ptr = ring.prev(elm);
    }else {
      ptr = elm;
    }

    // pin the predecessors
    for (uint k = 0; k < pinlist[i].pin_pred; k++) {
      if (!ptr) ptr = ring.last();
      ptr->pinned = true;
      ptr = ring.prev(ptr);
    }

    ptr = elm;
    // pin the successors, starting with j
    for (uint k = 0; k < pinlist[i].pin_succ; k++) {
      if (!ptr) ptr = ring.first();
      ptr->pinned = true;
      ptr = ring.next(ptr);
    }
    i++;
  }
  // evict entries that are not pinned
  elm = ring.first();
  idmapwrap *next; 
  while (elm) {
    next = ring.next(elm);
    if (elm->pinned) {
      elm->pinned = false;
    }else{
      ring.remove(elm->id);
      delete elm;
      assert (ring.repok ());
    } 
    elm = next;
  }

  assert(elm == NULL);
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
LocTable::get_all()
{
  vector<Chord::IDMap> v;
  v.clear();

  idmapwrap *currp; 
  currp = ring.first();
  while (currp) {
    v.push_back(currp->n);
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
LocTable::dump()
{
  printf("===%u,%qx loctable dump at %llu===\n", me.ip, me.id,now());
  idmapwrap *elm = ring.closestsucc(me.id+1);
  while (elm->n.ip != me.ip) {
    printf("(%qx, %u, %d, %llu)\n", elm->n.id, elm->n.ip, elm->is_succ, elm->timestamp);
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
    if ((elm->timestamp + _timeout) > t) {
      if (!succ.ip) succ = elm->n;
      if (elm->is_succ) 
	num_succ++;
      else
	num_finger++;
    }
    elm = ring.next(elm);
    if (!elm) elm = ring.first();
  }
  printf("%llu loctable stat for (%u,%qx): succ %u,%qx number of succs %u numer of finger %u\n", 
      t, me.ip, me.id, succ.ip, succ.id, num_succ, num_finger);
}
