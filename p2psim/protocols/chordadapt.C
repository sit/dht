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
 *
 */
#include <dmalloc.h>
#include "chordadapt.h"
#include <math.h>
#include <stdio.h>
#include <assert.h>

vector<IDMap> ChordAdapt::ids;
bool ChordAdapt::sorted;
vector<Time> ChordAdapt::sort_live;
vector<Time> ChordAdapt::sort_dead;

ChordAdapt::ChordAdapt(IPAddress i, Args& a) : P2Protocol(i)
{
  _burst_sz = a.nget<uint>("burst_size", 10, 10);
  _bw_overhead = a.nget<uint>("overhead_rate", 100, 10);
  _stab_basic_timer = a.nget<uint>("basictimer", 18000, 10);
  _rate_queue = new RateControlQueue(this, (double)_bw_overhead, _burst_sz, ChordAdapt::empty_cb);
  _adjust_interval = 1000*_burst_sz/_bw_overhead;
  _parallelism = 1;
  _next_adjust = _adjust_interval;

  _last_calculated = 0;
  _live_stat.clear();
  _dead_stat.clear();
  for (uint i = 0;i < 10; i++) 
    _calculated_prob.push_back(10*_stab_basic_timer);

  _lookup_times = 0;
  _empty_times = 0;
  _nsucc = a.nget<uint>("successors",16,10);
  _to_multiplier = a.nget<uint>("timeout_multiplier", 3, 10);
  _learn_num = a.nget<uint>("learn_num",10,10);
  _max_p = _burst_sz/(2*(40 + 4 * _learn_num));
  if (_max_p > 6)
    _max_p = 6;

  _stab_basic_running = false;
  _join_scheduled = 0;
  _last_stab = 0;

  _me.ip = ip();
  _me.id = ConsistentHash::ip2chid(_me.ip);
  _me.timestamp = 0;
  _id = _me.id;

  loctable = new LocTable();
  loctable->init(_me);

  _wkn.ip = 0;

  ids.push_back(_me);

  notifyinfo.clear();

  _top = Network::Instance()->gettopology();
  _max_succ_gap = 0;

}

ChordAdapt::~ChordAdapt()
{
  if (alive()) {
    vector<IDMap>::iterator p = find(ids.begin(),ids.end(),_me);
    ids.erase(p);
    for (HashMap<ConsistentHash::CHID, Time>::iterator i = _outstanding_lookups.begin();
	i != _outstanding_lookups.end(); ++i) {
      NDEBUG(2) << "done lookup key " << printID(i.key()) << "timeout failed started " 
	<< i.value() << endl;
      record_lookup_stat(_me.ip, _me.ip, now()-i.value(), false, false, 0, 0, 0);
    }
    if (ids.size() == 0) 
    Node::print_stats();
  }
  delete loctable;
}

unsigned
ChordAdapt::PKT_SZ(unsigned ids, unsigned others)
{
  return PKT_OVERHEAD + 4 * ids + others;
}

/* -------------- initstate ---------------- */
void
ChordAdapt::initstate()
{
  if (!sorted)
    sort(ids.begin(),ids.end(), IDMap::cmp);

  uint sz = ids.size();
  //add successors
  uint my_pos = find(ids.begin(), ids.end(), _me) - ids.begin();
  IDMap n;
  for (uint i = 1; i <= _nsucc; i++)  {
    n = ids[(my_pos+i) % sz];
    n.timestamp = now();
    loctable->add_node(n,true);
  }

  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);

  //add random nodes
  for (uint i = 0; i < 20; i++) {
    uint r = random() % sz;
    if (ids[r].id != _me.id) {
      n = ids[r];
      n.timestamp = now();
      loctable->add_node(n);
    }
  }

  IDMap succ = loctable->succ(_me.id+1);
  NDEBUG(3) << "inited succ " << succ.ip << "," << printID(succ.id) 
    << " locsz " << loctable->size() << " succsz " << loctable->succ_size() 
    << endl;
}

/* -------------- join --------------------- */
void
ChordAdapt::join(Args *args)
{
  if (!alive()) return;

  while ((!_wkn.ip) || (!Network::Instance()->alive(_wkn.ip))) {
    _wkn.ip = Network::Instance()->getnode(ids[random()%ids.size()].ip)->ip();
    _wkn.id = Network::Instance()->getnode(_wkn.ip)->id();
  }
  if (args) {
    _last_joined_time = now();
    _wkn.timestamp = now();
    _me.ip = ip();
    _me.id = ConsistentHash::ip2chid(_me.ip);
    _me.timestamp = now();
    _id = _me.id;
    loctable->init(_me);
    loctable->add_node(_wkn);
    vector<IDMap>::iterator p = upper_bound(ids.begin(),ids.end(),_me, IDMap::cmp);
    if (p->id!=_me.id)
      ids.insert(p,1,_me);
    _parallelism = 1;
    NDEBUG(1) << "start to join " << printID(_me.id-1) << " locsz " << loctable->size()  
      << " livesz " << loctable->live_size() 
      << " succsz " << loctable->succ_size() << " wkn " << _wkn.ip << endl;
  }else{
    NDEBUG(1) << "repeated join " << printID(_me.id-1) << " wkn " << _wkn.ip << " locsz " 
      << loctable->size() << endl;
  }
  
  IDMap succ = loctable->succ(_me.id+1);
  if ((args && args->nget<uint>("first",0,10)==1) || (succ.ip && succ.ip!=_me.ip)) {
    //start basic successor stabilization
    _join_scheduled = 0;
    if (!_stab_basic_running) {
      _stab_basic_running = true;
      delaycb(0,&ChordAdapt::stab_succ,(void *)0);
    } else {
      delaycb(0,&ChordAdapt::fix_succ, (void *)0);
    }
    return;
  }

  _join_scheduled = now();
  lookup_args *la = new lookup_args;
  lookup_ret *lr = new lookup_ret;
  bzero(la,sizeof(lookup_args));
  la->key = _me.id - 1;
  la->m = _nsucc;
  la->ori = _me;
  la->no_drop = true;
  la->parallelism = 1;
  la->type = TYPE_JOIN_LOOKUP;
  la->learnsz = _learn_num;
  la->overshoot = 0;

  _rate_queue->do_rpc(_wkn.ip, &ChordAdapt::find_successors_handler,
      &ChordAdapt::null_cb, la, lr, (uint)0, la->type,
      PKT_SZ(1,1), PKT_SZ(la->m,1),TIMEOUT(_me.ip, _wkn.ip));
}

int
ChordAdapt::null_cb(bool b, lookup_args *a, lookup_ret *r)
{
  if (a) delete a;
  if (r) delete r;
  if (b)
    return PKT_OVERHEAD;
  else 
    return 0;
}

void
ChordAdapt::join_handler(lookup_args *la, lookup_ret *lr)
{
  if (alive()) {
    la->src.timestamp = now();
    loctable->update_ifexists(la->src);
    la->from.timestamp = now();
    if (lr->v.size() > 0) 
      loctable->add_node(la->from);
    for (uint i = 0; i < lr->v.size(); i++) {
      if (lr->v[i].ip != _me.ip)  {
	loctable->add_node(lr->v[i],true);
      }
      if (i!=0) {
	ConsistentHash::CHID gap = lr->v[i].id - lr->v[i-1].id;
	if (_max_succ_gap == 0 || gap > _max_succ_gap) 
	  _max_succ_gap = gap;
      }
    }
    if (loctable->size() < 3) {
      NDEBUG(1) << "join_handler join failed sz " << lr->v.size() 
	<< " locsz " << loctable->size() << endl;
      delaycb(5000, &ChordAdapt::join, (Args *)0);
    } else {
      _join_scheduled = 0;
      //start basic successor stabilization
      if (!_stab_basic_running) {
	_stab_basic_running = true;
	delaycb(0,&ChordAdapt::stab_succ,(void *)0);
      } else {
	delaycb(0,&ChordAdapt::fix_pred,(void *)0);
      }
      join_learn();
    }
    IDMap succ = loctable->succ(_me.id+1);
    vector<IDMap> scs = loctable->succs(_me.id+1,100);
    NDEBUG(1) << "joined succ " << succ.ip << "," << printID(succ.id) 
      << "locsz " << loctable->size() << " livesz " 
      << loctable->live_size() << " succs: " << print_succs(scs) << 
      " scs: " << print_succs(lr->v) << endl;
  }
}

void
ChordAdapt::join_learn()
{
  vector<IDMap> scs = loctable->succs(_me.id+1,_nsucc);
  if (scs.size() < (_nsucc/2)) return;
  Time min = 1000000;
  IDMap min_n;
  min_n.ip = 0;
  for (uint i = 0; i < scs.size(); i++) {
    if (_top->latency(_me.ip, scs[i].ip) < min) {
      min = _top->latency(_me.ip, scs[i].ip);
      min_n = scs[i];
    }
  }
  learn_args *la = new learn_args;
  learn_ret *lr = new learn_ret;
  la->m = 3 * _learn_num; //means i want to learn from all
  la->n = min_n;
  la->src = _me;
  la->end = _me;
  NDEBUG(2) << "join_learn from " << la->n.ip << "," 
    << printID(la->n.id) << endl;
  _rate_queue->do_rpc(min_n.ip, &ChordAdapt::learn_handler, 
      &ChordAdapt::learn_cb, la, lr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), PKT_SZ(la->m,0),TIMEOUT(_me.ip,min_n.ip));

}

void
ChordAdapt::find_successors_handler(lookup_args *la, lookup_ret *lr)
{
 if (loctable->size() < 3) {
    lookup_args *lla = new lookup_args;
    lla->src = _me;
    bcopy(la,lla,sizeof(lookup_args));
    lookup_ret *llr = new lookup_ret;
    llr->v.clear();
    _rate_queue->do_rpc(la->ori.ip, &ChordAdapt::join_handler,
	&ChordAdapt::null_cb, lla, llr, 1, TYPE_JOIN_LOOKUP, 
	PKT_SZ(llr->v.size(),0),PKT_SZ(0,0),TIMEOUT(_me.ip, la->ori.ip));
      NDEBUG(2) << "find_successors_handler failed for " << la->ori.ip 
	<< "," << printID(la->ori.id) << " not joined" << endl;
  }else{
    lookup_args lla;
    bcopy(la,&lla,sizeof(lookup_args));
    lla.src = _me;
    lla.src.timestamp = now();
    lla.from = _me;
    lla.no_drop = true;
    NDEBUG(3)<<"find_successors_handler key " << printID(lla.key) << " from " 
      << lla.ori.ip << "," << printID(lla.ori.id) << endl;
    next_recurs(&lla,NULL);
  }
}

/* ------------------------ crash ------------------------------------*/
void
ChordAdapt::crash(Args *args)
{
  double ppp = _parallelism > 1?(exp(log(0.1)/(double)_parallelism)):0.1;
  Time timeout = est_timeout(ppp);
  NDEBUG(1) << "crashed locsz " << loctable->size() << " livesz " << loctable->live_size() 
    << " locsz_used " << loctable->size(LOC_HEALTHY, timeout) << " livesz_used " 
    << loctable->live_size(timeout) << " live_time " << now()-_last_joined_time 
    << " para " << _parallelism << " timeout " << timeout << endl;
  _last_joined_time = now();
  _rate_queue->stop_queue();
  loctable->del_all();
  for (HashMap<ConsistentHash::CHID, Time>::iterator i = _outstanding_lookups.begin();
      i != _outstanding_lookups.end(); ++i) {
    NDEBUG(2) << "done lookup key " << printID(i.key()) << "timeout failed started "
      << i.value() << endl;
    record_lookup_stat(_me.ip, _me.ip, now()-i.value(), false, false, 0, 0, 0);
  }
  _outstanding_lookups.clear();
  _forwarded.clear();
  _forwarded_nodrop.clear();
  vector<IDMap>::iterator p = find(ids.begin(),ids.end(),_me);
  ids.erase(p);
  notifyinfo.clear();
  _max_succ_gap = 0;
  _live_stat.clear();
  _dead_stat.clear();
  for (uint i = 0; i < 10; i++)
    _calculated_prob[i] = 10*_stab_basic_timer;
}

/* ------------------------ lookup (recursive) ----------------------- */
void
ChordAdapt::lookup(Args *args)
{
  if ((loctable->size() < 3)) {
    if (!_join_scheduled || (now()-_join_scheduled) > 20000) 
      delaycb(0,&ChordAdapt::join,(Args *)0);
    NDEBUG(2) << "lookup key failed not yet joined" << endl;
    record_lookup_stat(_me.ip, _me.ip, 0, false, false, 0, 0, 0);
    return;
  }

  lookup_args la;
  bzero(&la,sizeof(lookup_args));
  la.key = args->nget<ConsistentHash::CHID>("key",0,16);
  la.no_drop = true;
  la.nexthop = _me;
  la.from = _me;
  la.src = _me;
  la.src.timestamp = now();
  la.ori.ip = 0;
  la.m = 1;
  la.parallelism = _parallelism;
  la.type = TYPE_USER_LOOKUP;
  la.learnsz = _learn_num;

  _outstanding_lookups.insert(la.key, now());
  NDEBUG(2) << "start lookup key " << printID(la.key) << endl;
  if (_me.ip == 612 && now() == 333772) 
    fprintf(stderr,"shit!\n");
  next_recurs(&la,NULL);
}

void
ChordAdapt::donelookup_handler(lookup_args *la, lookup_ret *lr)
{
  if (la->ori.ip) {
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    lookup_ret *llr = new lookup_ret;
    llr->v = lr->v;
    _rate_queue->do_rpc(la->ori.ip, &ChordAdapt::join_handler,
	&ChordAdapt::null_cb, lla, llr, 0, TYPE_JOIN_LOOKUP, 
	PKT_SZ(llr->v.size(),0), PKT_SZ(0,0),TIMEOUT(_me.ip, la->ori.ip));
    return;
  };

  if (la->from.ip!=_me.ip) {
    la->from.timestamp = now();
    loctable->add_node(la->from);
  }
  assert(lr->done);
  Time t = _outstanding_lookups.find(la->key);
  if (t) {
    if (lr->v.size() == 0) {
      NDEBUG(2) << "done lookup key " << printID(la->key) << "from " 
	<< la->from.ip << "," << printID(la->from.id) << "failed " << endl;
      record_lookup_stat(_me.ip, la->from.ip, now()-t, false,false, 
	  la->hops, la->to_num, la->to_lat);
    }else{
      bool b = check_pred_correctness(la->key, la->from);
      NDEBUG(2) << "done lookup key " << printID(la->key) << "from " 
      << la->from.ip << "," << printID(la->from.id) 
      << "succ " << lr->v.size() << " " << (lr->v.size()>0?lr->v[0].ip:0) << "," 
      << printID(lr->v.size()>0?lr->v[0].id:0) << "best " << 
      2*Network::Instance()->gettopology()->latency(_me.ip, la->from.ip) << " lat " 
      << now()-t << " hops " << la->hops << " timeouts " << la->to_num 
      << " correct? " << (b?1:0) << endl;
      record_lookup_stat(_me.ip, la->from.ip, now()-t, true,
	  b, la->hops, la->to_num, la->to_lat);
    }
    _outstanding_lookups.remove(la->key);
  }
}

void
ChordAdapt::next_recurs(lookup_args *la, lookup_ret *lr)
{
  if (lr)
    lr->done = true;

  //learn the src node if this query
  loctable->add_node(la->src);
  la->from.timestamp = now();
  loctable->add_node(la->from);
  if (lr && la->overshoot) {
    //lr->v = loctable->get_closest_in_gap(la->learnsz, la->key, la->from);
    lr->v = loctable->get_closest_in_gap(la->learnsz, la->overshoot, la->from, la->timeout?la->timeout:est_timeout(0.9));
  }

  //if i have forwarded pkts for this key
  //and the packet is droppable
  if (_forwarded.find(la->src.id | la->key)) {
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << 
      la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip 
      << " forwarded before no_drop? " << (la->no_drop?1:0) << endl;
    if (!la->no_drop) return;
  }else
    _forwarded.insert(la->src.id | la->key, now());

  //for adjusting parallelisms in the next discrete interval
  if (now() >= _next_adjust)
    adjust_parallelism();
  _lookup_times++;

  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip || succ.ip == _me.ip) {
    NDEBUG(4) << "next_recurs not joined key " << printID(la->key) 
      << "failed" << endl;
    if ((!_join_scheduled) || (now()-_join_scheduled) > 20000)
      delaycb(0,&ChordAdapt::join, (Args *)0); //join again
    if (lr) {
      lr->v.clear();
      lr->done = false;
    }
    return;
  }

  if (succ.ip && ConsistentHash::between(_me.id,succ.id,la->key)) {
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    lla->from = _me;
    lla->nexthop = lla->src;
    lookup_ret *llr = new lookup_ret;
    llr->done = true;
    llr->v = loctable->succs(_me.id+1,la->m);
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << 
      la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip 
      << " no_drop? " << (la->no_drop?1:0) << " done succ " 
      << succ.ip << "," << printID(succ.id) << " hops " << lla->hops << endl;
    _rate_queue->do_rpc(lla->src.ip, &ChordAdapt::donelookup_handler,
	&ChordAdapt::null_cb, lla, llr, lla->ori.ip?0:1, lla->type, 
	PKT_SZ(1,0), PKT_SZ(0,0),TIMEOUT(_me.ip,lla->src.ip));
    return;
  }

  uint para = (_rate_queue->quota()+_burst_sz)/(2*(2*PKT_OVERHEAD+ 4*_learn_num));
  if (para > _parallelism)
    para = _parallelism;
  else if (para > la->parallelism)
    para = la->parallelism;
  else if (para <= 0)
    para = 1;
  double ppp = para > 1? (exp(log(0.1)/(double)para)):0.1;
  Time ttt = est_timeout(ppp);
  vector<IDMap> nexthops = loctable->preds(la->key, para, LOC_HEALTHY, ttt);
  uint nsz = nexthops.size();
  assert(nsz <= para);
  NDEBUG(3) << "next_recurs " << printID(la->key) << " nsz " << nsz << ": " << print_succs(nexthops) << endl;
  IDMap overshoot = loctable->succ(la->key);
  bool sent_success;
  uint i;
  for (i = 0; i < nexthops.size(); i++) {
    if (!ConsistentHash::between(_me.id,la->key,nexthops[i].id))
      break;
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    if ((la->no_drop) && i == 0) {
      lla->no_drop = true;
      lla->learnsz = _learn_num;
    } else {
      lla->no_drop = false;
      lla->learnsz = 1;
    }
    if (_rate_queue->critical())
      lla->learnsz = 1;
    lla->timeout = ttt;
    lla->hops = la->hops+1;
    lla->nexthop = nexthops[i];
    lla->from = _me;
    //lla->overshoot = overshoot.id;
    lla->overshoot = lla->key;
    lookup_ret *llr = new lookup_ret;
    llr->v.clear();
    NDEBUG(3) << "next_recurs key " << printID(la->key) << " quota " << _rate_queue->quota() << " locsz " << loctable->size() 
      << " livesz " << loctable->live_size() 
      << " src " << la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip << 
      " forward to next " << nexthops[i].ip << "," << printID(nexthops[i].id)  << "," 
      << now()-nexthops[i].timestamp << " hops " << lla->hops 
      << " est_to " << ttt << " est_sz " << (_live_stat.size()+_dead_stat.size())
      << " nsz " << nsz << " i " << i << " nodrop " << lla->no_drop << endl;
    sent_success = _rate_queue->do_rpc(nexthops[i].ip, &ChordAdapt::next_recurs,
	&ChordAdapt::next_recurs_cb, lla,llr, lla->ori.ip?0:(lla->no_drop?1:3), lla->type,
	PKT_SZ(1,0), PKT_SZ(lla->learnsz,0),TIMEOUT(_me.ip,nexthops[i].ip));
    if (!sent_success) break;
  }
  if ((la->no_drop) && (i>0))
    _forwarded_nodrop.insert(la->src.id | la->key, (uint)i);
}

int
ChordAdapt::next_recurs_cb(bool b, lookup_args *la, lookup_ret *lr)
{
  int ret_sz = 0;
  if (alive()) {
    add_stat(now()-la->nexthop.timestamp, b);
    la->nexthop.timestamp = now();
    if (b) {
      loctable->update_ifexists(la->nexthop);
      for (uint i = 0; i < lr->v.size(); i++) {
	IDMap xx = loctable->succ(lr->v[i].id,LOC_HEALTHY);
	if ((xx.ip == lr->v[i].ip) &&(lr->v[i].timestamp>xx.timestamp))
	  add_stat(lr->v[i].timestamp-xx.timestamp,true);
	loctable->add_node(lr->v[i]);
      }
      NDEBUG(4) << "next_recurs_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from " << la->nexthop.ip
	<< "," << printID(la->nexthop.id) << " learnt " << lr->v.size() << ": " 
	<< print_succs(lr->v) << " nodes locsz " 
	<< loctable->size() << " livesz " << loctable->live_size() << endl;
      ret_sz = PKT_SZ(lr->v.size(),0);
    } else {
      loctable->del_node(la->nexthop);
      ret_sz = 0;
    }
    uint outstanding = _forwarded_nodrop.find(la->src.id | la->key);
    if (outstanding) {
      if (b && la->no_drop) {
	_forwarded_nodrop.remove(la->key|la->src.id);
	NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	  << la->src.ip << " ori " << la->ori.ip << " from "
	  << la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	  la->ori.ip << " successfully forwarded nodrop" << endl;
      }else if (outstanding == 1) {
	  //send again, to myself
	  la->to_lat += TIMEOUT(_me.ip, la->nexthop.ip);
	  la->to_num++;
	  la->no_drop = true;
	  _forwarded_nodrop.remove(la->key|la->src.id);
	  NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	    << la->src.ip << " ori " << la->ori.ip << " from "
	    << la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	    la->ori.ip << "dead " << (b?0:1) << " restransmit" << endl;
	  next_recurs(la,lr);
       } else {
	 NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	   << la->src.ip << " ori " << la->ori.ip << " from "
	   << la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	   la->ori.ip << "dead " << (b?0:1) << " outstanding " << (outstanding-1) << endl; 
	 _forwarded_nodrop.insert(la->src.id|la->key,(outstanding-1));
       }
    }else {
      NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from "
	<< la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	la->ori.ip << (b?" live":" dead") << " dont care" << (outstanding-1) << endl; 
    }
  }
  delete la;
  if (lr) delete lr;
  return ret_sz;
}

/* ------------- fix successor routines ----------------- */
void
ChordAdapt::stab_succ(void *x)
{
  if (!alive()) {
    _stab_basic_running = false;
    return;
  }
  fix_pred(NULL);
  _last_stab = now();
  delaycb(_stab_basic_timer, &ChordAdapt::stab_succ, (void *)0);
}

void
ChordAdapt::notify_pred()
{
  IDMap pred = loctable->pred(_me.id-1);

  notify_succdeath_args *nsa = new notify_succdeath_args;
  notify_succdeath_ret *nsr = new notify_succdeath_ret;

  nsa->info = notifyinfo;
  assert(nsa->notifyinfo.size() <= 2*_nsucc);
  notifyinfo.clear();
  nsa->n = pred;
  nsa->src = _me;
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::notify_succdeath_handler,
      &ChordAdapt::notify_pred_cb, nsa, nsr, 0, TYPE_FIXPRED_UP, PKT_SZ(0,1),PKT_SZ(1,1),
      TIMEOUT(_me.ip,pred.ip));

}

void
ChordAdapt::notify_succdeath_handler(notify_succdeath_args *nsa, notify_succdeath_ret *nsr)
{
  nsr->succ = loctable->succ(_me.id+1);
  if (nsr->succ.ip != nsa->src.ip) {
    return;
  }else{
    bool b;
    for (uint i = 0; i < nsa->info.size(); i++) {
      if (nsa->info[i].dead) {
	b = loctable->del_node(nsa->info[i].n); 
      }else{
	b = loctable->add_node(nsa->info[i].n);
      }
      nsa->info[i].ttl--;
      if (b && nsa->info[i].ttl>0)
	notifyinfo.push_back(nsa->info[i]);
    }
  }
}

int
ChordAdapt::notify_pred_cb(bool b, notify_succdeath_args *nsa, notify_succdeath_ret *nsr)
{
  int ret_sz = 0;
  if (alive()) {
    if ((!b) || nsr->succ.ip!=_me.ip) {
      notifyinfo = nsa->info;
    }
    if (b) {
      ret_sz += PKT_SZ(nsa->info.size(),0);
    }else{
      //XXX
      loctable->del_node(nsa->n);
    }
  }
  delete nsa;
  delete nsr;
  return ret_sz;
}

void
ChordAdapt::fix_pred(void *a)
{
  if (!alive()) return;
  IDMap pred = loctable->pred(_me.id-1);
  get_predsucc_args *gpa = new get_predsucc_args;
  get_predsucc_ret *gpr = new get_predsucc_ret;
  gpa->m = 1;
  gpa->n = pred;
  gpa->src = _me;
  gpr->v.clear();
  NDEBUG(3) << " fix_pred " << pred.ip << "," << printID(pred.id) << endl;
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::get_predsucc_handler,
      &ChordAdapt::fix_pred_cb, gpa, gpr, 0, TYPE_FIXPRED_UP, PKT_SZ(0,1), PKT_SZ(1,1),
      TIMEOUT(_me.ip,pred.ip));
}

int
ChordAdapt::fix_pred_cb(bool b, get_predsucc_args *gpa, get_predsucc_ret *gpr)
{
  int ret_sz = 0;
  if (alive()) {
    gpa->n.timestamp = now();
    NDEBUG(4) << "fix_pred_cb pred " << gpa->n.ip << (b?" alive":" dead") << endl;
    if (b) {
      ret_sz = PKT_SZ(1,0);
      loctable->update_ifexists(gpa->n);
      if (gpr->v.size()>0) 
	loctable->add_node(gpr->v[0]);
    } else {
      loctable->del_node(gpa->n,true);
    } 
    delaycb(200,&ChordAdapt::fix_succ,(void*)0);
  }
  delete gpa;
  delete gpr;
  return ret_sz;
}

void 
ChordAdapt::fix_succ(void *a)
{
  if (!alive()) return;

  IDMap succ = loctable->succ(_me.id+1);

  if (succ.ip == 0) {
    NDEBUG(1) << "fix_succ locsz " << loctable->size() 
      << " reschedule join" << endl;
    if ((!_join_scheduled) || (now()-_join_scheduled) > 20000)
      delaycb(200, &ChordAdapt::join, (Args *)0);
    return;
  }

  get_predsucc_args *gpa = new get_predsucc_args;
  get_predsucc_ret *gpr = new get_predsucc_ret;
  gpr->v.clear();
  gpa->n = succ;
  gpa->src = _me;
  gpa->m= _nsucc;

  NDEBUG(2) << "fix_succ succ " << succ.ip << "," << printID(succ.id) << endl;
  _rate_queue->do_rpc(succ.ip, &ChordAdapt::get_predsucc_handler,
	&ChordAdapt::fix_succ_cb, gpa,gpr, 0, TYPE_FIXSUCC_UP, PKT_SZ(0,1), PKT_SZ(gpa->m,0),
	TIMEOUT(_me.ip,succ.ip));
}

void
ChordAdapt::get_predsucc_handler(get_predsucc_args *gpa, 
    get_predsucc_ret *gpr)
{
  gpr->pred = loctable->pred(_me.id-1);
  if (gpa->m)
    gpr->v = loctable->succs(_me.id+1,gpa->m);
  gpa->src.timestamp = now();
  loctable->add_node(gpa->src);
}

int
ChordAdapt::fix_succ_cb(bool b, get_predsucc_args *gpa, get_predsucc_ret *gpr)
{
  int ret_sz = 0;
  if (alive()) {
    vector<IDMap> scs = loctable->succs(_me.id + 1, _nsucc);
    NDEBUG(3) << "fix_succ_cb get " << gpr->v.size() << " succs, old succ " << gpa->n.ip << "," 
      << printID(gpa->n.id) << (b?" alive":" dead") 
      << " succsz " << scs.size() << "(" << print_succs(scs) 
      << ")" << endl;
    gpa->n.timestamp = now();
    if (b) {
      ret_sz = PKT_SZ(1+gpr->v.size(),0);
      IDMap succ = loctable->succ(_me.id+1);
      loctable->update_ifexists(gpa->n);
      if (gpr->pred.ip == _me.ip) {
      }else if (gpr->pred.ip != _me.ip) {
	loctable->add_node(gpr->pred);
	IDMap newsucc = loctable->succ(_me.id+1);
      }
      consolidate_succ_list(gpa->n,scs,gpr->v);
      vector<IDMap> newscs = loctable->succs(_me.id+1,100);
      NDEBUG(3) << "fix_succ_cb pred " << gpr->pred.ip << " new succ " << (newscs.size()>0?newscs[0].ip:0) << "," << 
	(newscs.size()>0?printID(newscs[0].id):"??") << " succsz " << newscs.size() << "(" <<
	print_succs(newscs) << ")" << " retsz " << ret_sz << " newsz " << gpr->v.size() << endl;

      //delaycb(200,&ChordAdapt::fix_pred,(void *)0);

    } else {
      loctable->del_node(gpa->n); //XXX: don't delete after one try?
      delaycb(200,&ChordAdapt::fix_succ, (void *)0);
    }
  }else{
    _stab_basic_running = false;
  }
  delete gpa;
  delete gpr;
  return ret_sz;
}

void
ChordAdapt::consolidate_succ_list(IDMap n, vector<IDMap> oldlist, vector<IDMap> newlist)
{
  if (now() == 4311349 && _me.ip == 1175) 
    fprintf(stderr,"heng!\n");
  uint oldi = 0, newi = 0;
  while (oldi < oldlist.size()) {
    if (oldlist[oldi].ip == n.ip)
      break;
    oldi++;
  }
  oldi++;
  while (1) {

    if (newi >= newlist.size())
      break;
    if (oldi >= oldlist.size()) {
      while (oldi < _nsucc && newi < newlist.size()) {
	loctable->add_node(newlist[newi],true);
	newi++;
	oldi++;
      }
      if (oldi == _nsucc) 
	loctable->last_succ(newlist[(newi-1)]);
      while (newi < newlist.size()) {
	loctable->add_node(newlist[newi]);
	newi++;
      }
      break;
    }
    if (oldlist[oldi].ip == newlist[newi].ip) {
      loctable->add_node(newlist[newi],true);
      newi++;
      oldi++;
    }else if (ConsistentHash::between(_me.id, newlist[newi].id, oldlist[oldi].id)) {
      oldlist[oldi].timestamp = now();
      loctable->del_node(oldlist[oldi]);//XXX the timestamp is not very correct
      oldi++;
    }else {
      loctable->add_node(newlist[newi],true);
      newi++;
    }

  }
  vector<IDMap> updated = loctable->succs(_me.id+1,_nsucc);
  NDEBUG(4) << "consolidate fix_succ : old (" <<print_succs(oldlist) << ") new: ("
    << print_succs(newlist) << ") updated (" << print_succs(updated)
    << ")" << endl;
}


/* ---------------------------- empty queue ------------------------------ */
void
ChordAdapt::empty_cb(void *x)  //wrapper function
{
  ChordAdapt *c = (ChordAdapt *)x;
  return c->empty_queue(NULL);
}

void
ChordAdapt::empty_queue(void *a) 
{

  if (loctable->size() < 3) {
    NDEBUG(4) << "empty_queue locsz " << loctable->size() 
      << " reschedule join" << endl;
    if (!_join_scheduled || (now()-_join_scheduled)>20000)
      delaycb(0,&ChordAdapt::join,(Args *)0);
    return;
  }

  if (now()>=_next_adjust)
    adjust_parallelism();
  _empty_times++;

  double ppp = _parallelism > 1?(exp(log(0.1)/(double)_parallelism)):0.1;
  Time timeout = est_timeout(ppp);
  if (timeout && timeout < _stab_basic_timer) {
    NDEBUG(4) << " wierd para " << _parallelism << " timeout " << timeout 
      << " deadsz " << _dead_stat.size() << " livesz " << _live_stat.size() << endl;
  }

  IDMap pred, next;
  pred.ip = _me.ip;
  Time tt = timeout;
  Time oldest;

  while (pred.ip == _me.ip) {
    oldest = loctable->pred_biggest_gap(pred, next, _max_succ_gap, tt);
    tt = tt/2;
    if (tt == 0) break;
  }
  if (pred.ip == _me.ip && tt == 0) {
    NDEBUG(4) << "nothing to learn " << endl;
    return;
  }

  learn_args *la = new learn_args;
  learn_ret *lr = new learn_ret;
  Time to = _stab_basic_timer;


  la->m = _learn_num;
  la->timeout = timeout;
  la->n = pred;
  la->src = _me;
  la->end = next;

  NDEBUG(2) << "empty_queue quota " << _rate_queue->quota() << " succsz " << loctable->succ_size() 
    << " locsz " << loctable->size() << " livesz " 
    << loctable->live_size() << " locsz_used " 
    << loctable->size(LOC_HEALTHY,timeout) << " livesz_used " << loctable->live_size(timeout)
    << " learn from " << la->n.ip << "," 
    << printID(la->n.id) << " old " << (now()-la->n.timestamp) 
    << " para " << _parallelism << " est " << timeout << " tt " << tt << " oldest " << oldest
    << " statsz " << (_dead_stat.size()+_live_stat.size()) 
    << " end " << la->end.ip << endl;

  _rate_queue->do_rpc(pred.ip, &ChordAdapt::learn_handler, 
      &ChordAdapt::learn_cb, la, lr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), PKT_SZ(la->m,0),TIMEOUT(_me.ip,pred.ip));
}

void
ChordAdapt::learn_handler(learn_args *la, learn_ret *lr)
{
  la->src.timestamp = now();
  loctable->add_node(la->src);
 
  int stat = loctable->find_node(la->end);
  IDMap succ = loctable->succ(_me.id + 1);

  lr->stat = stat;
  if (ConsistentHash::between(_me.id, succ.id, la->end.id)) 
    lr->stat = LOC_DEAD;

  if (lr->stat < LOC_DEAD) {
    lr->v = loctable->get_closest_in_gap(la->m, la->end.id, la->src, la->timeout);
  }else{
    lr->v = loctable->succs(_me.id+1,la->m);
  }
  if (lr->v.size() == 0 && la->end.ip!=succ.ip){
    fprintf(stderr,"%u learn_handler from %u, end is %u, my succ is %u locsz %u requested timeout %u\n",
	_me.ip, la->src.ip, la->end.ip, succ.ip, loctable->size(),la->timeout);
    IDMap succ = loctable->succ(_me.id+1);
    fprintf(stderr,"haha %u\n",succ.ip);
  }
}

int
ChordAdapt::learn_cb(bool b, learn_args *la, learn_ret *lr)
{
  uint ret_sz = 0;
  if (alive()) {
    add_stat(now()-la->n.timestamp,b);
    la->n.timestamp = now();
    if (b) {
      loctable->update_ifexists(la->n);
      if (lr->stat < LOC_DEAD) {
	ConsistentHash::CHID gap = (la->end.id - la->n.id);
	if ((lr->v.size() == 0) && (gap > _max_succ_gap))
	  _max_succ_gap = gap;
      }else{
	IDMap deadnode = la->end;
	while (1) {
	  loctable->del_node(deadnode);
	  deadnode = loctable->succ(deadnode.id+1);
	  if (!deadnode.ip || !lr->v.size() || !ConsistentHash::between(la->n.id,lr->v[lr->v.size()-1].id,deadnode.id))
	    break;
	}
	deadnode = la->end;
      }
      for (uint i = 0; i < lr->v.size(); i++)  {
	IDMap xx = loctable->succ(lr->v[i].id,LOC_HEALTHY);
	if (xx.ip == lr->v[i].ip && xx.timestamp < lr->v[i].timestamp)
	  add_stat(lr->v[i].timestamp - xx.timestamp, true);
	loctable->add_node(lr->v[i]);
      }
      NDEBUG(4) << "learn_cb quota " << _rate_queue->quota() << " locsz " << loctable->size() <<" livesz " << loctable->live_size() << " succ_sz " << loctable->succ_size() 
	<< " max_gap " << printID(_max_succ_gap) << "this_gap " 
	<< printID(la->end.id - la->n.id) << "learn_cb " << la->m 
	<< " from " << la->n.ip 
	<< (lr->stat<LOC_DEAD?" alive ":" dead ") << la->end.ip << " sz " << lr->v.size() << ": " << print_succs(lr->v) << endl;

      ret_sz = PKT_SZ(lr->v.size()+1,0);
    }else{
      uint bsz = loctable->size();
      loctable->del_node(la->n); //XXX: should not delete a finger after one failure
      NDEBUG(4) << " learn_cb quota " << _rate_queue->quota() << " node " << la->n.ip << " dead! locsz " 
	<< bsz << "," << loctable->size() << " livesz " << loctable->live_size() << endl;
     // uint x = loctable->add_check(la->n);
      //assert(x > LOC_HEALTHY);
      ret_sz = PKT_OVERHEAD;
    }
    if (_rate_queue->empty()) 
      delaycb(0, &ChordAdapt::empty_queue, (void *)0);
  }
  delete la;
  delete lr;
  return ret_sz;
}

string
ChordAdapt::print_succs(vector<IDMap> v)
{
  char buf[1024];
  char *b = buf;
  b += sprintf(b," ");
  for (uint i = 0; i < v.size(); i++) {
    if (i == v.size()-1) { 
      b += sprintf(b,"%u", v[i].ip);
    }else{
      b += sprintf(b,"%u,",v[i].ip);
    }
  }
  return string(buf);
}

string
ChordAdapt::printID(ConsistentHash::CHID id)
{
  char buf[128];
  sprintf(buf,"%qx ",id);
  return string(buf);
}

bool 
ChordAdapt::check_pred_correctness(ConsistentHash::CHID k, IDMap n)
{
  IDMap tmp;
  tmp.id = k;
  uint pos = upper_bound(ids.begin(),ids.end(),tmp, IDMap::cmp) - ids.begin();
  if (pos)
    pos--;
  else
    pos = ids.size()-1;
  if (ids[pos].ip == n.ip) 
    return true;
  else {
    NDEBUG(4) << "key " << printID(k) << "wrong " << n.ip << "," 
      << printID(n.id) << "right " << ids[pos].ip << "," << 
      printID(ids[pos].id) << endl;
    return false;
  }
}

void
ChordAdapt::adjust_parallelism()
{
  /*
  _parallelism = 1;
  return;
  */
  uint old_p = _parallelism;
  if (_empty_times > _lookup_times)
    _parallelism++;
  else if (!_empty_times && _lookup_times)
    _parallelism = _parallelism/2;

  if (_parallelism < 1)
    _parallelism = 1;
  NDEBUG(4) << "adjust_parallelism from " << old_p << " to " << 
    _parallelism << " empty_times " << _empty_times << " lookup_times " 
    << _lookup_times << " maxp " << _max_p << endl;

  _empty_times = 0;
  _lookup_times = 0;
  while (_next_adjust < now()) 
    _next_adjust += _adjust_interval;
  if (_parallelism > _max_p)
    _parallelism = _max_p;
}

void
ChordAdapt::add_stat(Time t, bool live)
{
  if (t == 0) return;
  if (live) {
    _live_stat.push_back(t);
  }else{
    _dead_stat.push_back(t);
  }
  uint lsz = _live_stat.size(), dsz = _dead_stat.size();
  if ((_live_stat.size()+_dead_stat.size())> 1000) {
    if (_dead_stat.size() == 0) {
      _live_stat.erase(_live_stat.begin());
    }else if (_live_stat.size() == 0) {
      _dead_stat.erase(_dead_stat.begin());
    }else if (_live_stat.front() < _dead_stat.front())
      _live_stat.erase(_live_stat.begin());
    else
      _dead_stat.erase(_dead_stat.begin());
  }
}

Time
ChordAdapt::est_timeout(double prob)
{
//  return 0;
  if ((now()-_last_calculated > (_adjust_interval)) 
      && ((_live_stat.size()+_dead_stat.size()) > 50)) {
    sort_live.clear();
    sort_live.resize(_live_stat.size());
    for (uint i = 0; i < _live_stat.size(); i++)
      sort_live[i] = _live_stat[i];
    sort_dead.clear();
    sort_dead.resize(_dead_stat.size());
    for (uint i = 0; i < _dead_stat.size(); i++)
      sort_dead[i] = _dead_stat[i];

    sort(sort_live.begin(),sort_live.end());
    sort(sort_dead.begin(),sort_dead.end());

    for (uint i = 0; i < 10; i++)
      _calculated_prob[i] = 0;

    uint lsz = sort_live.size();
    uint dsz = sort_dead.size();
    uint live = lsz, dead = 0;
    uint i = 0, j = 0;
    Time tt;
    while (1) {
      if (i < lsz && j < dsz) {
	if (sort_live[i] < sort_dead[j]) {
	  tt = sort_live[i];
	  live--;
	  i++;
	}else{
	  tt = sort_dead[j];
	  dead++;
	  j++;
	}
      }else if (i < lsz) {
	tt = sort_live[i];
	live--;
	i++;
      }else if (j < dsz) {
	tt = sort_dead[j];
	dead++;
	j++;
      }else
	break;
      uint p = (uint)(10.0*dead/(double)(dead+live)) + 1;
      if (p>=10) 
	p = 9;
      _calculated_prob[p] = tt;
    }
    _last_calculated = now();
  }
  return _calculated_prob[(uint)(10*prob)];
}

#include "p2psim/bighashmap.cc"
