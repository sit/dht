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
#include "chordadapt.h"
#include <math.h>
#include <stdio.h>
#include <assert.h>

vector<uint> ChordAdapt::rtable_sz;

#define EST_TIMEOUT_SZ 100

vector<IDMap> ChordAdapt::ids;
bool ChordAdapt::sorted;
vector<double> ChordAdapt::sort_live;
vector<double> ChordAdapt::sort_dead;

ChordAdapt::ChordAdapt(IPAddress i, Args& a) : P2Protocol(i)
{
  _stab_basic_timer = a.nget<uint>("basictimer", 18000, 10);
  _fixed_lookup_to = (double)(a.nget<uint>("fixed_lookup_to",90,10))/100.0;
  _fixed_stab_to = (double)(a.nget<uint>("fixed_stab_to",100,10))/100.0;
  _fixed_stab_int = a.nget<uint>("fixed_stabtimer",0,10);
  if (_fixed_stab_int)
    _parallelism = a.nget<uint>("fixed_para",0,10);
  else
    _parallelism = 1;

  _bw_overhead = a.nget<uint>("overhead_rate", 10, 10);
  _burst_sz = a.nget<uint>("burst_size",0,10);
  uint _burst;
  if (!_burst_sz) {
    _burst = a.nget<uint>("burst",10,10);
    _burst_sz = _burst * _bw_overhead;
  }else
    _burst = _burst_sz/_bw_overhead;

  uint _min_bw_overhead = a.nget<uint>("min_rate",0,10);
  uint _max_bw_overhead = a.nget<uint>("max_rate",0,10);
  uint numnodes = Network::Instance()->gettopology()->num();
  if (_min_bw_overhead && _max_bw_overhead) {
    _bw_overhead = _min_bw_overhead + 
      (uint)((this->first_ip()-1)*((double)(_max_bw_overhead-_min_bw_overhead)/(double)numnodes));
    if (_bw_overhead == _min_bw_overhead)
      _special = 1;
    else
      _special = 0;
    _burst_sz = _burst * _bw_overhead;
    _adjust_interval = _burst * 500;
  }else {
    _adjust_interval = 500*_burst_sz/_bw_overhead;
    _special = 0;
  }

  _me.ip = ip();
  _me.id = ConsistentHash::ip2chid(_me.ip);
  _me.timestamp = 0;
  _me.alivetime = 0;
  _id = _me.id;


  _rate_queue = new RateControlQueue(this, (double)_bw_overhead, _burst_sz, _fixed_stab_int, ChordAdapt::empty_cb);

  _next_adjust = _adjust_interval;

  _last_calculated = 0;
  _stat.clear();
  for (uint i = 0;i < 10; i++) 
    _calculated_prob.push_back(1-0.1*i);

  _lookup_times = 0;
  _empty_times = 0;
  _nsucc = a.nget<uint>("successors",16,10);
  _to_multiplier = a.nget<uint>("timeout_multiplier", 3, 10);
  _learn_num = a.nget<uint>("learn_num",5,10);
  _max_p = _burst_sz/(40 + 8 * _learn_num);
  if (_max_p > 6)
    _max_p = 6;
  else if (_max_p < 1) 
    _max_p = 1;

  _stab_basic_running = false;
  _join_scheduled = 0;
  _last_stab = 0;

  loctable = new LocTable();
  loctable->init(_me);

  _wkn.ip = 0;

  ids.push_back(_me);

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
    if (ids.size() == 0)  {
      Node::print_stats();
      printf("<-----STATS----->\n");
      sort(rtable_sz.begin(),rtable_sz.end());
      uint totalrtable = 0;
      uint rsz = rtable_sz.size();
      for (uint i = 0; i < rsz; i++) 
	totalrtable += rtable_sz[i];
      printf("RTABLE:: 10p:%u 50p:%u 90p:%u avg:%.2f\n", rtable_sz[(uint)(0.1*rsz)], rtable_sz[(uint)(0.5*rsz)],
	  rtable_sz[(uint)(0.9*rsz)], (double)totalrtable/(double)rsz);
      printf("<-----ENDSTATS----->\n");
    }
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
    << " maxp " << _max_p << endl;
}

/* -------------- join --------------------- */
void
ChordAdapt::join(Args *args)
{
  if (!alive()) return;

  _last_bytes = 0;
  _last_bytes_time = now();

  while ((!_wkn.ip) || (!Network::Instance()->alive(_wkn.ip))) {
    _wkn.ip = Network::Instance()->getnode(ids[random()%ids.size()].ip)->ip();
    _wkn.id = Network::Instance()->getnode(_wkn.ip)->id();
    _wkn.alivetime = 0;
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
    if (!_fixed_stab_int) 
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
    _last_joined_time = 0;
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
  la->ori.timestamp = now()-_last_joined_time;
  if (!la->ori.timestamp)
    la->ori.timestamp = 1;
  la->no_drop = true;
  la->parallelism = 1;
  la->type = TYPE_JOIN_LOOKUP;
  la->learnsz = _learn_num;
  la->overshoot = 0;
  la->nexthop = _wkn;

  _rate_queue->do_rpc(_wkn.ip, &ChordAdapt::find_successors_handler,
      &ChordAdapt::null_cb, la, lr, (uint)0, la->type,
      PKT_SZ(1,1), PKT_SZ(2*la->m,1),TIMEOUT(_me.ip, _wkn.ip));
}

int
ChordAdapt::null_cb(bool b, lookup_args *a, lookup_ret *r)
{
  if (!a->nexthop.ip)
    abort();
  a->nexthop.timestamp = now();
  loctable->update_ifexists(a->nexthop);
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
  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip) {
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
    IDMap succ = loctable->succ(_me.id+1);
    IDMap pred = loctable->pred(_me.id-1);
    vector<IDMap> scs = loctable->succs(_me.id+1,100);
    NDEBUG(1) << "joined succ " << succ.ip << "," << printID(succ.id) 
      << "locsz " << loctable->size() << " livesz " 
      << loctable->live_size() << " succs: " << print_succs(scs) << 
      " scs: " << print_succs(lr->v)  << " pred " << la->from.ip 
      << "," << printID(la->from.id) << " mypred " << pred.ip << endl;
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
  la->src.alivetime = now()-_last_joined_time;
  la->end = _me;
  NDEBUG(2) << "join_learn from " << la->n.ip << "," 
    << printID(la->n.id) << endl;
  _rate_queue->do_rpc(min_n.ip, &ChordAdapt::learn_handler, 
      &ChordAdapt::learn_cb, la, lr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), PKT_SZ(2*la->m,0),TIMEOUT(_me.ip,min_n.ip));

}

void
ChordAdapt::find_successors_handler(lookup_args *la, lookup_ret *lr)
{
  if (la->nexthop.ip == _me.ip)
    la->nexthop.alivetime = now()-_last_joined_time;
  else
    abort();
  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip) {
    /*
      lookup_args *lla = new lookup_args;
      lla->src = _me;
      lla->src.alivetime = now()-_last_joined_time;
      lla->from = _me;
      lla->from.alivetime = now()-_last_joined_time;
      bcopy(la,lla,sizeof(lookup_args));
      lookup_ret *llr = new lookup_ret;
      llr->v.clear();
      lla->nexthop.ip = 0;
      _rate_queue->do_rpc(la->ori.ip, &ChordAdapt::join_handler,
	  &ChordAdapt::null_cb, lla, llr, 1, TYPE_JOIN_LOOKUP, 
	  PKT_SZ(2*llr->v.size(),0),PKT_SZ(0,0),TIMEOUT(_me.ip, la->ori.ip));
	  */
	NDEBUG(2) << "find_successors_handler failed for " << la->ori.ip 
	  << "," << printID(la->ori.id) << " not joined" << endl;
    }else{
      lookup_args lla;
      bcopy(la,&lla,sizeof(lookup_args));
      lla.src = _me;
      lla.src.alivetime = now()-_last_joined_time;
      lla.src.timestamp = now();
      lla.from = _me;
      lla.from.alivetime = lla.src.alivetime;
      lla.no_drop = true;
      NDEBUG(3)<<"find_successors_handler key " << printID(lla.key) << " from " 
	<< lla.ori.ip << "," << printID(lla.ori.id) << endl;
      next_recurs(&lla,NULL);
    }
  NDEBUG(5) << " find_successors_handler reply " << PKT_SZ(0,1) << " bytes to " 
    << la->ori.ip << " quota " << _rate_queue->quota() << endl;
}

/* ------------------------ crash ------------------------------------*/
void
ChordAdapt::crash(Args *args)
{
  double timeout;
  if (_fixed_stab_int) {
    timeout = _fixed_lookup_to;
  }else{
    double ppp = _parallelism > 1?(exp(log(0.1)/(double)_parallelism)):0.1;
    timeout = est_timeout(ppp);
  }

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
  _max_succ_gap = 0;
  _stat.clear();
  for (uint i = 0; i < 10; i++)
    _calculated_prob[i] = (1-0.1*i);
}

/* ------------------------ lookup ----------------------------------*/
void
ChordAdapt::lookup(Args *args)
{
  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip) {
    if (!_join_scheduled || (now()-_join_scheduled) > 20000) 
      delaycb(0,&ChordAdapt::join,(Args *)0);
    NDEBUG(2) << "lookup key failed not yet joined" << endl;
    record_lookup_stat(_me.ip, _me.ip, 0, false, false, 0, 0, 0);
    return;
  }

  lookup_args la;
  lookup_ret lr;
  bzero(&la,sizeof(lookup_args));
  la.key = args->nget<ConsistentHash::CHID>("key",0,16);
  la.no_drop = true;
  la.nexthop = _me;
  la.from = _me;
  la.from.alivetime = now()-_last_joined_time;
  la.src = _me;
  la.src.alivetime = now()-_last_joined_time;
  la.src.timestamp = now();
  la.ori.ip = 0;
  la.m = 1;
  la.parallelism = _parallelism;
  la.type = TYPE_USER_LOOKUP;
  la.learnsz = _learn_num;
  //la.deadnodes.clear();
  la.deadnode.ip = 0;
  lr.done = false;
  lr.v.clear();
  lr.is_succ = false;

  _outstanding_lookups.insert(la.key, now());
  NDEBUG(2) << "start lookup key " << printID(la.key) << endl;
  if (1) 
    next_recurs(&la,NULL);
  else 
    next_iter(&la,&lr);
}


/* ------------------------ lookup (iterative) ------------------------*/
void
ChordAdapt::next_iter(lookup_args *la, lookup_ret *lr)
{
  if (now() >= _next_adjust)
        adjust_parallelism();

  IDMap succ = loctable->succ(_me.id+1);
  if (ConsistentHash::between(_me.id,succ.id,la->key)) {
    la->from = _me;
    la->nexthop = _me;
    lr->v.push_back(succ);
    lr->done = true;
    donelookup_handler(la,lr);
    return;
  }else if (lr->done) {
    _sent.remove(la->src.id|la->key);
    _progress.remove(la->src.id|la->key);
    la->nexthop = _me;
    donelookup_handler(la,lr);
    return;
  }

  //estimate how many hops are left for this lookup
  /*
  uint est_n = ((ConsistentHash::CHID) -1 )/ConsistentHash::distance(_me.id,succ.id);
  est_n *= 2; //give it some slack
  //loctable->size is fast but not an accurate measure of routing table sz
  uint est_b = (uint)(loctable->size()/(2*log(est_n)));
  if (est_b < 2) 
    est_b = 2;
  uint est_hops = 0;
  ConsistentHash::CHID d = ConsistentHash::distance(la->nexthop.id,la->key);
  while (d > ConsistentHash::distance(_me.id,succ.id)) {
    d = d/est_b;
    est_hops++;
  }
  if (!est_hops)
    est_hops = 1;
  NDEBUG(4) << "next_iter est_hop " << est_hops << endl;
  */

  double ttt;
  int para;
  if (!_fixed_stab_int) {
    //calculate the parallelism needed
    para = (_rate_queue->quota()+_burst_sz)/(2*PKT_OVERHEAD+ 8*_learn_num);
   // para = para/est_hops;
    if (para > _parallelism)
      para = _parallelism;
    else if (para > la->parallelism)
      para = la->parallelism;
    else if (para <= 0)
      para = 1;
    double ppp = para > 1? (exp(log(0.1)/(double)para)):0.1;
    ttt = est_timeout(ppp);
  } else {
    para = _parallelism;
    ttt = _fixed_lookup_to;
  }

  vector<IDMap> nexthops = loctable->preds(la->key, para, LOC_HEALTHY, ttt);
  ConsistentHash::CHID mostprog = _progress.find(la->src.id|la->key);
  ConsistentHash::CHID sent = _sent.find(la->src.id|la->key);
  if (sent && ConsistentHash::between(mostprog,la->key,sent))
    mostprog = sent;

  uint sentout = 0;
  uint i=0,j=0;
  IDMap nh;
  ConsistentHash::CHID bestsent = mostprog;
  while (sentout < para) {
    if (i < nexthops.size()) 
      nh = nexthops[i++];
    else if (lr && j < lr->v.size()) 
      nh = lr->v[j++];
    else
      break;
    if ((!j && (!mostprog || ConsistentHash::between(mostprog,la->key,nh.id)))
      || ConsistentHash::between(bestsent,la->key,nh.id)) {
  //  l->push_back(nh.id);
      lookup_args *lla = new lookup_args;
      bcopy(la,lla,sizeof(lookup_args));
      /*
      for (uint iii = 0; iii < la->deadnodes.size();iii++)
	lla->deadnodes.push_back(la->deadnodes[i]);
	*/
      //lla->deadnodes = la->deadnodes;
      lla->timeout = ttt;
      lla->from.alivetime = now() - _last_joined_time;
      lla->nexthop = nh;
      lla->hops = la->hops+1;
      lookup_ret *llr = new lookup_ret;
      llr->is_succ = false;
      llr->done = false;
      llr->v.clear();
      NDEBUG(4) << " key moha " << printID(la->key) << " to " << nh.ip 
	<< "," << printID(nh.id) << " dead " 
	<< lla->deadnode.ip << endl;
      _rate_queue->do_rpc(nh.ip, &ChordAdapt::next,
	  &ChordAdapt::next_iter_cb, lla, llr, 0, TYPE_USER_LOOKUP, 
	  PKT_SZ(1,0), PKT_SZ(0,0),TIMEOUT(_me.ip, nh.ip));
      sentout++;
      if (!bestsent || ConsistentHash::between(bestsent,la->key,nh.id))
	bestsent = nh.id;
    }
  }
  if (bestsent)
    _sent.insert(la->src.id|la->key,bestsent);
  uint outstanding =  _forwarded_nodrop.find(la->src.id | la->key);

  NDEBUG(4) << "next_iter key " << printID(la->key) << " mostprog " 
    << printID(mostprog) << " from " << la->nexthop.ip
  << "," << printID(la->nexthop.id) << " qsz " << _rate_queue->size() 
  << " bestsent " << printID(bestsent)<< " " << para << "," << _parallelism 
  << "," << sentout<< "," << outstanding << endl;

  if (!outstanding && !sentout) {
    assert(nexthops.size());
    nh = nexthops[0];
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    //lla->deadnodes = la->deadnodes;
    lla->timeout = ttt;
    lla->from.alivetime = now() - _last_joined_time;
    lla->nexthop = nh;
    lla->hops = la->hops+1;
    lookup_ret *llr = new lookup_ret;
    llr->is_succ = false;
    llr->done = false;
    llr->v.clear();
    NDEBUG(4) << " key resend " << printID(la->key) << " to " << nh.ip 
      << "," << printID(nh.id) << " dead " 
      << lla->deadnode.ip << endl;
    _rate_queue->do_rpc(nh.ip, &ChordAdapt::next,
	&ChordAdapt::next_iter_cb, lla, llr, 0, TYPE_USER_LOOKUP, 
	PKT_SZ(1,0), PKT_SZ(0,0),TIMEOUT(_me.ip, nh.ip));
    sentout++;
  }
  _forwarded_nodrop.insert(la->src.id|la->key,outstanding+sentout);
}

void
ChordAdapt::next(lookup_args *la, lookup_ret *lr)
{
  loctable->update_ifexists(la->from,0);
  la->nexthop.alivetime = now() - _last_joined_time;

  if (la->deadnode.ip) {
    NDEBUG(4) << " key " << printID(la->key) << " from " << la->from.ip << " del " << 
      la->deadnode.ip << "," << printID(la->deadnode.id) << endl;
    loctable->del_node(la->deadnode,true);
  }

  IDMap succ = loctable->succ(_me.id+1);
  if (ConsistentHash::between(_me.id,succ.id,la->key)) {
    lr->done = true;
    la->from = _me;
    lr->v = loctable->succs(_me.id+1,la->m);
  } else {
    assert(la->nexthop.ip == _me.ip);
    if (!_fixed_stab_int) 
      lr->v = loctable->preds(la->key, 
	  _rate_queue->critical()?1:la->learnsz, LOC_HEALTHY, la->timeout);
    else
      lr->v = loctable->preds(la->key, la->learnsz, 
	  LOC_HEALTHY, _fixed_stab_to);
  }
}

int
ChordAdapt::next_iter_cb(bool b, lookup_args *la, lookup_ret *lr)
{
  int ret_sz = 0;
  if (alive()) {
    Time delta = now()-la->nexthop.timestamp;
    if (delta < la->nexthop.alivetime && delta > 5000)
      add_stat((double)(la->nexthop.alivetime-delta)/(double)(la->nexthop.alivetime), b);
    la->nexthop.timestamp = now();
    if ((b) && (la->nexthop.ip!=_me.ip)){
      /*
      if ((lr->is_succ) && (lr->v.size() > 0)){
	loctable->update_ifexists(la->nexthop,ConsistentHash::distance(la->nexthop.id,lr->v[lr->v.size()-1].id));
	vector<IDMap> oldlist = loctable->between(la->nexthop.id+1,lr->v[lr->v.size()-1].id);
	consolidate_succ_list(la->nexthop,oldlist,lr->v,false);
	vector<IDMap> newlist = loctable->between(la->nexthop.id+1,lr->v[lr->v.size()-1].id);
	NDEBUG(4) << "next_recurs_cb After consolidate: " << print_succs(newlist) << endl;
//	for (uint i = 1; i < lr->v.size(); i++) 
//	  loctable->update_ifexists(lr->v[i-1],lr->v[i].ip);
      } else {
      */
	loctable->update_ifexists(la->nexthop,0);
	for (uint i = 0; i < lr->v.size(); i++)  {
	  /*
	  IDMap xx = loctable->succ(lr->v[i].id,LOC_HEALTHY);
	  if (xx.ip == lr->v[i].ip && xx.timestamp < lr->v[i].timestamp && lr->v[i].timestamp - xx.timestamp > 5000)
	    add_stat((double)xx.alivetime/(double)(lr->v[i].timestamp - xx.timestamp+xx.alivetime), true);
	    */
	  loctable->add_node(lr->v[i]);
	}
      //}
      NDEBUG(4) << "next_iter_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from " << la->nexthop.ip
	<< "," << printID(la->nexthop.id) << " learnsz " << la->learnsz << " learnt " << lr->v.size() << ": " 
	<< print_succs(lr->v) << " nodes " << printID(lr->v.size()?lr->v[0].id:0) << ","<<lr->v[0].alivetime
	<<","<< (now()-lr->v[0].timestamp) << " overshoot " << printID(la->overshoot) << " locsz " 
	<< loctable->size(LOC_HEALTHY,0.9) << " livesz " << loctable->live_size(0.9) << " done? " << (lr->done?1:0)<< endl;
      ret_sz = PKT_SZ(2*lr->v.size(),0);
    } else {
      la->to_num++;
      la->to_lat += TIMEOUT(_me.ip, la->nexthop.ip);
      if (!la->deadnode.ip || ConsistentHash::between(la->deadnode.id,la->key,la->nexthop.id)) 
	la->deadnode = la->nexthop;
      loctable->del_node(la->nexthop);
      ConsistentHash::CHID bestsent = _sent.find(la->src.id|la->key);
      if (bestsent == la->nexthop.id)
	_sent.remove(la->src.id|la->key);
      ret_sz = 0;
      NDEBUG(4) << "next_iter_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from " << la->nexthop.ip
	<< "," << printID(la->nexthop.id) << " dead " << endl;
    }
    uint outstanding =  _forwarded_nodrop.find(la->src.id | la->key);
    outstanding--;
    _forwarded_nodrop.insert(la->src.id|la->key,outstanding);
    ConsistentHash::CHID prog = _progress.find(la->src.id|la->key);
    if (b && (!prog || ConsistentHash::between(prog,la->key,la->nexthop.id))) { 
      _progress.insert(la->src.id|la->key,la->nexthop.id);
      next_iter(la,lr);
    }else if (!outstanding &&  _outstanding_lookups.find(la->key)) 
      next_iter(la,lr);
  }
  delete la;
  if (lr) delete lr;
  return ret_sz;
}

/* ------------------------ lookup (recursive) ----------------------- */
void
ChordAdapt::donelookup_handler(lookup_args *la, lookup_ret *lr)
{
  if (la->nexthop.ip == _me.ip) {
    la->nexthop.alivetime = now() - _last_joined_time;
  }else
    abort();

  if (la->from.ip!=_me.ip) {
    la->from.timestamp = now();
    loctable->add_node(la->from);
  }
  if (la->ori.ip) {
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    la->nexthop = la->ori;
    lookup_ret *llr = new lookup_ret;
    llr->v = lr->v;
    if (lr->v.size() > 0) {
      la->ori.timestamp = now();
      loctable->add_node(la->ori);
    }
    _rate_queue->do_rpc(la->ori.ip, &ChordAdapt::join_handler,
	&ChordAdapt::null_cb, lla, llr, 0, TYPE_JOIN_LOOKUP, 
	PKT_SZ(2*llr->v.size(),0), PKT_SZ(0,0),TIMEOUT(_me.ip, la->ori.ip));
     return;
  };

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
      if (now()-t > 4000) {
	record_lookup_stat(_me.ip, la->from.ip, now()-t, false,
	  false, la->hops, la->to_num, la->to_lat);
      }else{
	record_lookup_stat(_me.ip, la->from.ip, now()-t, true,
	  b, la->hops, la->to_num, la->to_lat);
      }
      for (uint i = 0; i < lr->v.size();i++)
	loctable->add_node(lr->v[i]);
    }
    _outstanding_lookups.remove(la->key);
  }
}

void
ChordAdapt::next_recurs(lookup_args *la, lookup_ret *lr)
{
  if (lr)
    lr->done = true;

  IDMap succ = loctable->succ(_me.id+1);

  //update my alivetime
  //XXX bug??
  la->nexthop.alivetime = now() - _last_joined_time;

  //learn the src node if this query
  la->src.timestamp = now();
  loctable->add_node(la->src);
  la->from.timestamp = now();
  loctable->add_node(la->from);
  if (lr && la->overshoot) {
    //lr->v = loctable->get_closest_in_gap(la->learnsz, la->key, la->from);
    if (_rate_queue->very_critical()) {
      if (la->overshoot!=succ.id) {
	lr->v.push_back(succ);
      }else{
	lr->v.clear();
      }
      lr->is_succ = true;
    }else {
      if (_rate_queue->critical())
	la->learnsz = 1;
      else
	la->learnsz = _learn_num;
      if (!_fixed_stab_int)
	lr->v = loctable->get_closest_in_gap(la->learnsz, la->overshoot, la->from, 20*80*1000/_bw_overhead,la->timeout?la->timeout:est_timeout(0.1));
      else
	lr->v = loctable->get_closest_in_gap(la->learnsz, la->overshoot, la->from, 20*_fixed_stab_int,_fixed_stab_to);

      if (!lr->v.size()) {
	if (!_rate_queue->critical())
	  lr->v = loctable->succs(_me.id+1,la->learnsz);
	else
	  lr->v.clear();
	lr->is_succ = true;
      }else{
	lr->is_succ = false;
      }
    }
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
    lla->from.alivetime = now()-_last_joined_time;
    lla->nexthop = lla->src;
    lookup_ret *llr = new lookup_ret;
    llr->done = true;
    llr->v = loctable->succs(_me.id+1,la->m);
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << 
      la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip 
      << " no_drop? " << (la->no_drop?1:0) << " done succ " 
      << succ.ip << "," << printID(succ.id) 
      << " quota " << _rate_queue->quota() << " qsz " << _rate_queue->size() 
      << " hops " << lla->hops << endl;
    _rate_queue->do_rpc(lla->src.ip, &ChordAdapt::donelookup_handler,
	&ChordAdapt::null_cb, lla, llr, lla->ori.ip?0:1, lla->type, 
	PKT_SZ(2*llr->v.size(),0), PKT_SZ(0,0),TIMEOUT(_me.ip,lla->src.ip));
    return;
  }

  double ttt;
  int para;
  if (!_fixed_stab_int) {
    para = (_rate_queue->quota()+_burst_sz)/(2*(2*PKT_OVERHEAD+ 8*_learn_num));
    if (_rate_queue->critical())
      para = 1;
    if (para > _parallelism)
      para = _parallelism;
    else if (para > la->parallelism)
      para = la->parallelism;
    else if (para <= 0)
      para = 1;
    double ppp = para > 1? (exp(log(0.1)/(double)para)):0.1;
    ttt = est_timeout(ppp);
  }else{
    para = _parallelism;
    ttt = _fixed_lookup_to;
  }

  //vector<IDMap> nexthops = loctable->preds(la->key, para, LOC_HEALTHY, ttt);
  vector<IDMap> nexthops = loctable->next_close_hops(la->key, para, ttt);

  if (_fixed_stab_int)
    ttt = _fixed_stab_to;
  else if (ttt > 0.5)
    ttt = est_timeout(0.5);

  uint nsz = nexthops.size();

  if (((nsz == 0) || (nexthops[0].ip == _me.ip)) && succ.ip) {
    fprintf(stderr,"%llu para %u _para %u shit! %u,%qx key %qx succ %qx\n",now(), para, _parallelism, _me.ip,_me.id, la->key, succ.id);
    abort();
  }

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
    } else {
      lla->no_drop = false;
    }
    if (_rate_queue->critical())
      lla->learnsz = 1;
    lla->timeout = ttt;
    lla->hops = la->hops+1;
    lla->nexthop = nexthops[i];
    lla->from = _me;
    //lla->overshoot = overshoot.id;
    lla->overshoot = (i>=1)?nexthops[i-1].id:la->key;
    lookup_ret *llr = new lookup_ret;
    llr->v.clear();
    NDEBUG(3) << "next_recurs key " << printID(la->key) << " quota " << _rate_queue->quota() << " qsz " 
      << _rate_queue->size() << " locsz " << loctable->size(LOC_HEALTHY, ttt) 
      << " livesz " << loctable->live_size(ttt) 
      << " src " << la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip << 
      " forward to next " << nexthops[i].ip << "," << printID(nexthops[i].id)  << "," 
      << now()-nexthops[i].timestamp << "," << nexthops[i].alivetime << " to " 
      << (double)nexthops[i].alivetime/(double)(nexthops[i].alivetime+ now()-nexthops[i].timestamp)
      << " hops " << lla->hops 
      << " est_to " << ttt << " est_sz " << _stat.size() 
      << " nsz " << nsz << " i " << i << " nodrop " << lla->no_drop << endl;
    if (nexthops[i].ip == _me.ip)  {
      fprintf(stderr,"what?! loop?!!!\n");
      exit(3);
    }
    sent_success = _rate_queue->do_rpc(nexthops[i].ip, &ChordAdapt::next_recurs,
	&ChordAdapt::next_recurs_cb, lla,llr, lla->ori.ip?0:(lla->no_drop?1:3), lla->type,
	PKT_SZ(1,0), PKT_SZ(2*lla->learnsz,0),TIMEOUT(_me.ip,nexthops[i].ip));
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
    Time delta = now()-la->nexthop.timestamp;
    if (delta < la->nexthop.alivetime && delta > 5000)
      add_stat((double)(la->nexthop.alivetime-delta)/(double)(la->nexthop.alivetime), b);
    la->nexthop.timestamp = now();
    if ((b) && (la->nexthop.ip!=_me.ip)){
      if ((lr->is_succ) && (lr->v.size() > 0)){
	loctable->update_ifexists(la->nexthop,ConsistentHash::distance(la->nexthop.id,lr->v[lr->v.size()-1].id));
	vector<IDMap> oldlist = loctable->between(la->nexthop.id+1,lr->v[lr->v.size()-1].id);
	consolidate_succ_list(la->nexthop,oldlist,lr->v,false);
	vector<IDMap> newlist = loctable->between(la->nexthop.id+1,lr->v[lr->v.size()-1].id);
	NDEBUG(4) << "next_recurs_cb After consolidate: " << print_succs(newlist) << endl;
	/*
	for (uint i = 1; i < lr->v.size(); i++) 
	  loctable->update_ifexists(lr->v[i-1],lr->v[i].ip);
	  */
      } else {
	loctable->update_ifexists(la->nexthop,0);
	for (uint i = 0; i < lr->v.size(); i++)  {
	  IDMap xx = loctable->succ(lr->v[i].id,LOC_HEALTHY);
	  if (xx.ip == lr->v[i].ip && xx.timestamp < lr->v[i].timestamp && lr->v[i].timestamp - xx.timestamp > 5000)
	    add_stat((double)xx.alivetime/(double)(lr->v[i].timestamp - xx.timestamp+xx.alivetime), true);
	  loctable->add_node(lr->v[i]);
	}
      }
      NDEBUG(4) << "next_recurs_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from " << la->nexthop.ip
	<< "," << printID(la->nexthop.id) << " learnt " << lr->v.size() << ": " 
	<< print_succs(lr->v) << " nodes is_succ " << (lr->is_succ?1:0) 
	<< " overshoot " << printID(la->overshoot) << " locsz " 
	<< loctable->size(LOC_HEALTHY,0.9) << " livesz " << loctable->live_size(0.9) << endl;
      ret_sz = PKT_SZ(2*lr->v.size(),0);
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
	  la->parallelism = 1;
	  NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	    << la->src.ip << " ori " << la->ori.ip << " from "
	    << la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	    la->ori.ip << (b?" live":" dead") << " restransmit" << endl;
	  next_recurs(la,lr);
       } else {
	 NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	   << la->src.ip << " ori " << la->ori.ip << " from "
	   << la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	   la->ori.ip << (b?" live":" dead") << " outstanding " << (outstanding-1) << endl; 
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
ChordAdapt::fix_pred(void *a)
{
  if (!alive()) return;
  IDMap pred = loctable->pred(_me.id-1);
  get_predsucc_args *gpa = new get_predsucc_args;
  get_predsucc_ret *gpr = new get_predsucc_ret;
  gpa->m = 1;
  gpa->n = pred;
  gpa->src = _me;
  gpa->src.alivetime = now()-_last_joined_time;
  gpr->v.clear();
  NDEBUG(3) << " fix_pred " << pred.ip << "," << printID(pred.id) 
    << " quota " << _rate_queue->quota() << endl;
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::get_predsucc_handler,
      &ChordAdapt::fix_pred_cb, gpa, gpr, 0, TYPE_FIXPRED_UP, PKT_SZ(0,1), PKT_SZ(2,1),
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
      ret_sz = PKT_SZ(2,0);
      loctable->update_ifexists(gpa->n);
      if (gpr->v.size()>0) 
	loctable->add_node(gpr->v[0]);
    } else {
      loctable->del_node(gpa->n,true);
    } 
    delaycb(10000,&ChordAdapt::fix_succ,(void*)0);
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
  gpa->src.alivetime = now()-_last_joined_time;

  vector<IDMap> scs = loctable->succs(_me.id+1,_nsucc);
  if (scs.size() > _nsucc/2) 
    gpa->m = 1;
  else
    gpa->m= _nsucc;

  NDEBUG(2) << "fix_succ succ " << succ.ip << "," << printID(succ.id) << endl;
  _rate_queue->do_rpc(succ.ip, &ChordAdapt::get_predsucc_handler,
	&ChordAdapt::fix_succ_cb, gpa,gpr, 0, TYPE_FIXSUCC_UP, PKT_SZ(0,1), PKT_SZ(2*gpa->m,0),
	TIMEOUT(_me.ip,succ.ip));
}

void
ChordAdapt::get_predsucc_handler(get_predsucc_args *gpa, 
    get_predsucc_ret *gpr)
{
  if (gpa->n.ip == _me.ip) 
    gpa->n.alivetime = now()-_last_joined_time;
  else
    abort();
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
      ret_sz = PKT_SZ(1+2*gpr->v.size(),0);
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
ChordAdapt::consolidate_succ_list(IDMap n, vector<IDMap> oldlist, vector<IDMap> newlist, bool is_succ)
{
  IDMap sss;
  IDMap mee;
  uint oldi = 0, newi = 0;
  if (is_succ) {
    while (oldi < oldlist.size()) {
      if (oldlist[oldi].ip == n.ip)
	break;
      oldi++;
    }
    oldi++;
    mee = _me;
  }else{
    mee = n;
  }
  while (1) {

    if (newi >= newlist.size())
      break;
    if (oldi >= oldlist.size()) {
      if (is_succ)  { // i need to clean up some shit
	sss = loctable->succ(oldlist[oldi-1].id);
      }
      while (newi < newlist.size()) {
	if (is_succ) {
	  while (newlist[newi].ip!=sss.ip && ConsistentHash::between(mee.id,newlist[newi].id,sss.id)) {
	    sss.timestamp = now();
	    loctable->del_node(sss);
	    sss = loctable->succ(sss.id+1);
	  }
	}
	loctable->add_node(newlist[newi],is_succ);
	newi++;
	oldi++;
	if (is_succ) {
	  sss = loctable->succ(newlist[newi-1].id+1);
	}
	if (is_succ && oldi == _nsucc) 
	  break;
      }
      if (is_succ && oldi == _nsucc) 
	loctable->last_succ(newlist[(newi-1)]);
      while (newi < newlist.size()) {
	loctable->add_node(newlist[newi]);
	newi++;
      }
      break;
    }
    if (oldlist[oldi].ip == newlist[newi].ip) {
      loctable->add_node(newlist[newi],is_succ);
      newi++;
      oldi++;
    }else if (ConsistentHash::between(_me.id, newlist[newi].id, oldlist[oldi].id)) {
      oldlist[oldi].timestamp = now();
      loctable->del_node(oldlist[oldi]);//XXX the timestamp is not very correct
      oldi++;
    }else {
      loctable->add_node(newlist[newi],is_succ);
      newi++;
    }

  }
  if (is_succ) {
    vector<IDMap> updated = loctable->succs(_me.id+1,_nsucc);
    NDEBUG(4) << "consolidate fix_succ : old (" <<print_succs(oldlist) << ") new: ("
      << print_succs(newlist) << ") updated (" << print_succs(updated)
      << ")" << endl;
  }
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
  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip){
    if (!_join_scheduled || (now()-_join_scheduled)>20000) {
      NDEBUG(4) << "empty_queue locsz " << loctable->size() 
	<< " reschedule join" << endl;
      delaycb(0,&ChordAdapt::join,(Args *)0);
    }
    return;
  }

  if (now()>=_next_adjust)
    adjust_parallelism();
  _empty_times++;

  double tt;
  if (_fixed_stab_int) {
    tt = _fixed_stab_to;
  }else {
    double ppp = _parallelism > 1?(exp(log(0.1)/(double)_parallelism)):0.1;
    if (ppp > 0.5)
      tt = est_timeout(0.5);
    else
      tt = est_timeout(ppp);
  }

  IDMap pred, next;
  pred.ip = _me.ip;

  double oldest;
  if (_fixed_stab_int) 
    oldest = loctable->pred_biggest_gap(pred, next, 20*_fixed_stab_int, tt);
  else 
    oldest = loctable->pred_biggest_gap(pred, next, 20*80*1000/_bw_overhead, tt);
  
  if (pred.ip == _me.ip || !pred.ip) {
    NDEBUG(4) << "nothing to learn oldest " << oldest << endl;
    return;
  }

  learn_args *la = new learn_args;
  learn_ret *lr = new learn_ret;
  Time to = _stab_basic_timer;


  la->m = _learn_num;
  la->timeout = tt;
  la->n = pred;
  la->src = _me;
  la->src.alivetime = now()-_last_joined_time;
  la->end = next;

  NDEBUG(2) << "empty_queue quota " << _rate_queue->quota() << " succsz " << loctable->succ_size() 
    << " locsz " << loctable->size() << " livesz " 
    << loctable->live_size() << " locsz_used " 
    << loctable->size(LOC_HEALTHY,tt) << " livesz_used " << loctable->live_size(tt)
    << " learn from " << la->n.ip << "," 
    << printID(la->n.id) << " old " << (now()-la->n.timestamp) 
    << " para " << _parallelism << " est_tt " << tt << " oldest " << oldest
    << " statsz " << _stat.size() 
    << " end " << la->end.ip << endl;

  _rate_queue->do_rpc(pred.ip, &ChordAdapt::learn_handler, 
      &ChordAdapt::learn_cb, la, lr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), PKT_SZ(2*la->m,0),TIMEOUT(_me.ip,pred.ip));
}

void
ChordAdapt::learn_handler(learn_args *la, learn_ret *lr)
{
  la->src.timestamp = now();
  loctable->add_node(la->src);
  if (la->n.ip == _me.ip) 
    la->n.alivetime = now()-_last_joined_time;
  else
    abort();

  if (_rate_queue->very_critical()) {
    IDMap succ = loctable->succ(_me.id+1);
    if (la->end.id != succ.id) 
      lr->v.push_back(succ);
    lr->is_succ = true;
  }else {
    /*
    if (_rate_queue->critical())
      la->m = 1;
      */
    lr->v = loctable->get_closest_in_gap(la->m, la->end.id, la->src, 20*80*1000/_bw_overhead, la->timeout);
    if (!lr->v.size()) {
      if (!_rate_queue->critical())
	lr->v = loctable->succs(_me.id+1,la->m);
      else
	lr->v.clear();
      lr->is_succ = true;
    }else{
      lr->is_succ = false;
    }
  }
}

int
ChordAdapt::learn_cb(bool b, learn_args *la, learn_ret *lr)
{
  uint ret_sz = 0;
  if (alive()) {
    Time delta = now()-la->n.timestamp;
    if (delta < la->n.alivetime && delta > 5000)
      add_stat((double)(la->n.alivetime-delta)/(double)(la->n.alivetime),b);
    la->n.timestamp = now();
    if (b) {
      IDMap neighborsucc;
      if (lr->is_succ) {
	if (lr->v.size()>0) 
	  neighborsucc = lr->v[0];
	else
	  neighborsucc = la->end;
	loctable->update_ifexists(la->n,ConsistentHash::distance(la->n.id,neighborsucc.id));
	ConsistentHash::CHID gap = ConsistentHash::distance(la->n.id,neighborsucc.id);
	_max_succ_gap = gap;
	if (lr->v.size() > 0) {
	  vector<IDMap> oldlist = loctable->between(la->n.id+1,lr->v[lr->v.size()-1].id);
	  consolidate_succ_list(la->n,oldlist,lr->v,false);
	  if (p2psim_verbose) {
	    vector<IDMap> newlist = loctable->between(la->n.id+1,lr->v[lr->v.size()-1].id);
	    NDEBUG(4) << "learn_cb After consolidate " << la->n.ip << "," << la->n.alivetime << " : " << print_succs(newlist) << endl;
	  }
	}
	/*
	for (uint i = 1; i < lr->v.size();i++) 
	  loctable->update_ifexists(lr->v[i-1],lr->v[i].ip);
	  */
      } else {
	neighborsucc = lr->v[0];
	loctable->update_ifexists(la->n);
	for (uint i = 0; i < lr->v.size(); i++)  {
	  IDMap xx = loctable->succ(lr->v[i].id,LOC_HEALTHY);
	  if (xx.ip == lr->v[i].ip && xx.timestamp < lr->v[i].timestamp)
	    add_stat((double)xx.alivetime/(double)(lr->v[i].timestamp - xx.timestamp+xx.alivetime), true);
	  loctable->add_node(lr->v[i]);
	}
      }
      NDEBUG(4) << "learn_cb quota " << _rate_queue->quota() << " locsz " 
	<< loctable->size(LOC_HEALTHY,0.9) <<" livesz " << loctable->live_size(0.9) 
	<< "learn_cb " << la->m 
	<< " from " << la->n.ip << ":" << la->end.ip << " " << printID(la->n.id) << ":" <<  printID(la->end.id)
	<< " is_succ " << (lr->is_succ?1:0) 
	<< " sz " << lr->v.size() << " ex " << neighborsucc.alivetime << "," << now()-neighborsucc.timestamp << " : " << print_succs(lr->v) << endl;

      ret_sz = PKT_SZ(2*lr->v.size()+1,0);
    }else{
      loctable->del_node(la->n); //XXX: should not delete a finger after one failure
      NDEBUG(4) << " learn_cb quota " << _rate_queue->quota() << " node " << la->n.ip 
	<< "," << printID(la->n.id)<< "," << (now()-la->n.timestamp) << "," << la->n.alivetime << "," <<
	(double)(la->n.alivetime)/(double)(now()-la->n.timestamp+la->n.alivetime) << " dead! locsz " 
	<< loctable->size(LOC_HEALTHY,la->timeout) << " livesz " << loctable->live_size(la->timeout) << endl;
    }
    if (_rate_queue->empty() && !_fixed_stab_int) 
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
  if ((ids[pos].ip == n.ip) || (!Network::Instance()->alive(n.ip)))
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
  uint old_p = _parallelism;
  if (!_fixed_stab_int) {
    if (_empty_times > _lookup_times)
      _parallelism++;
    else if (!_empty_times && _lookup_times)
      _parallelism = _parallelism/2;

    if (_parallelism < 1)
      _parallelism = 1;
    else if (_parallelism > _max_p)
      _parallelism = _max_p;
  } 

  unsigned long b = Node::collect_stat()?Node::get_out_bw_stat():0;

  NDEBUG(4) << "adjust_parallelism from " << (_fixed_stab_int?111:old_p) 
    << " to " << _parallelism << " empty_times " << _empty_times << " lookup_times " 
    << _lookup_times << " bytes " << (b-_last_bytes) << " time " 
    << now()-_last_bytes_time << endl;

  _last_bytes = b;
  _last_bytes_time = now();

  _empty_times = 0;
  _lookup_times = 0;
  while (_next_adjust < now()) 
    _next_adjust += _adjust_interval;

  if ((Node::collect_stat()) && (now()-_last_joined_time>600000)){
    double ppp = _parallelism > 1? (exp(log(0.1)/(double)_parallelism)):0.1;
    double ttt = est_timeout(ppp);
    uint rsz = loctable->size(LOC_HEALTHY,ttt);
    rtable_sz.push_back(rsz);
    if (rtable_sz.size() > 10000)
      rtable_sz.erase(rtable_sz.begin());
  }
}

void
ChordAdapt::add_stat(double ti, bool live)
{
  if ((_fixed_stab_int) || (ti <= 0.0) || (ti >= 1.0))
    return;

  Stat s;
  s.alive = live;
  s.ti = ti;
  _stat.push_back(s);
  if (_stat.size()> EST_TIMEOUT_SZ) 
    adjust_timeout();
}

double
ChordAdapt::est_timeout(double prob)
{
  return _calculated_prob[(uint)(10*prob)];
}

void
ChordAdapt::adjust_timeout()
{
  if (_fixed_stab_int)
    return;

  uint ssz = _stat.size();
  vector<double> new_prob;
  sort_live.clear();
  sort_dead.clear();
  for (uint i = 0; i < _stat.size(); i++) {
    if (_stat[i].alive)
      sort_live.push_back(_stat[i].ti);
    else
      sort_dead.push_back(_stat[i].ti);
  }
  sort(sort_live.begin(),sort_live.end());
  sort(sort_dead.begin(),sort_dead.end());
  _stat.clear();
  if (sort_dead.size() == 0 || sort_live.size() ==0)
    return;

  new_prob.resize(10);
  for (uint i = 0; i < 10; i++)
    new_prob[i] = 0.0;

  uint lsz = sort_live.size();
  uint dsz = sort_dead.size();
  uint live = 0, dead = dsz;
  uint i = 0, j = 0;
  double tt;
  while (1) {
    if (i < lsz && j < dsz) {
      if (sort_live[i] < sort_dead[j]) {
	tt = sort_live[i];
	live++;
	i++;
      }else{
	tt = sort_dead[j];
	dead--;
	j++;
      }
    }else if (i < lsz) {
      tt = sort_live[i];
      live++;
      i++;
    }else if (j < dsz) {
      tt = sort_dead[j];
      dead--;
      j++;
    }else
      break;
    uint p = (uint)(10.0*dead/(double)(dead+live))+ 1;
    if (p>=10) 
      p = 9;
    if (new_prob[p] <= 0.0000001)
      new_prob[p] = tt;
  }
  _last_calculated = now();
  NDEBUG(4) << "estimated timeout " << _calculated_prob[1] << endl;
  if (_calculated_prob[1] > 0.99) {
    printf("sort_live %u:  ",lsz);
    for (uint i = 0; i < sort_live.size(); i++) 
      printf("%.2f ",sort_live[i]);
    printf("\n");
    printf("sort_dead %u:  ",dsz);
    for (uint i = 0; i < sort_dead.size(); i++) 
      printf("%.2f ",sort_dead[i]);
    printf("\n");
    printf("calculated prob : ");
    for (uint i = 0; i < _calculated_prob.size();i++) 
      printf("%.2f ",_calculated_prob[i]);
    printf("\n");
    new_prob[1] = 0.95;
  }
  for (uint i = 0; i < 10; i++) 
    _calculated_prob[i] = 0.9*_calculated_prob[i] + 0.1*new_prob[i];
}

#include "p2psim/bighashmap.cc"
