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

#include "chordadapt.h"
#include <stdio.h>
#include <assert.h>

vector<IDMap> ChordAdapt::ids;
bool ChordAdapt::sorted;

ChordAdapt::ChordAdapt(IPAddress i, Args& a) : P2Protocol(i)
{
  _burst_sz = a.nget<uint>("burst_size", 10, 10);
  _bw_overhead = a.nget<uint>("overhead_rate", 100, 10);
  _stab_basic_timer = a.nget<uint>("basictimer", 18000, 10);
  _rate_queue = new RateControlQueue(this, (double)_bw_overhead, _burst_sz, ChordAdapt::empty_cb);
  _parallelism = a.nget<uint>("parallelism", 1,10);
  _nsucc = a.nget<uint>("successors",16,10);
  _to_multiplier = a.nget<uint>("timeout_multiplier", 3, 10);
  _learn_num = a.nget<uint>("learn_num",10,10);

  _stab_basic_running = false;
  _join_scheduled = false;
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
      NDEBUG(2) << "done lookup key " << printID(i.key()) << "timeout failed" << endl;
      record_lookup_stat(_me.ip, _me.ip, now()-i.value(), false, false, 0, 0, 0);
    }
  }
  delete loctable;
  if (ids.size() == 0) 
    Node::print_stats();

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
  for (uint i = 1; i <= _nsucc; i++) 
    loctable->add_node(ids[(my_pos+i) % sz],true);

  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);

  //add random nodes
  for (uint i = 0; i < 20; i++) {
    uint r = random() % sz;
    if (ids[r].id != _me.id)
      loctable->add_node(ids[r]);
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
  assert(0);
  _max_succ_gap = 0;
  while ((!_wkn.ip) || (!Network::Instance()->alive(_wkn.ip))) {
    _wkn.ip = Network::Instance()->getnode(ids[random()%ids.size()].ip)->ip();
    _wkn.id = Network::Instance()->getnode(_wkn.ip)->id();
  }
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

  NDEBUG(1) << "start to join locsz " << loctable->size()  << " succsz " 
    << loctable->succ_size() << " wkn " << _wkn.ip << " idsz " << ids.size() << endl;

  if (args && args->nget<uint>("first",0,10)==1) {
    //start basic successor stabilization
    if (!_stab_basic_running) {
      _stab_basic_running = true;
      stab_succ(NULL);
    } else {
      fix_succ();
    }
    return;
  }

  _join_scheduled = true;
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
      (uint)PKT_SZ(1,1), TIMEOUT(_me.ip, _wkn.ip));
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
  _join_scheduled = false;
  if (alive()) {
    la->src.timestamp = now();
    loctable->update_ifexists(la->src);
    _max_succ_gap = 0;
    for (uint i = 0; i < lr->v.size(); i++) {
      if (lr->v[i].ip != _me.ip) 
	loctable->add_node(lr->v[i],true);
      if (i!=0) {
	ConsistentHash::CHID gap = lr->v[i].id - lr->v[i-1].id;
	if (_max_succ_gap == 0 || gap > _max_succ_gap) 
	  _max_succ_gap = gap;
      }
    }
    if (loctable->size() < 2) {
      NDEBUG(1) << "join_handler joinsz " << lr->v.size() 
	<< " locsz " << loctable->size() << " reschedule join" 
	<< " key " << printID(_me.id-1) << "wkn " << _wkn.ip << endl;
      delaycb(200,&ChordAdapt::join, (Args *)0); //join again
    } else {
      //start basic successor stabilization
      if (!_stab_basic_running) {
	_stab_basic_running = true;
	stab_succ(NULL);
      } else {
	fix_succ();
      }
      join_learn();
    }
    IDMap succ = loctable->succ(_me.id+1);
    NDEBUG(1) << "joined succ " << succ.ip << "," << printID(succ.id) 
      << "locsz " << loctable->size() << endl;
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
      PKT_SZ(0,1), TIMEOUT(_me.ip,min_n.ip));

}

void
ChordAdapt::find_successors_handler(lookup_args *la, lookup_ret *lr)
{
  lookup_args lla;
  bcopy(la,&lla,sizeof(lookup_args));
  lla.src = _me;
  NDEBUG(3)<<"find_successors_handler key " << printID(lla.key) << " from " 
    << lla.ori.ip << "," << printID(lla.ori.id) << endl;
  next_recurs(&lla,NULL);
}

/* ------------------------ crash ------------------------------------*/
void
ChordAdapt::crash(Args *args)
{
  NDEBUG(1) << "crashed locsz " << loctable->size() << " ids " 
    << ids.size() << endl;
  _rate_queue->stop_queue();
  loctable->del_all();
  _outstanding_lookups.clear();
  _forwarded.clear();
  vector<IDMap>::iterator p = find(ids.begin(),ids.end(),_me);
  ids.erase(p);
  notifyinfo.clear();
}

/* ------------------------ lookup (recursive) ----------------------- */
void
ChordAdapt::lookup(Args *args)
{
  if ((_join_scheduled) && (loctable->size() < 2)) {
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
  la.ori.ip = 0;
  la.m = 1;
  la.parallelism = _parallelism;
  la.type = TYPE_USER_LOOKUP;
  la.learnsz = _learn_num;

  _outstanding_lookups.insert(la.key, now());
  NDEBUG(2) << "start lookup key " << printID(la.key) << endl;
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
	&ChordAdapt::null_cb, lla, llr, 1, TYPE_JOIN_LOOKUP, 
	PKT_SZ(llr->v.size(),0), TIMEOUT(_me.ip, la->ori.ip));
    return;
  };

  assert(lr->done);
  Time t = _outstanding_lookups.find(la->key);
  if (t) {
    bool b = check_correctness(la->key, lr->v);
    NDEBUG(2) << "done lookup key " << printID(la->key) << "from " 
      << la->from.ip << "," << printID(la->from.id) 
      << "succ " << lr->v.size() << " " << (lr->v.size()>0?lr->v[0].ip:0) << "," 
      << printID(lr->v.size()>0?lr->v[0].id:0) << "best " << 
      2*Network::Instance()->gettopology()->latency(_me.ip, la->from.ip) << " lat " 
      << now()-t << " hops " << la->hops << " timeouts " << la->to_num 
      << " correct? " << (b?1:0) << endl;
    record_lookup_stat(_me.ip, la->from.ip, now()-t, lr->v.size()?true:false, 
	b, la->hops, la->to_num, la->to_lat);
    _outstanding_lookups.remove(la->key);
  }
}

void
ChordAdapt::next_recurs(lookup_args *la, lookup_ret *lr)
{
  string s = printID(la->key);
  if (lr && la->overshoot) {
    //lr->v = loctable->get_closest_in_gap(la->learnsz, la->key, la->from);
    lr->v = loctable->get_closest_in_gap(la->learnsz, la->overshoot, la->from);
  }

  //if i have forwarded pkts for this key
  //and the packet is droppable
  if ((!la->no_drop) && _forwarded.find(la->src.id | la->key)) {
    //XXX give the learnsz?
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << 
      la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip 
      << " forwarded before" << endl;
    return;
  }

  _forwarded.insert(la->src.id | la->key, now());

  IDMap succ = loctable->succ(_me.id+1);
  if (!succ.ip || succ.ip == _me.ip) {
    if (!_join_scheduled) {
      delaycb(200,&ChordAdapt::join, (Args *)0); //join again
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
	&ChordAdapt::null_cb, lla, llr, 1, lla->type, 
	PKT_SZ(1,0), TIMEOUT(_me.ip,lla->src.ip));
    return;
  }

  vector<IDMap> nexthops = loctable->preds(la->key, la->parallelism);
  IDMap overshoot = loctable->succ(la->key);
  bool sent_success = true;
  uint i= 0;
  while ((sent_success) && (i< nexthops.size())) {
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    if ((la->no_drop) && i == 0)
      lla->no_drop = true;
    else
      lla->no_drop = false;
    lla->hops = la->hops+1;
    lla->nexthop = nexthops[i];
    lla->from = _me;
    lla->overshoot = overshoot.id;
    lookup_ret *llr = new lookup_ret;
    llr->v.clear();
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "locsz " << loctable->size() 
      << " src " << la->src.ip << " ori " << la->ori.ip << " from " << la->from.ip << 
      " forward to next " << nexthops[i].ip << "," << printID(nexthops[i].id) 
      << " hops " << lla->hops << endl;
    sent_success = _rate_queue->do_rpc(nexthops[i].ip, &ChordAdapt::next_recurs,
	&ChordAdapt::next_recurs_cb, lla,llr, lla->no_drop?1:3, lla->type,
	PKT_SZ(1,0), TIMEOUT(_me.ip,nexthops[i].ip));
    i++;
  }
  if (la->no_drop)
    _forwarded_nodrop.insert(la->src.id | la->key, i);

}

int
ChordAdapt::next_recurs_cb(bool b, lookup_args *la, lookup_ret *lr)
{
  int ret_sz = 0;
  if (alive()) {
    if (b) {
      loctable->update_ifexists(la->nexthop);
      if (_forwarded_nodrop.find(la->src.id | la->key))
	_forwarded_nodrop.remove(la->src.id|la->key);
      ret_sz = PKT_SZ(lr->v.size(),0);
      for (uint i = 0; i < lr->v.size(); i++) 
	loctable->add_node(lr->v[i]);
      NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src "
	<< la->src.ip << " ori " << la->ori.ip << " from "
	<< la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	la->ori.ip << " learn (" << lr->v.size() << ") " << 
	print_succs(lr->v) << endl;
    }else {
      //increase the timeout value of the next hop by 1
      //XXX timeout instead of delete
      loctable->del_node(la->nexthop, true);
      //uint x = loctable->add_check(la->nexthop);
      //assert(x > LOC_HEALTHY);
      NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from "
	<< la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	la->ori.ip << " dead" << endl;

      uint outstanding = _forwarded_nodrop.find(la->key | la->src.id);
      if (outstanding == 1) {
	//send again, to myself
	la->to_lat += TIMEOUT(_me.ip, la->nexthop.ip);
	la->to_num++;
	la->no_drop = true;
	_forwarded_nodrop.remove(la->key|la->src.id);
	NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src "
	<< la->src.ip << " ori " << la->ori.ip << " retransmit" << endl;
	next_recurs(la,lr);
	return 0;
      }else if (outstanding){
	outstanding--;
	_forwarded_nodrop.insert(la->key|la->src.id,outstanding);
      }
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
  fix_succ();
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
      &ChordAdapt::notify_pred_cb, nsa, nsr, 0, TYPE_FIXPRED_UP, PKT_SZ(0,1),
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
	b = loctable->del_node(nsa->info[i].n,true); 
      }else{
	b = loctable->add_node(nsa->info[i].n,true);
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
      loctable->del_node(nsa->n,true);
    }
  }
  delete nsa;
  delete nsr;
  return ret_sz;
}

void
ChordAdapt::fix_pred()
{
  IDMap pred = loctable->pred(_me.id-1);
  get_predsucc_args *gpa = new get_predsucc_args;
  get_predsucc_ret *gpr = new get_predsucc_ret;
  gpa->m = 1;
  gpa->n = pred;
  gpa->src = _me;
  gpr->v.clear();
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::get_predsucc_handler,
      &ChordAdapt::fix_pred_cb, gpa, gpr, 0, TYPE_FIXPRED_UP, PKT_SZ(0,1),
      TIMEOUT(_me.ip,pred.ip));
}

int
ChordAdapt::fix_pred_cb(bool b, get_predsucc_args *gpa, get_predsucc_ret *gpr)
{
  int ret_sz = 0;
  if (alive()) {
    if (b) {
      ret_sz = PKT_SZ(1,0);
      loctable->update_ifexists(gpa->n);
      if (gpr->v.size()>0) 
	loctable->add_node(gpr->v[0]);
    } else {
      loctable->del_node(gpa->n,true);
    } 
  }
  delete gpa;
  delete gpr;
  return ret_sz;
}

void 
ChordAdapt::fix_succ()
{
  IDMap succ = loctable->succ(_me.id+1);

  if (succ.ip == 0) {
    NDEBUG(1) << "fix_succ locsz " << loctable->size() 
      << " reschedule join" << endl;
    if (!_join_scheduled)
      delaycb(0, &ChordAdapt::join, (Args *)0);
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
	&ChordAdapt::fix_succ_cb, gpa,gpr, 0, TYPE_FIXSUCC_UP, PKT_SZ(0,1),
	TIMEOUT(_me.ip,succ.ip));
}

void
ChordAdapt::get_predsucc_handler(get_predsucc_args *gpa, 
    get_predsucc_ret *gpr)
{
  loctable->add_node(gpa->src);
  gpr->pred = loctable->pred(_me.id-1);
  if (gpa->m)
    gpr->v = loctable->succs(_me.id+1,gpa->m);
}

int
ChordAdapt::fix_succ_cb(bool b, get_predsucc_args *gpa, get_predsucc_ret *gpr)
{
  int ret_sz = 0;
  if (alive()) {
    vector<IDMap> scs = loctable->succs(_me.id + 1, _nsucc);
    NDEBUG(3) << "fix_succ_cb old succ " << gpa->n.ip << "," 
      << printID(gpa->n.id) << " alive? " << (b?1:0) 
      << " succsz " << scs.size() << "(" << print_succs(scs) 
      << ")" << endl;
    if (b) {
      ret_sz = PKT_SZ(1+gpr->v.size(),0);
      IDMap succ = loctable->succ(_me.id+1);
      loctable->update_ifexists(gpa->n);
      if (gpr->pred.ip == _me.ip) {
      }else if (gpr->pred.ip != _me.ip) {
	loctable->add_node(gpr->pred);
      }
      consolidate_succ_list(gpa->n,scs,gpr->v);
      vector<IDMap> newscs = loctable->succs(_me.id+1,_nsucc);
      NDEBUG(3) << "fix_succ_cb pred " << gpr->pred.ip << " new succ " << (newscs.size()>0?newscs[0].ip:0) << "," << 
	(newscs.size()>0?printID(newscs[0].id):"??") << " succsz " << newscs.size() << "(" <<
	print_succs(newscs) << ")" << endl;

      fix_pred();

    } else {
      loctable->del_node(gpa->n,true); //XXX: don't delete after one try?
      fix_succ();
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
      break;
    }
    if (oldlist[oldi].ip == newlist[newi].ip) {
      loctable->add_node(newlist[newi],true);
      newi++;
      oldi++;
    }else if (ConsistentHash::between(_me.id, newlist[newi].id, oldlist[oldi].id)) {
      loctable->del_node(oldlist[oldi],true);
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
  return c->empty_queue();
}

void
ChordAdapt::empty_queue() 
{

  if (_join_scheduled)
    return;

  //find the biggest gap and get nodes
  learn_args *la = new learn_args;
  learn_ret *lr = new learn_ret;
  IDMap pred, next;
  Time to = 0;
  Time tt = 0;

  do {
    assert(to < 21600000);
    tt = loctable->pred_biggest_gap(pred, next, to>0?0:_max_succ_gap, to);
    assert(tt < 21600000);
    assert(tt <= to);
    to = tt/2;
  }while (pred.ip == _me.ip);

  la->m = _learn_num;
  la->n = pred;
  la->src = _me;
  la->end = next;

  NDEBUG(2) << "empty_queue locsz " << loctable->size() << " " 
    << printID(_max_succ_gap) << "learn from " << la->n.ip << "," 
    << printID(la->n.id) << " max old entry " << tt << endl;

  if (pred.ip == _me.ip ) {
    delete la;
    delete lr;
    return;
  }
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::learn_handler, 
      &ChordAdapt::learn_cb, la, lr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), TIMEOUT(_me.ip,pred.ip));
}

void
ChordAdapt::learn_handler(learn_args *la, learn_ret *lr)
{
  vector<IDMap> scs = loctable->succs(_me.id+2, 3 * la->m);
  lr->v.clear();
  la->n = _me;
  if (la->m > 0) {
    lr->v = loctable->get_closest_in_gap(la->m, la->end.id, la->src);
  }else{
    lr->v = loctable->get_all();
  }
}

int
ChordAdapt::learn_cb(bool b, learn_args *la, learn_ret *lr)
{
  uint ret_sz = 0;
  if (alive()) {
    if (b) {
      uint b_sz = loctable->size();
      uint b_succsz = loctable->succ_size();
      loctable->update_ifexists(la->n);
      ConsistentHash::CHID gap = (la->end.id - la->n.id);
      if ((lr->v.size() == 0) && (la->end.id!=_me.id) && (gap > _max_succ_gap))
	_max_succ_gap = gap;
      for (uint i = 0; i < lr->v.size(); i++) 
	loctable->add_node(lr->v[i]);
      NDEBUG(4) << "locsz (" << b_sz <<"," << loctable->size() << ") succ_sz (" << b_succsz << "," 
	<< loctable->succ_size() << ") max_gap " << printID(_max_succ_gap) << "this_gap " 
	<< printID(la->end.id - la->n.id) << "learn_cb " << la->m 
	<< " from " << la->n.ip 
	<< " sz " << lr->v.size() << ": " << print_succs(lr->v) << endl;

      ret_sz = PKT_SZ(lr->v.size()+1,0);
    }else{
      NDEBUG(4) << " learn_cb node " << la->n.ip << " dead!" << endl;
      loctable->del_node(la->n,true); //XXX: should not delete a finger after one failure
     // uint x = loctable->add_check(la->n);
      //assert(x > LOC_HEALTHY);
      ret_sz = PKT_OVERHEAD;
    }
    if (_rate_queue->empty()) 
      empty_queue();
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
ChordAdapt::check_correctness(ConsistentHash::CHID k, vector<IDMap> v)
{
  if (v.size() == 0) return false;

  IDMap tmp;
  tmp.id = k;
  uint pos = upper_bound(ids.begin(),ids.end(),tmp, IDMap::cmp) - ids.begin();
  for (uint i = 0; i < v.size(); i++) {
    if (ids[pos].ip == v[i].ip) {
      pos = (pos+1) % ids.size();
    }else {
      NDEBUG(4) << "key " << printID(k) << "wrong " << v[i].ip << "," 
	<< printID(v[i].id) << "right " << ids[pos].ip << "," << 
	printID(ids[pos].id) << endl;
      return false;
    }
  }
  return true;
}

#include "p2psim/bighashmap.cc"
