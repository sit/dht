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
  _id = _me.id;

  loctable = new LocTable();
  loctable->set_timeout(0);
  loctable->init(_me);

  _wkn.ip = 0;

  ids.push_back(_me);

  notifyinfo.clear();
}

ChordAdapt::~ChordAdapt()
{
  if (_me.ip == 1) 
    Node::print_stats();

  delete loctable;
}

/* -------------- initstate ---------------- */
void
ChordAdapt::initstate()
{
  if (_me.ip == 1) {
    sort(ids.begin(),ids.end(), IDMap::cmp);
  }
  uint sz = ids.size();
  //add successors
  uint my_pos = find(ids.begin(), ids.end(), _me) - ids.begin();
  for (uint i = 1; i <= _nsucc; i++) 
    loctable->add_node(ids[(my_pos+i) % sz],true);

  //add predecessor
  loctable->add_node(ids[(my_pos-1) % sz]);

  //add random nodes
  uint n = 0;
  while (n < 20) { 
    uint r = random() % sz;
    loctable->add_node(ids[r]);
    n++;
  }

  IDMap succ = loctable->succ(_me.id+1);
  NDEBUG(3) << "inited succ " << succ.ip << "," << printID(succ.id) << endl;
}

/* -------------- join --------------------- */
void
ChordAdapt::join(Args *args)
{
  /*
  if (!_wkn.ip) {
    _wkn.ip = args->nget<IPAddress>("wellknown");
    _wkn.id = dynamic_cast<ChordAdapt *>(Network::Instance()->getnode(_wkn.ip))->id();
  }
  */
  while ((!_wkn.ip) || (!Network::Instance()->alive(_wkn.ip))) {
    _wkn.ip = Network::Instance()->getnode(ids[random()%ids.size()].ip)->ip();
    _wkn.id = Network::Instance()->getnode(_wkn.ip)->id();
  }
  _me.ip = ip();
  _me.id = ConsistentHash::ip2chid(_me.ip);
  _id = _me.id;
  loctable->init(_me);

  vector<IDMap>::iterator p = upper_bound(ids.begin(),ids.end(),_me, IDMap::cmp);
  if (p->id!=_me.id)
    ids.insert(p,1,_me);

  NDEBUG(1) << "start to join locsz " << loctable->size() << " wkn " 
    << _wkn.ip << endl;

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

  _rate_queue->do_rpc(_wkn.ip, &ChordAdapt::find_successors_handler,
      &ChordAdapt::null_cb, la, lr, (uint)0, la->type,
      (uint)PKT_SZ(1,1), TIMEOUT(_me.ip, _wkn.ip));
}

int
ChordAdapt::null_cb(bool b, lookup_args *a, lookup_ret *r)
{
  if (a) delete a;
  if (r) delete r;
  return PKT_OVERHEAD;
}

void
ChordAdapt::join_handler(lookup_args *la, lookup_ret *lr)
{
  _join_scheduled = false;
  if (alive()) {
    for (uint i = 0; i < lr->v.size(); i++) {
      if (lr->v[i].ip != _me.ip)
	loctable->add_node(lr->v[i],true);
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
    }
    IDMap succ = loctable->succ(_me.id+1);
    NDEBUG(1) << "joined succ " << succ.ip << "," << printID(succ.id) 
      << "locsz " << loctable->size() << endl;
  }
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
  _rate_queue->stop_queue();
  loctable->del_all();
  _outstanding_lookups.clear();
  _forwarded.clear();
  vector<IDMap>::iterator p = find(ids.begin(),ids.end(),_me);
  ids.erase(p);
  notifyinfo.clear();
  NDEBUG(1) << "crashed" << endl;
}

/* ------------------------ lookup (recursive) ----------------------- */
void
ChordAdapt::lookup(Args *args)
{
  if ((_join_scheduled) && (loctable->size() < 2)) {
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
    lookup_ret *llr = new lookup_ret;
    llr->v = lr->v;
    _rate_queue->do_rpc(la->ori.ip, &ChordAdapt::join_handler,
	&ChordAdapt::null_cb, (lookup_args *)NULL, llr, 
	1, TYPE_JOIN_LOOKUP, PKT_SZ(llr->v.size(),0), TIMEOUT(_me.ip, la->ori.ip));
    return;
  };

  assert(lr->done);
  Time t = _outstanding_lookups.find(la->key);
  if (t) {
    bool b = check_correctness(la->key, lr->v);
    NDEBUG(2) << "done lookup key " << printID(la->key) << "from " 
      << la->from.ip << "," << printID(la->from.id) 
      << "succ " << lr->v.size() << " " << (lr->v.size()>0?lr->v[0].ip:0) << "," 
      << printID(lr->v.size()>0?lr->v[0].id:0) << " hops " << la->hops
      << " timeouts " << la->to_num << " correct? " << (b?1:0) << endl;
    record_lookup_stat(_me.ip, la->from.ip, now()-t, lr->v.size()?true:false, 
	b, la->hops, la->to_num, la->to_lat);
    _outstanding_lookups.remove(la->key);
  }
}

void
ChordAdapt::next_recurs(lookup_args *la, lookup_ret *lr)
{
  string s = printID(la->key);
  if (s == "18a0acd24ae9aee6 " && _me.ip == 852) 
    fprintf(stderr,"%llu %u fuck src %u ori %u hops %u!\n", now(),_me.ip, la->src.ip, la->ori.ip, la->hops);

  //if i have forwarded pkts for this key
  //and the packet is droppable
  if ((!la->no_drop) && _forwarded.find(la->src.id | la->key)) {
    //XXX give the learnsz?
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << 
      la->src.ip << " ori " << la->ori.ip << " forwarded before" 
      << endl;
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
      la->src.ip << " ori " << la->ori.ip << " done succ " 
      << llr->v.size() << " " << succ.ip << "," << printID(succ.id) 
      << " hops " << lla->hops << endl;
    _rate_queue->do_rpc(lla->src.ip, &ChordAdapt::donelookup_handler,
	&ChordAdapt::null_cb, lla, llr, 1, lla->type, 
	PKT_SZ(1,0), TIMEOUT(_me.ip,lla->src.ip));
    return;
  }

  vector<IDMap> nexthops = loctable->preds(la->key, la->parallelism);
  bool sent_success = true;
  uint i= 0;
  if (lr) lr->v.clear();
  while ((sent_success) && (i< nexthops.size())) {
    lookup_args *lla = new lookup_args;
    bcopy(la,lla,sizeof(lookup_args));
    if ((la->no_drop) && i == 0)
      la->no_drop = true;
    else
      la->no_drop = false;
    lla->hops = la->hops+1;
    lla->nexthop = nexthops[i];
    lla->from = _me;
    lookup_ret *llr = new lookup_ret;
    NDEBUG(3) << "next_recurs key " << printID(la->key) << "src " << la->src.ip 
      << " ori " << la->ori.ip << " forward to next "
      << nexthops[i].ip << "," << printID(nexthops[i].id) 
      << " hops " << lla->hops << endl;
    sent_success = _rate_queue->do_rpc(nexthops[i].ip, &ChordAdapt::next_recurs,
	&ChordAdapt::next_recurs_cb, lla,llr, lla->no_drop?1:3, lla->type,
	PKT_SZ(1,0), TIMEOUT(_me.ip,nexthops[i].ip));
    i++;
  }

  if (lr)
    lr->v = loctable->preds(la->key, la->learnsz);

}

int
ChordAdapt::next_recurs_cb(bool b, lookup_args *la, lookup_ret *lr)
{
  int ret_sz = 0;
  if (alive()) {
    if (b) {
      ret_sz = PKT_SZ(lr->v.size(),0);
      for (uint i = 0; i < lr->v.size(); i++) 
	loctable->add_node(lr->v[i]);
      NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src "
	<< la->src.ip << " ori " << la->ori.ip << " from "
	<< la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	la->ori.ip << " learn " << lr->v.size() << endl;
    }else {
      //increase the timeout value of the next hop by 1
      //XXX timeout instead of delete
      loctable->del_node(la->nexthop, true);
      NDEBUG(3) << "next_recurs_cb key " << printID(la->key) << "src " 
	<< la->src.ip << " ori " << la->ori.ip << " from "
	<< la->nexthop.ip << "," << printID(la->nexthop.id) << "ori " << 
	la->ori.ip << " dead" << endl;
      if (la->no_drop) {
	//send again, to myself
	la->to_lat += TIMEOUT(_me.ip, la->nexthop.ip);
	la->to_num++;
	la->parallelism = 1;
	next_recurs(la,lr);
	return 0;
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
      loctable->del_node(gpa->n);
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
      loctable->del_node(gpa->n); //XXX: don't delete after one try?
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
  vector<IDMap> oldlist2 = loctable->succs(_me.id+1,_nsucc);
  if (oldlist.size()!=oldlist2.size()) {
    NDEBUG(4) << "fuck!" << endl;
  }

  for (uint i = 0; i < oldlist.size(); i++) {
    if (oldlist[i].ip!=oldlist2[i].ip) {
      NDEBUG(4) << "fuck fuck i !" << i << " old " 
	<< oldlist[i].ip << " old2" << oldlist2[i].ip << endl;
    }
  }

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
  //find the biggest gap and get nodes
  get_predsucc_args *gpa = new get_predsucc_args;
  get_predsucc_ret *gpr = new get_predsucc_ret;
  IDMap pred = loctable->pred_biggest_gap();
  gpa->m = _learn_num;
  gpa->n = pred;
  gpa->src = _me;
  NDEBUG(2) << "empty_queue learn from " << gpa->n.ip << "," 
    << printID(gpa->n.id) << endl;
  _rate_queue->do_rpc(pred.ip, &ChordAdapt::get_predsucc_handler, 
      &ChordAdapt::learn_cb, gpa, gpr, 3, TYPE_FINGER_UP, 
      PKT_SZ(0,1), TIMEOUT(_me.ip,pred.ip));
}

int
ChordAdapt::learn_cb(bool b, get_predsucc_args *gpa, get_predsucc_ret *gpr)
{
  uint ret_sz = 0;
  if (alive()) {
    if (b) {
      loctable->add_node(gpr->pred);
      for (uint i = 0; i < gpr->v.size(); i++) 
	loctable->add_node(gpr->v[i]);
      ret_sz = PKT_SZ(gpr->v.size()+1,0);
    }else{
      loctable->del_node(gpa->n); //XXX: should not delete a finger after one failure
      ret_sz = PKT_OVERHEAD;
    }
    if (_rate_queue->empty()) 
      empty_queue();
  }
  delete gpa;
  delete gpr;
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
