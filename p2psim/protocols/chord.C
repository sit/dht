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

//global statistics collecting varaibles
vector<double> Chord::stat;
vector<double> Chord::_lookup_lat_v;
double Chord::_lookup_num = 0;
double Chord::_lookup_int_num = 0;
double Chord::_lookup_hops = 0;
double Chord::_lookup_timeouts = 0;
double Chord::_lookup_to_waste = 0;
double Chord::_lookup_interval = 0;
double Chord::_lookup_success = 0;
double Chord::_lookup_retries = 0;

#define DNODE 33

Chord::Chord(IPAddress i, Args& a, LocTable *l) : P2Protocol(i), _isstable (false)
{

  if(a.find("static_sim") != a.end())
    static_sim = true;
  else
    static_sim = false;

  //whether Chord uses vivaldi
  _vivaldi_dim = a.nget<uint>("vivaldidim", 0, 10);

  //stabilization timer
  _stab_basic_timer = a.nget<uint>("basictimer", 10000, 10);
  _stab_succlist_timer = a.nget<uint>("succlisttimer",_stab_basic_timer,10);

  //location table timeout values
  //_timeout = a.nget<uint>("timeout", 5*_stab_succlist_timer, 10);

  //successors
  _nsucc = a.nget<uint>("successors", 1, 10);

  //fragments
  _frag = a.nget<uint>("m",1,10);

  //how many successors are fragments on?
  _allfrag = a.nget<uint>("allfrag",1,10);
  assert(_allfrag <= _nsucc);

  //recursive routing?
  _recurs = a.nget<uint>("recurs",0,10);
  _recurs_direct = a.nget<uint>("recurs_direct",0,10);
  assert(!_recurs_direct || static_sim);

  //parallel lookup? parallelism only works in iterative lookup now
  _parallel = a.nget<uint>("parallelism",1,10);
  _alpha = a.nget<uint>("alpha",1,10);

  _asap = a.nget<uint>("asap",_frag,10);

  _max_lookup_time = a.nget<uint>("maxlookuptime",4000,10);

  _vivaldi = NULL;
  _wkn.ip = 0;

  assert(_frag <= _nsucc);

  me.ip = ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  me.choices = 0;

  if (l) 
    loctable = l;
  else
    loctable = New LocTable();

  if (static_sim)
    loctable->set_timeout(0);
  else
    loctable->set_timeout(5*_stab_basic_timer);
  //loctable->set_timeout(0);
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

  if (me.ip == 1) {
    stat.clear();
    for (uint i = 0; i <= TYPE_PNS_UP; i++) 
      stat.push_back(0.0);
    _lookup_lat_v.clear();
  }

}

void
Chord::record_stat(uint bytes, uint type)
{
  assert(type <= TYPE_PNS_UP);
  stat[type] += ((double) PKT_OVERHEAD+bytes);
}

Chord::~Chord()
{
#ifdef CHORD_DEBUG
  printf("Chord done (%u,%qx)", me.ip, me.id);
  for (uint i = 0; i < stat.size(); i++) 
    printf(" %u", stat[i]);
  printf("\n");
#endif
  if (me.ip == 1) {
    double allpkts = 0.0;
    for (uint i = 0; i <= TYPE_PNS_UP; i++) 
      allpkts += stat[i];

    printf("%.0f ", allpkts);
    for (uint i = 0; i <= TYPE_PNS_UP; i++)
      printf("%.0f ", stat[i]);

    sort(_lookup_lat_v.begin(),_lookup_lat_v.end());
    uint sz = _lookup_lat_v.size();

    printf("%.3f ", _lookup_interval/_lookup_num); //average lookup latency
    printf("%.3f ", _lookup_lat_v[(int)sz*0.1]);
    printf("%.3f ", _lookup_lat_v[(int)sz*0.5]);
    printf("%.3f ", _lookup_lat_v[(int)sz*0.9]);
    printf("%.3f ", _lookup_success/_lookup_num); //average success rate
    printf("%.3f ", _lookup_retries/_lookup_num); //average lookup retry
    printf("%.3f ", _lookup_hops/_lookup_int_num); //average lookup hopcount
    printf("%.3f ", _lookup_timeouts/_lookup_int_num); //average number of timeouts happened per lookup
    printf("%.3f ", _lookup_to_waste/_lookup_int_num); //average time wasted in lookup timeouts
    printf("%.0f\n", _lookup_num); //total number of user lookups
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

void
Chord::check_static_init()
{
  if (!static_sim) return;
  if (!_inited) {
    _inited = true;
    this->initstate();
  }
}

bool 
Chord::check_correctness(CHID k, vector<IDMap> v)
{
  //assert(v.size() > 0);
  if (v.size() > 1) return true; //LJY to be fixed

  uint vsz = v.size();
  assert(_frag == 1 && _allfrag == 1 && vsz == 1);

  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes();
  IDMap tmp;
  tmp.id = k;
  uint idsz = ids.size();
  uint pos = upper_bound(ids.begin(), ids.end(), tmp, Chord::IDMap::cmp) - ids.begin();

  while (1) {
    if (pos >= idsz) pos = 0;
    Node *node = Network::Instance()->getnode(ids[pos].ip);
    if ((node->alive() && ((Chord *)getpeer(ids[pos].ip))->inited()))
      break;
    pos++;
  }

  if (ConsistentHash::betweenrightincl(k,ids[pos].id,v[0].id)) {
#ifdef CHORD_DEBUG
	printf("%s lookup correct key %16qx succ should be (%u,%qx) \
	    instead of (%u, %qx) inited? %u\n", ts(), k, ids[pos].ip, 
	    ids[pos].id, v[0].ip, v[0].id, ((Chord *)getpeer(ids[pos].ip))->inited()?1:0);
#endif
	return true; //LJY i do not count this as failure
  } else if (ids[pos].ip != v[0].ip) {
#ifdef CHORD_DEBUG
      printf("%s lookup incorrect key %16qx succ should be (%u,%qx) \
	instead of (%u, %qx)\n", ts(), k, ids[pos].ip, 
	ids[pos].id, v[0].ip, v[0].id);
#endif
      return false;
  }else
    return true;
}

void
Chord::lookup(Args *args) 
{
  check_static_init();
  CHID k = args->nget<CHID>("key");
  assert(k);
  lookup_args *a = New lookup_args;
  a->key = k;
  a->start = now();
  a->retrytimes.clear();
  lookup_internal(a); //lookup internal deletes a
}

void
Chord::lookup_internal(lookup_args *a)
{
  vector<IDMap> v;
  uint lookup_int = 0; 
  Time before = now();

  //now() - before includes some extra garbage collection time that needs to be subtracted.
  int extra_time = 0; 


  if (_recurs) {
    v = find_successors_recurs(a->key, _frag, _allfrag, TYPE_USER_LOOKUP, _recurs_direct>0?(&lookup_int):NULL);
  } else {
    v = find_successors(a->key, _frag, _allfrag, TYPE_USER_LOOKUP, &lookup_int);
  }
  if (lookup_int > 0) {
    extra_time = now() - before - lookup_int;
    assert(extra_time >=0 || (_recurs_direct));
  }

  if (!alive()) {
    delete a;
    return;
  }else if (v.size() > 0) {
    _lookup_success += 1;
#ifdef CHORD_DEBUG
    printf("%s key %qx lookup correct interval ", ts(), a->key);
#endif
  }else if (now()-a->start >= _max_lookup_time) {
#ifdef CHORD_DEBUG
    printf("%s key %qx lookup incorrect interval ", ts(), a->key); 
#endif
  }else{
    assert(!static_sim);
    a->retrytimes.push_back((uint)(now() - a->start - extra_time));
    delaycb((100-extra_time)>0?100-extra_time:0, &Chord::lookup_internal, a);
    return;
  }

  a->retrytimes.push_back((uint)(now()-a->start-extra_time));

  _lookup_num += 1;
  _lookup_interval += a->retrytimes[a->retrytimes.size()-1];
  _lookup_retries +=  a->retrytimes.size();
  if (_lookup_lat_v.size() < 10000) 
    _lookup_lat_v.push_back(a->retrytimes[a->retrytimes.size()-1]);
  else {
    int k = random() % 10001;
    //displace a random sample
    if (k < 10000) 
      _lookup_lat_v[k] = a->retrytimes[a->retrytimes.size()-1];
  }

#ifdef CHORD_DEBUG
  for (uint i = 0; i < a->retrytimes.size(); i++) 
    printf("%u ", a->retrytimes[i]);
  printf("\n");
#endif
  delete a;
}

void
Chord::find_successors_handler(find_successors_args *args, 
			       find_successors_ret *ret)
{
  check_static_init();
  if (_recurs)
    ret->v = find_successors_recurs(args->key, args->m, args->all, TYPE_JOIN_LOOKUP);
  else
    ret->v = find_successors(args->key, args->m, args->all, TYPE_JOIN_LOOKUP, NULL, &(ret->last));

#ifdef CHORD_DEBUG
  if (ret->v.size() > 0) 
    printf("%s find_successors_handler key %qx succ %u,%qx\n",ts(),args->key,ret->v[0].ip,ret->v[0].id);
#endif
}

vector<Chord::IDMap>
Chord::find_successors(CHID key, uint m, uint all, uint type, uint *lookup_int, IDMap *last)
{
  //parallelism controls how many queries are inflight
  next_args na;
  hash_map<IPAddress, bool> asked;
  hop_info lastfinished;
  list<hop_info> tasks;
  list<hop_info> savefinished;
  bool ok;
  unsigned rpc, donerpc;
  unsigned totalrpc = 0;
  unsigned totalto = 0;
  nextretinfo *reuse = NULL;
  nextretinfo *p;
  RPCSet rpcset;
  RPCSet alertset;
  hash_map<unsigned, nextretinfo*> resultmap;
  hash_map<unsigned, alert_args*> alertmap;
  hop_info h;
  vector<IDMap> results;
  uint outstanding, alertoutstanding;
  Time before = now();

  h.from = me;
  h.to = me;
  h.hop = 1;

  na.key = key;
  na.m = m;
  na.all = all;
  na.deadnodes.clear();

  savefinished.clear();
  lastfinished.from.ip = 0;
  lastfinished.to.ip = 0;
  lastfinished.hop = 0;

  //put myself in the task queue
  tasks.push_back(h);
  asked[me.ip] = true;

  results.clear();
  outstanding = alertoutstanding = 0;
#ifdef CHORD_DEBUG
  vector<hop_info> recorded;
  recorded.clear();
#endif
  uint to = 0;
  Time lastwasted;
  uint wasted = 0;

  while (1) {

#ifdef CHORD_DEBUG
    if (totalrpc >= 100) {
      fprintf(stderr,"%s route: key %qx\n",ts(),key);
      for (uint i = 0; i < recorded.size(); i++) 
	fprintf(stderr,"(%u,%qx,%u) ", recorded[i].to.ip, recorded[i].to.id, recorded[i].hop);
      fprintf(stderr,"\n");
      assert(0);
    }
#endif
    assert(totalrpc < 100);

    lastwasted = 0;
    if ((!outstanding) && (tasks.size() == 0)) {
      tasks.push_back(lastfinished);
      to++;
      lastwasted = now();
    }
    while ((outstanding < _parallel) && (tasks.size() > 0)){
      h = tasks.front();
      tasks.pop_front();
      if (reuse) {
	p = reuse;
	reuse = NULL;
      }else
	p = New nextretinfo;
      p->link = h;
      totalrpc++;
      record_stat(4+1, type);
      assert(h.to.ip > 0 && h.to.ip < 3000);
#ifdef CHORD_DEBUG
      recorded.push_back(h);
#endif
      record_stat(4+na.deadnodes.size()*4, type);
      assert((h.to.ip == me.ip) || (h.to.ip!=h.from.ip));
      rpc = asyncRPC(h.to.ip, &Chord::next_handler, &na, &(p->ret));
      rpcset.insert(rpc);
      resultmap[rpc] = p;
      outstanding++;
    }
    
    if (reuse) {
      delete reuse;
      reuse = NULL;
    }
    assert(outstanding > 0);

    //fill out all my allowed parallel connections, wait for response
    donerpc = rcvRPC(&rpcset, ok);
#ifdef CHORD_DEBUG
    if (me.ip == DNODE) {
      fprintf(stderr,"%s find_successor key %qx outstanding RPC %d\n", ts(), key, outstanding);
    }
#endif

    outstanding--;
    reuse = resultmap[donerpc];
    if (lastwasted > 0) {
      if (me.ip == DNODE) {
	fprintf(stderr,"add now %llu last %llu\n",now(), lastwasted);
      }
      wasted += (now() - lastwasted);
      assert(wasted > 0 || reuse->link.to.ip == me.ip);
      lastwasted = 0;
    }
    if (!alive()) goto DONE;
    if (ok) {
      record_stat(resultmap[donerpc]->ret.done?resultmap[donerpc]->ret.next.size()*4:resultmap[donerpc]->ret.v.size()*4,type);

      if (resultmap[donerpc]->link.from.ip == me.ip) 
	loctable->add_node(resultmap[donerpc]->link.to); //update timestamp of MY neighbors

      if (resultmap[donerpc]->ret.done) {
	lastfinished = resultmap[donerpc]->link;
	goto DONE;//success
      }

      //LJY: to be fixed, does not look like it will work for parallel lookup
      if (resultmap[donerpc]->ret.next.size() == 0 ) {
//	  && resultmap[donerpc]->link.to.ip == lastfinished.to.ip)  //this will fail coz i cannot mark this node as dead
	lastfinished = resultmap[donerpc]->link;
	goto DONE;//failed
      }
      
      list<hop_info>::iterator iter = tasks.begin();
      IDMap tmptmp;
      for (uint i = 0; i < resultmap[donerpc]->ret.next.size(); i++) {
	tmptmp = resultmap[donerpc]->ret.next[i];
	if (asked.find(resultmap[donerpc]->ret.next[i].ip) != asked.end()) {
	  continue;
	}
	h.from = resultmap[donerpc]->link.to;
	h.to = resultmap[donerpc]->ret.next[i];
	h.hop = resultmap[donerpc]->link.hop + 1;

	while (iter != tasks.end()) {
	  if (ConsistentHash::between(iter->to.id, key, resultmap[donerpc]->ret.next[i].id))
	    break;
	  iter++;
	}
	tasks.insert(iter, h);
	asked[h.to.ip] = true;
      }
      //insert into the history of finished rpcs
      iter = savefinished.begin();
      while (iter != savefinished.end()) {
	if (ConsistentHash::betweenrightincl(me.id, iter->to.id, resultmap[donerpc]->link.to.id)) 
	  break;
	iter++;
      }
      if (iter == savefinished.end() || resultmap[donerpc]->link.to.ip != iter->to.ip) 
	savefinished.insert(iter, resultmap[donerpc]->link);
      lastfinished = savefinished.back();
    } else {
      totalto++;
      //notify
      alert_args *aa = New alert_args;
      aa->n = resultmap[donerpc]->link.to;
      record_stat(4,type);
      assert(resultmap[donerpc]->link.from.ip > 0 && resultmap[donerpc]->link.from.ip < 3000);
      rpc = asyncRPC(resultmap[donerpc]->link.from.ip, &Chord::alert_handler, aa,(void *)NULL);
      alertset.insert(rpc);
      alertmap[rpc] = aa;
      alertoutstanding++;
      if (ConsistentHash::betweenleftincl(lastfinished.to.id, key, aa->n.id)) {
	na.deadnodes.push_back(aa->n);
      }
      if (lastfinished.to.ip == aa->n.ip) {
	assert(savefinished.size()>0);
	savefinished.pop_back();
	lastfinished = savefinished.back();
      }
    }
  }
DONE:

  //jesus christ i'm done, however, i need to clean up my shit
  assert(reuse);
  if ((type == TYPE_USER_LOOKUP) && (alive())) {

    if (lookup_int) 
      *lookup_int = now()-before;

    _lookup_hops += lastfinished.hop;
    _lookup_timeouts += totalto;
    _lookup_int_num += 1;
    _lookup_to_waste += wasted;

#ifdef CHORD_DEBUG
    printf("%s lookup key %qx, hops %d timeout %d wasted %d totalrpc %d\n", ts(), key, lastfinished.hop, totalto, wasted, totalrpc);
    Topology *t = Network::Instance()->gettopology();
    printf("%s key %qx route: ", ts(), key);
    IDMap last;
    last = lastfinished.to;
    assert(last.ip <= 1024 && last.ip > 0);
    for (uint i = recorded.size()-1; i >= 0; i--) {
      if (last.ip == me.ip) break;
      if (recorded[i].to.ip == last.ip) {
	printf("(%u,%qx %llu) ", recorded[i].to.ip, recorded[i].to.id, (uint)2*t->latency(me.ip, last.ip));
	last = recorded[i].from;
	assert(last.ip <= 1024 && last.ip > 0);
      }
    }
    printf("\n");
#endif
  }

  if ((reuse->ret.done) && (reuse->ret.correct))
    results = reuse->ret.v;

  delete reuse;

  for (uint i = 0; i < outstanding; i++) {
    donerpc = rcvRPC(&rpcset, ok);
    if (ok)
      record_stat(resultmap[donerpc]->ret.done?resultmap[donerpc]->ret.next.size()*4:resultmap[donerpc]->ret.v.size()*4,type);
    delete resultmap[donerpc];
  }
  for (uint i = 0;i < alertoutstanding; i++) {
    donerpc = rcvRPC(&alertset, ok);
    if (ok)
      record_stat(0,type);
    delete alertmap[donerpc];
  }
  return results;
}

/*
// Returns at least m successors of key.
// This is the lookup() code in Figure 3 of the SOSP03 submission.
// A local call, use find_successors_handler for an RPC.
// Not recursive.
vector<Chord::IDMap>
Chord::find_successors(CHID key, uint m, uint all, uint type, IDMap *last)
{
  assert(0);
  bool ok;
  assert(m <= _nsucc);

  int count = 0;

  vector<IDMap> route;

  if (vis && type == TYPE_USER_LOOKUP)
    printf ("vis %llu search %16qx %16qx\n", now(), me.id, key);

  next_args na;
  next_ret nr;
  nr.v.clear();

  na.key = key;
  na.m = m;
  na.all = all;

  IDMap nprime = me;

  route.clear();
  route.push_back(me);


  uint timeout = 0;

  while(1){
    assert(count++ < 500);
    if (vis && type == TYPE_USER_LOOKUP)
      printf ("vis %llu step %16qx %16qx\n", now(), me.id, nprime.id);

    if (last) {
      *last = nprime;
    }
#ifdef CHORD_DEBUG
    printf("%s next hop %u,%qx for key %qx\n", ts(), nprime.ip,nprime.id, key);
#endif

    //lookup argument -- 1 ID, 1 extra
    record_stat(4+1,type);
    if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(nprime.ip));
      ok = _vivaldi->doRPC(nprime.ip, target, &Chord::next_handler, &na, &nr);
    } else
      ok = doRPC(nprime.ip, &Chord::next_handler, &na, &nr);
    if (ok) record_stat(nr.done?(nr.v.size()*4):4,type);


    if (!alive()) break;

    if(ok && nr.done){
      break;
    } 
    
    if ((ok) && (nr.next.ip)){

      route.push_back(nr.next);
      if (route.size() >= 20 ) {
	printf("%s key %qx route : ",ts(), key);
	for (uint i = 0; i < route.size(); i++) {
	  printf("(%u,%qx) ", route[i].ip,route[i].id);
	}
	printf("\n");
      }
      assert(route.size() < 20);
      nprime = nr.next;

      if (vis && type == TYPE_USER_LOOKUP)
	printf ("vis %llu step %16qx %16qx\n", now(), me.id, nr.next.id);

    } else {
      timeout++;
      uint rsz = route.size();
      assert(rsz < 20);
      if (rsz > 1) {
	route.pop_back (); 
	//if (!r) {
	  alert_args aa;
	  aa.n = nprime;
	  nprime = route.back ();
	  record_stat(4,type);
	  doRPC(nprime.ip, &Chord::alert_handler, &aa, (void *)NULL);
	//}
      } else {
	nr.v.clear ();
	break; 
      }
    }
  }

  if (type == TYPE_USER_LOOKUP) {
    printf ("%s find_successor for key %qx ", ts(), key);
    if (nr.v.size () > 0) {
      printf ("is (%u, %qx) hops %d timeout %d\n", nr.v[0].ip, 
	  nr.v[0].id, route.size(), timeout);
    }
  }

#ifdef CHORD_DEBUG
  Topology *t = Network::Instance()->gettopology();
  if (nr.v.size() > 0) {
    printf("%s find_successors %qx route: ", ts(), key);
    for (uint i = 0; i < route.size(); i++) {
      printf("(%u, %qx, %d, %d) ", route[i].ip, route[i].id, 2 * (unsigned) t->latency(me.ip,route[i].ip), route[i].choices);
    }
    printf("\n");
  }
#endif
  return nr.v;
}
*/

void
Chord::my_next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  doRPC(me.ip, &Chord::next_recurs_handler, args, ret);
}

vector<Chord::IDMap>
Chord::find_successors_recurs(CHID key, uint m, uint all, uint type, uint *recurs_int)
{
  next_recurs_args fa;
  next_recurs_ret fr;
  fa.path.clear();

  lookup_path tmp;
  tmp.n = me;
  tmp.tout = 0;
  fa.path.push_back(tmp);

  fa.key = key;
  fa.m = m;
  fa.all = all;
  fa.type = type;
  fr.v.clear();
  fr.correct = false;

  if (vis && type == TYPE_USER_LOOKUP)
    printf ("vis %llu search %16qx %16qx\n", now(), me.id, key);

  //no overhead for sending to myself
  my_next_recurs_handler(&fa,&fr);

  if (!alive()) return fr.v;

  Topology *t = Network::Instance()->gettopology();
#ifdef CHORD_DEBUG
  printf("%s find_successors_recurs %qx result %d,%d route: ",ts(), key, m,fr.v.size());
#endif
  uint psz = fa.path.size();
  assert(psz > 0 && (!fa.path[0].tout) && fa.path[0].n.ip == me.ip);

  uint total_to = 0;
  uint total_lat = 0;
  uint wasted = 0;
  IDMap nn, np;
  np = fa.path[0].n;
  for (uint i = 1; i < psz; i++) {
    nn = fa.path[i].n;
    if (fa.path[i].tout) {
      wasted += (2*t->latency(np.ip, nn.ip));
      total_to++;
    }
#ifdef CHORD_DEBUG
    printf("(%u,%qx,%u,%u,%u) ", 
	nn.ip, nn.id, (unsigned) t->latency(np.ip, nn.ip), nn.choices, fa.path[i].tout);
#endif
    if (i > 0) 
      total_lat += t->latency(np.ip, nn.ip);

    if (!fa.path[i].tout) 
      np = nn;
  }
#ifdef CHORD_DEBUG
  printf("\n");
#endif

  //XXX recursive lookup has a different way of calculating lookup latency
  if (recurs_int) {
    assert(static_sim);
    *recurs_int = total_lat + t->latency(fa.path[psz-1].n.ip, me.ip);
  }

  if (type == TYPE_USER_LOOKUP) {
#ifdef CHORD_DEBUG
    printf("%s lookup key %qx,%d, hops %d timeout %d wasted %d\n", ts(), key, m, psz-total_to, total_to, wasted);
#endif
    if (!fr.correct) 
      fr.v.clear();
    _lookup_int_num += 1;
    _lookup_hops += (psz - total_to);
    _lookup_timeouts += total_to;
    _lookup_int_num += 1;
    _lookup_to_waste += wasted;
  }
  assert(psz < 20);
  return fr.v;
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
  Chord* target;

  check_static_init();
  assert(alive());

  while (1) {
    succs = loctable->succs(me.id+1,_nsucc);

    if (succs.size() == 0) {
      //lookup failed
      //XXX do i need to backtrack?
#ifdef CHORD_DEBUG
      printf("%s succ size is 0, failed request %qx\n", ts(), args->key);
#endif
      ret->v.clear();

      //rejoin baby
      if (!_join_scheduled) {
	_join_scheduled++;
	delaycb(0, &Chord::join, (Args *)0);
      }
      return;
    }
    succ = succs[0];

    if (ConsistentHash::betweenrightincl(me.id, succs[0].id, args->key))  {
      ret->v.clear();
      sz = args->m < succs.size()? args->m : succs.size();
      for (i = 0; i < sz; i++) {
	ret->v.push_back(succs[i]);
	if (ret->v.size() > args->m) return;
      }
      ret->correct = check_correctness(args->key,ret->v);
      assert(!static_sim || ret->correct);
#ifdef CHORD_DEBUG
      if (!ret->correct)
	printf("%s incorrect key %qx, succ %u,%qx\n",ts(), args->key, succs[0].ip, succs[0].id);
#endif
      return;
    }else {
      if (args->path.size() >= 20) {
	fprintf(stderr,"path too long %s: ", ts());
	for (uint i = 0; i < args->path.size(); i++) {
	  fprintf(stderr,"(%u,%qx %u) ", args->path[i].n.ip, args->path[i].n.id, args->path[i].tout);
	}
	assert(0);
      }

      //XXX i never check if succ is dead or not
      next = loctable->next_hop(args->key,1,1);
      assert(ConsistentHash::between(me.id, args->key, next.id));

      tmp.n = next;
      tmp.tout = 0;
      args->path.push_back(tmp);

      record_stat(4,args->type);
      if (_vivaldi) {
	target = dynamic_cast<Chord *>(getpeer(next.ip));
	r = _vivaldi->doRPC(next.ip, target, &Chord::next_recurs_handler, args, ret);
      } else
	r = doRPC(next.ip, &Chord::next_recurs_handler, args, ret);

      if (!alive()) {
#ifdef CHORD_DEBUG
	printf("%s lost lookup request %qx\n", ts(), args->key);
#endif
	ret->v.clear();
	return;
      }

      if (r) {
	record_stat(ret->v.size()*4,args->type);
	loctable->add_node(next); //update timestamp
	return;
      }else{
#ifdef CHORD_DEBUG
	printf ("%s next hop to %u,%16qx failed\n", ts(), next.ip, next.id);
#endif
	if (vis) 
	  printf ("vis %llu delete %16qx %16qx succ %u,%qx\n", now (), me.id, next.id, succ.ip,succ.id);
	args->path[args->path.size()-1].tout = 1;
	loctable->del_node(next);
      }
    }
  }
}

void
Chord::null_handler (void *args, void *ret) 
{
  return;
}

// Always attempt to go to the predecessor for the key regardless of m
void
Chord::next_handler(next_args *args, next_ret *ret)
{
  check_static_init();
/* don't trust other people's information about my dead neighbors coz of non-transitivity
  for (uint i = 0; i < args->deadnodes.size(); i++) 
    loctable->del_node(args->deadnodes[i]);
    */

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);
  assert((!static_sim) || succs.size() >= _allfrag);

  ret->v.clear();
  ret->next.clear();

  if (succs.size() < 1) {
    ret->lastnode = me;
    ret->done = true;
    ret->correct = false;
  } else if (ConsistentHash::betweenrightincl(me.id, succs[0].id, args->key)) { 
    //XXX: need to take care of < m nodes situation in future
    uint max = args->m < succs.size()? args->m: succs.size();
    for (uint i = 0; i < max; i++) 
      ret->v.push_back(succs[i]);
    ret->lastnode = me;
    ret->done = true;
    ret->correct = check_correctness(args->key,ret->v);
    if (!ret->correct) 
      printf("%s incorrect key %qx, succ %u,%qx\n",ts(), args->key, succs[0].ip, succs[0].id);
  } else {
    ret->done = false;
    ret->next.clear();
    IDMap n;
    CHID k = args->key;
    uint i,j;
    i = j = 0;
    //LJY: very inefficient stuff
    while (i < _alpha) {
      n = loctable->next_hop(k);
      if (n.ip == me.ip) break;
      for (j = 0; j < args->deadnodes.size(); j++) 
	if (args->deadnodes[j].ip == n.ip) 
	  break;
      if (j == args->deadnodes.size()) {
	ret->next.push_back(n);
	i++;
      }
      k = n.id - 1;
    }
    /*
    uint ss = args->deadnodes.size();
    IDMap nn = ss>0?args->deadnodes[0]:me;
    assert(ret->next.size()>0);
    assert(ret->next[0].ip != me.ip);
    */
 }
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  if (static_sim) return;

#ifdef CHORD_DEBUG
  Time before = now();
#endif

  if (args) {
    ChordObserver::Instance(NULL)->addnode();
    _join_scheduled++;
    // XXX: Thomer says: not necessary
    // node()->set_alive(); //if args is NULL, it's an internal join
#ifdef CHORD_DEBUG
    assert(before<5000 || (!_inited));
    printf("%s start to join inited? %u basic stab running %u\n",ts(), _inited?1:0, _stab_basic_running?1:0);
    if (me.ip == DNODE) {
      fprintf(stderr,"%s start to join\n", ts());
    }
#endif
  }else{

    if (!alive()) {
      _join_scheduled--;
      return;
    }
#ifdef CHORD_DEBUG
    printf("%s rescheduled join inited? %u\n", ts(), _inited?1:0);
#endif
  }

  if (vis) {
    printf("vis %llu join %16qx\n", now (), me.id);
  }

  if (_vivaldi_dim > 0) {
    _vivaldi = New Vivaldi10(this, _vivaldi_dim, 0.05, 1); 
  }

  if (!_wkn.ip) {
    assert(args);
    _wkn.ip = args->nget<IPAddress>("wellknown");
    assert (_wkn.ip);
    _wkn.id = ConsistentHash::ip2chid(_wkn.ip);
  }

  find_successors_args fa;
  find_successors_ret fr;
  //fa.key = me.id + 1;
  fa.key = me.id - 1;
  //try to get multiple successors in case some failed
  assert(_nsucc > 3);
  fa.m = _nsucc;
  fa.all = _nsucc;

  record_stat(4, TYPE_JOIN_LOOKUP);
  bool ok = doRPC(_wkn.ip, &Chord::find_successors_handler, &fa, &fr);
  if (ok) record_stat(4*fr.v.size(), TYPE_JOIN_LOOKUP);

  if (!alive()) {
    _join_scheduled--;
    return;
  }

  if (fr.v.size() < 1) {
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

#ifdef CHORD_DEBUG
  Time after = now();
  if (me.ip == DNODE) 
    fprintf(stderr,"%s joined succ %u,%16qx elapsed %llu stab running %d _inited %u\n",
         ts(), fr.v[0].ip, fr.v[0].id,
         after - before, _stab_basic_running?1:0, _inited?1:0);
  printf("%s joined elapsed %llu stab running %d _inited %u succ: ",
         ts(), after - before, _stab_basic_running?1:0, _inited?1:0);
  for (uint i = 0; i < fr.v.size(); i++) {
    printf("%u,%qx ",fr.v[i].ip, fr.v[i].id);
  }
  printf("\n");
#endif
 

  if (!_stab_basic_running) {
    _stab_basic_running = true;
    reschedule_basic_stabilizer(NULL);
  }else{
    Chord::stabilize();
#ifdef CHORD_DEBUG
    printf("%s stabilization already running _inited %u\n",ts(), _inited?1:0);
#endif
  }

  if ((loctable->size() < 2)  && (alive())) {
#ifdef CHORD_DEBUG
    fprintf(stderr,"%s join failed! return %u\n", ts(), fr.v.size());
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
#ifdef CHORD_DEBUG
  if (me.ip == DNODE) {
    printf("%s special treatment\n",ts());
  }
#endif

  IDMap pred1 = loctable->pred(me.id-1);
  IDMap succ1 = loctable->succ(me.id+1);
  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);
  if (!succ1.ip) return;

#ifdef CHORD_DEBUG
  printf("%s Chord stabilize BEFORE pred %u,%qx succ %u,%qx succsz %u _inited %u\n", ts(), pred1.ip, pred1.id, succ1.ip, succ1.id, succs.size(), _inited?1:0);
#endif

  fix_successor();
  if (!alive()) return;

  succs = loctable->succs(me.id+1, _nsucc);

  if ((_nsucc > 1) && ((succs.size() < _nsucc*0.5) || (now()-_last_succlist_stabilized > _stab_succlist_timer))) {
    _last_succlist_stabilized = now();
    fix_successor_list();
  }

  if (!alive()) return;

  fix_predecessor();
  if (!alive()) return;

#ifdef CHORD_DEBUG
  IDMap pred = loctable->pred(me.id-1);
  IDMap succ = loctable->succ(me.id+1);
  succs=loctable->succs(me.id+1,_nsucc);
  printf("%s Chord stabilize AFTER pred %u,%qx succ %u,%qx _inited %u succsz %u\n", ts(), pred.ip, pred.id, succ.ip, succ.id,  _inited, succs.size());
#endif
}

bool
Chord::stabilized(vector<CHID> lid)
{
  vector<CHID>::iterator iter;
  iter = find(lid.begin(), lid.end(), me.id);
  assert(iter != lid.end());

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);

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
  get_successor_list_args gsa;
  get_successor_list_ret gsr;

  gsa.m = 1;
  record_stat(0, TYPE_FIXPRED_UP);
  if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(pred.ip));
      ok = _vivaldi->doRPC(pred.ip, target, 
	  &Chord::get_successor_list_handler, &gsa, &gsr);
  } else 
      ok = doRPC(pred.ip, &Chord::get_successor_list_handler, &gsa, &gsr);

  if (ok) record_stat(4, TYPE_FIXPRED_UP);

  if (!alive()) return;

  if (ok) {
    loctable->add_node(pred); //refresh timestamp
    if (gsr.v.size() > 0) {
      loctable->add_node(gsr.v[0]);
      if (gsr.v[0].ip == me.ip) {
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
  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  bool ok;
  Chord *target;
  alert_args aa;

  assert(alive());

  while (1) {

    aa.n.ip = 0;

    succ1 = loctable->succ(me.id+1);
    if (succ1.ip == 0 || succ1.ip == me.ip) {
      //sth. wrong, i lost my succ, join again
      if (!_join_scheduled) {
	_join_scheduled++;
	delaycb(0, &Chord::join, (Args *)0);
      }
      return;
    }

    record_stat(0, TYPE_FIXSUCC_UP);
    if (_vivaldi) {
      target = dynamic_cast<Chord *>(getpeer(succ1.ip));
      ok = _vivaldi->doRPC(succ1.ip, target, &Chord::get_predecessor_handler, &gpa, &gpr);
    } else
      ok = doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);
  
    if (ok) record_stat(4, TYPE_FIXSUCC_UP);

    if (!alive()) return;

    if (!ok) {
#ifdef CHORD_DEBUG
      printf("%s fix_successor old succcessor (%u,%qx) died\n", 
	  ts(), succ1.ip, succ1.id);
#endif
      loctable->del_node(succ1); //successor dead
      aa.n = succ1;
    } else {
      loctable->add_node(succ1,true); //refresh timestamp
#ifdef CHORD_DEBUG
      printf("%s fix_successor successor (%u,%qx)'s predecessor is (%u, %qx)\n", 
	ts(), succ1.ip, succ1.id, gpr.n.ip, gpr.n.id);
#endif

      if (gpr.n.ip && gpr.n.ip == me.ip) {
	return;
      }else if (gpr.n.ip && gpr.n.ip!= me.ip) {
	if (ConsistentHash::between(me.id, succ1.id, gpr.n.id)) {
	  loctable->add_node(gpr.n,true); //successor has changed, i should stabilize it immeidately
	} else {
	  assert(ConsistentHash::between(gpr.n.id, succ1.id, me.id));
	  pred = loctable->pred(me.id-1);
	  if (ConsistentHash::between(pred.id,me.id,gpr.n.id))
	    loctable->add_node(gpr.n);
	  //my successor's predecessor is behind me
	  //notify my succ of his predecessor change
	  notify_args na;
	  notify_ret nr;
	  na.me = me;

	  //XXX what if the alert message is lost
	  record_stat(4, TYPE_FIXSUCC_UP);
	  if (_vivaldi) {
	    Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
	    ok = _vivaldi->doRPC(succ1.ip, target, &Chord::notify_handler, &na, &nr);
	  } else
	    ok = doRPC(succ1.ip, &Chord::notify_handler, &na, &nr);
	  if (ok) record_stat(0, TYPE_FIXSUCC_UP);

	  if(!alive()) return;
      
	  if (!ok) {
#ifdef CHORD_DEBUG
	    printf("%s notify succ dead %u,%qx\n",ts(), succ1.ip, succ1.id);
#endif
	    loctable->del_node(succ1); //successor dead
	    aa.n = succ1;
	  } 
	}
      }
    }

    /* notify my new successor of this node's death*/
    if (aa.n.ip) {
      succ2 = loctable->succ(me.id+1); 
      aa.n = succ1;
      if (succ2.ip) {
	record_stat(4,TYPE_FIXSUCC_UP);
	doRPC(succ2.ip, &Chord::alert_handler, &aa, (void*)NULL);
	//i should not immediately stabilize new successor,  
	//i should wait for this new successor to discover the failure himself
	return;
      }
    }
  }
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, 
				  get_successor_list_ret *ret)
{
  check_static_init();
  ret->v = loctable->succs(me.id+1,args->m);
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(me.id+1);
  if (!succ.ip) return;

  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  bool ok;

  gsa.m = _nsucc;

  record_stat(0, TYPE_FIXSUCCLIST_UP);
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ.ip));
    ok = _vivaldi->doRPC(succ.ip, target, &Chord::get_successor_list_handler, 
	&gsa, &gsr);
  } else
    ok = doRPC(succ.ip, &Chord::get_successor_list_handler, &gsa, &gsr);
  if (ok) record_stat(gsr.v.size()*4, TYPE_FIXSUCCLIST_UP);

  if (!alive()) return;

#ifdef CHORD_DEBUG
  vector<IDMap> v = loctable->succs(me.id+1,_nsucc);
  printf("%s fix_successor_list (succ %u,%qx): ",ts(), succ.ip,succ.id);
  for (uint i = 0; i < gsr.v.size(); i++) {
    printf("%u,%qx ", gsr.v[i].ip, gsr.v[i].id);
  }
  printf("\n");
#endif

  if (!ok) {
    loctable->del_node(succ);
  }else{

    loctable->add_node(succ,true);//update timestamp

    vector<IDMap> scs = loctable->succs(succ.id + 1, _nsucc);
    uint scs_i = 1;
    uint gsr_i = 0;
    while (1) {
      if (scs_i >= scs.size() || gsr_i >= gsr.v.size()) break;
      if (ConsistentHash::between(me.id,gsr.v[gsr_i].id,scs[scs_i].id)) {
	//delete the successors that my successor failed to pass to me
#ifdef CHORD_DEBUG
	printf("%s del %u %u node %u,%qx (%u,%qx) from successor list\n",ts(), scs_i, gsr_i, scs[scs_i].ip, scs[scs_i].id, gsr.v[gsr_i].ip,gsr.v[gsr_i].id);
#endif
	loctable->del_node(scs[scs_i]);
	scs_i++;
      } else {
	loctable->add_node(gsr.v[gsr_i], true);
	gsr_i++;
	scs_i++;
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

  /*
  IDMap tmp = loctable->succ(me.id+1);
  assert(tmp.ip!=args->n.ip);
  */

  //due to non-transitivity problem, i check before i delete, 
  //this will make find_successor() slower, but fortunately iterative lookup is async
  record_stat(0,TYPE_JOIN_LOOKUP); //LJY: temporarily lump this as join traffic
  bool b = doRPC(args->n.ip,&Chord::null_handler,(void *)NULL, (void *)NULL);
  if (!b) loctable->del_node(args->n);
}

void
Chord::get_predecessor_handler(get_predecessor_args *args,
                               get_predecessor_ret *ret)
{
  assert(!static_sim);
  ret->n = loctable->pred(me.id-1);
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
  loctable->init(me);
}

void
Chord::crash(Args *args)
{
  ChordObserver::Instance(NULL)->delnode();
  assert(!static_sim);
  if (vis)
    printf ("vis %llu crash %16qx\n", now (), me.id);

  // XXX: Thomer says: not necessary
  //node()->crash ();
  _inited = false;
  loctable->del_all();
  loctable->init(me);
#ifdef CHORD_DEBUG
  if (me.ip == DNODE) 
    fprintf(stderr,"%s crashed\n", ts());
  printf("%s crashed! loctable sz %d\n",ts(), loctable->size());
#endif

}


/*************** LocTable ***********************/

LocTable::LocTable()
{
  _evict = false;
} 

void LocTable::init (Chord::IDMap m)
{
  me = m;
  pin(me.id, 1, 0);
  idmapwrap *elm = New idmapwrap(me, now());

  bool ok = ring.insert(elm);
  assert(ok);
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

//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id, Time *ts)
{
  if (size() == 1) {
    return me;
  }
#ifdef CHORD_DEBUG
  uint before = size();
#endif
  vector<Chord::IDMap> v = succs(id, 1, ts);
  uint vsz = v.size();
  if (vsz > 0) {
    return v[0];
  }
#ifdef CHORD_DEBUG

  if (id == (me.id + 1)) {
    idmapwrap *ptr = ring.closestsucc(id);
    fprintf(stderr,"ring sz %u before %u me %d %qx ptr %u,%qx is_succ %d timestamp %llu\n", size(), before, me.ip,me.id,ptr->n.ip,ptr->n.id,ptr->is_succ?1:0, ptr->timestamp);
  }
#endif
  Chord::IDMap tmp;
  tmp.ip = 0;
  return tmp;
}

/* returns m successors including or after the number id
   also returns the timestamp of the first node
 */
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m, Time *ts)
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
    ptrnext = ring.next(ptr);
    if (!ptrnext) ptrnext = ring.first();

    if ((id == (me.id+1)) && (!ptr->is_succ)) return v;

    if (ptr->n.ip == me.ip) return v;

    if ((!_timeout)||(t - ptr->timestamp) < _timeout) {
      if (v.size() > 0) 
	assert(ptr->n.ip != v[v.size()-1].ip);
      if ((v.size() == 0) && (ts)){
	*ts = ptr->timestamp;
      }
      v.push_back(ptr->n);
      j++;
      if (j >= ring.size()) return v;
    }else{
      assert(ptr->n.ip!=me.ip);
      ring.remove(ptr->id);
      //printf("%u,%qx del %p\n", me.ip, me.id, ptr);
      bzero(ptr, sizeof(*ptr));
      delete ptr;
      if (ring.size() <= 1) {
	return v;
      }
      //assert(ring.size() >= 2);
    }
    ptr = ptrnext;
  }
  return v;
}

Chord::IDMap
LocTable::pred(Chord::CHID id)
{
  if (size() == 1) {
    return me;
  }
  vector<Chord::IDMap> v = preds(id, 1);
  if (v.size() > 0) {
    return v[0];
  }

  Chord::IDMap tmp;
  tmp.ip = 0;
  return tmp;
}

vector<Chord::IDMap>
LocTable::preds(Chord::CHID id, uint m) 
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
    elmprev = ring.prev(elm);
    if (!elmprev) elmprev = ring.last();

    if ((!_timeout) || (elm->id == me.id)
	|| (t - elm->timestamp < _timeout)) { //never delete myself
      if (elm->n.ip == me.ip) break;
      v.push_back(elm->n);
      if (v.size() == m) break;
    }else {
      assert(elm->n.ip != me.ip);
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

void
LocTable::add_node(Chord::IDMap n, bool is_succ)
{
  Chord::IDMap succ1; 
  Chord::IDMap pred1; 

  assert(n.ip > 0 && n.ip < 32000);
  if (vis) {
    succ1 = succ(me.id+1);
    pred1 = pred(me.id-1);
  }
 
  idmapwrap *elm = ring.closestsucc(n.id);
  if (elm->id == n.id) {
    elm->timestamp = now();
    if (is_succ) 
      elm->is_succ = is_succ;
    return;
  } else {
    idmapwrap *newelm = New idmapwrap(n,now());
    if (elm->is_succ) 
      newelm->is_succ = true;
    else
      newelm->is_succ = is_succ;
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
}

void
LocTable::del_node(Chord::IDMap n)
{
#ifdef CHORD_DEBUG
  Chord::IDMap succ1 = succ(me.id+1);
#endif
  assert(n.ip != me.ip);
  idmapwrap *elm = ring.search(n.id);
  if (!elm) return;

  elm = ring.remove(n.id);
  //printf("%u,%qx del %p\n", me.ip, me.id, elm);
  bzero(elm,sizeof(*elm));
  delete elm;
#ifdef CHORD_DEBUG
  Chord::IDMap succ2 = succ(me.id +1);
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
  return preds(key, nsz);
}

Chord::IDMap
LocTable::next_hop(Chord::CHID key, uint m, uint nsucc)
{
  vector<Chord::IDMap> v = next_hops(key, 1);
  if (v.size() == 0) 
    return me;
  else
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
