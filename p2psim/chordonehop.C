#include  "chordonehop.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
using namespace std;
extern bool static_sim;

ChordOneHop::ChordOneHop(Node *n, Args &a): Chord(n,a)
{
}

ChordOneHop::~ChordOneHop()
{
}

void
ChordOneHop::init_state(vector<IDMap> ids)
{
  for (uint i = 0; i < ids.size(); i++) {
    loctable->add_node(ids[i]);
  }
  printf("%s init_state %d entries\n", ts(), loctable->size());
}
void
ChordOneHop::reschedule_stabilizer(void *x)
{
  printf("%s start stabilizing\n",ts());
  if (!node()->alive()) {
    _stab_running = false;
    return;
  }

  Time t = now();
  ChordOneHop::stabilize();
  printf("%s end stabilizing\n",ts());

  t = now() - t - _stabtimer;
  if (t < 0) t = 0;
  delaycb(_stabtimer, &ChordOneHop::reschedule_stabilizer, (void *)0);
}

void
ChordOneHop::stabilize()
{
  //completely different stabilizer

  //ping successor
  IDMap succ = loctable->succ(me.id + 1);
  assert(succ.ip);
  record_stat(); 
  bool ok;
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord*>(getpeer(succ.ip));
    ok = _vivaldi->doRPC(succ.ip, target, &Chord::null_handler, 
	(void *)NULL, (void *)NULL);
  }else{
    ok = doRPC(succ.ip, &Chord::null_handler, (void *)NULL, (void *)NULL);
  }

  if (!ok) { //notify the whole world
    loctable->del_node(succ);
    vector<Chord::IDMap> v = loctable->get_all();

    deladd_args da;
    da.n = succ;

    unsigned rpc = 0;
    RPCSet rpcset;

    for (uint i = 0; i < v.size(); i++) {
      if (v[i].ip == me.ip) continue;
      record_stat();
      //cancelRPC(asyncRPC(v[i].ip, &ChordOneHop::del_handler, 
      //&da, (void *)NULL)); 
      rpc = asyncRPC(v[i].ip, &ChordOneHop::del_handler, &da, (void *)NULL); 
      assert(rpc);
      rpcset.insert(rpc);
      //doRPC(v[i].ip, &ChordOneHop::del_handler, &da, (void *)NULL); 
    }

    //i must receive these RPCs, otherwise, aa got deallocated 
    //and receivers will get garbage
    uint outstanding = rpcset.size();
    while (outstanding > 0) {
      // unsigned donerpc = rcvRPC(&rpcset);
      outstanding--;
    }
  } 

}

void
ChordOneHop::del_handler(deladd_args *args, void *ret)
{
  loctable->del_node(args->n);
}

void
ChordOneHop::add_handler(deladd_args *args, void *ret)
{
  loctable->add_node(args->n);
}

void
ChordOneHop::getloctable_handler(void *args, get_successor_list_ret *ret)
{
  ret->v = loctable->get_all();
  assert(ret->v.size() < 2000);
}

void
ChordOneHop::join(Args *args)
{
  if (_vivaldi_dim > 0) {
    _vivaldi = New Vivaldi10(node(), _vivaldi_dim, 0.05, 1); 
  }

  _inited = true;
  IDMap wkn;
  wkn.ip = args->nget<IPAddress>("wellknown");
  assert (wkn.ip);
  wkn.id = ConsistentHash::ip2chid(wkn.ip);
  loctable->add_node(wkn);

  get_successor_list_ret gr;
  record_stat();

  bool ok = doRPC(wkn.ip, &ChordOneHop::getloctable_handler, (void *)NULL, &gr);
  assert(ok);

  for (uint i = 0; i < gr.v.size(); i++) {
    loctable->add_node(gr.v[i]);
  }
  printf("%s haha get loctable size %d new loctable size %d\n",
      ts(), gr.v.size(), loctable->size());

  deladd_args aa;
  aa.n  = me;
  vector<Chord::IDMap> v = loctable->get_all();

  unsigned rpc = 0;
  RPCSet rpcset;
  unsigned total = 0;

  for (uint i = 0; i < v.size(); i++) {
    if (v[i].ip == me.ip) continue;
    record_stat();
    assert(v[i].ip);
    //cancelRPC(asyncRPC(v[i].ip, &ChordOneHop::add_handler, 
	//	&aa, (void *)NULL)); 
    rpc = asyncRPC(v[i].ip, &ChordOneHop::add_handler, &aa, (void *)NULL); 
    assert(rpc);
    rpcset.insert(rpc);
    assert(aa.n.ip == me.ip);
    total++;
    //doRPC(v[i].ip, &ChordOneHop::add_handler, &aa, (void *)NULL); 
  }

  assert(total == rpcset.size());
  //i must receive these RPCs, otherwise, aa got deallocated 
  //and receivers will get garbage
  uint outstanding = rpcset.size();
  while (outstanding > 0) {
    // unsigned donerpc = rcvRPC(&rpcset);
    outstanding--;
  }

}

vector<Chord::IDMap>
ChordOneHop::find_successors(CHID k, uint m, bool is_lookup)
{
  vector<Chord::IDMap> v;
  v.clear();

  uint timeout = 0;
  IDMap succ;
  bool ok;

  //go directly to the successor
  while (1) {
    succ = loctable->succ(k);
    record_stat(is_lookup?1:0);
    ok = doRPC(succ.ip, &Chord::null_handler, (void *)NULL, (void *)NULL);
    if ((!ok) && (node()->alive())) {
      timeout++;
      loctable->del_node(succ);
    }else{
      v.push_back(succ);
      break;
    }
  }

  if (is_lookup) {
    printf ("find_successor for (id %qx, key %qx)", me.id, k);
    printf ("is (%u, %qx) hops 2 timeout %d\n", v[0].ip, v[0].id,timeout);
  }
  return v;
}
