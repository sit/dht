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

#include  "chordonehop.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
using namespace std;
extern bool static_sim;

ChordOneHop::ChordOneHop(IPAddress i, Args &a): Chord(i, a)
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
ChordOneHop::reschedule_basic_stabilizer(void *x)
{
  assert(!static_sim);
  printf("%s start stabilizing\n",ts());
  if (!alive()) {
    _stab_basic_running = false;
    return;
  }

  _stab_basic_running = true;
  if (_stab_basic_outstanding > 0) {
  }else{
    _stab_basic_outstanding++;
    stabilize();
    _stab_basic_outstanding--;
    assert(_stab_basic_outstanding == 0);
  }
  delaycb(_stab_basic_timer, &ChordOneHop::reschedule_basic_stabilizer, (void *) 0);
}

void
ChordOneHop::stabilize()
{
  //completely different stabilizer

  //ping successor
  IDMap succ = loctable->succ(me.id + 1);
  assert(succ.ip);
  //record_stat(); 
  bool ok;
  ok = doRPC(succ.ip, &Chord::null_handler, (void *)NULL, &succ);

  if (!ok) { //notify the whole world
    loctable->del_node(succ);
    vector<Chord::IDMap> v = loctable->get_all();

    deladd_args da;
    da.n = succ;

    unsigned rpc = 0;
    RPCSet rpcset;

    for (uint i = 0; i < v.size(); i++) {
      if (v[i].ip == me.ip) continue;
      //record_stat();
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
      assert(outstanding == rpcset.size());
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
ChordOneHop::getloctable_handler(void *args, get_predsucc_ret *ret)
{
  ret->v = loctable->get_all();
  assert(ret->v.size() < 2000);
}

void
ChordOneHop::join(Args *args)
{

  _inited = true;
  IDMap wkn;
  wkn.ip = args->nget<IPAddress>("wellknown");
  assert (wkn.ip);
  wkn.id = ConsistentHash::ip2chid(wkn.ip);
  loctable->add_node(wkn);

  get_predsucc_ret gr;
  //record_stat();

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
  rpcset.clear();
  unsigned total = 0;

  for (uint i = 0; i < v.size(); i++) {
    if (v[i].ip == me.ip) continue;
    //record_stat();
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
    assert(outstanding == rpcset.size());
    assert(aa.n.ip == me.ip);
  }

}

vector<Chord::IDMap>
ChordOneHop::find_successors(CHID k, uint m, uint all, bool is_lookup)
{
  vector<Chord::IDMap> v;
  v.clear();

  uint timeout = 0;
  IDMap succ;
  bool ok;

  //go directly to the successor
  while (1) {
    succ = loctable->succ(k);
    //record_stat(is_lookup?1:0);
    ok = doRPC(succ.ip, &Chord::null_handler, (void *)NULL, &succ);
    if ((!ok) && (alive())) {
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
