#include <stdio.h>

AnjaliOneHop::AnjaliOneHop(Node *n, Args& a) : P2Protocol(n)
{
  _slices = a.nget<uint>("slices",10,10);
  _units = a.nget<unit>("units",10,10);
  loctable = new AnjaliLocTable(_slices, _units);
  loctable->init(me);
  loctable->set_timeout(0); //no timeouts on loctable entries
}

AnjaliOneHop::~AnjaliOneHop()
{
}

void
AnjaliOneHop::init_state(vector<IDMap> ids)
{
  for (uint i = 0; i < ids.size(); i++) {
    loctable->add_node(ids[i]);
  }
}

void
AnjaliOneHop::join()
{
  node()->set_alive();
  wkn.ip = args->nget<IPAddress>("wellknown");
  assert(wkn.ip);
  wkn.id = ConsistentHash::ip2chid(wkn.ip);

  find_successors_args fa;
  find_successors_ret fr;

  bool ok = doRPC(wkn.ip, &AnjaliOneHop::find_successors_handler, &fa, &fr);
  
  //XXX talk to my successor to get his routing table

  //XXX: notify my slice leader of my presence

  if (!_stab_running) {
    _stab_running = true;
    reschedule_stabilizer(NULL);
  }else{
    stabilize();
  }
}

void
AnjaliOneHop::reschedule_stabilizer(void *x)
{
  if (!node()->alive()) {
    _stab_running = false;
    return;
  }
  _stab_running = true;
  if (_stab_outstanding > 0) {
  }else{
    _stab_outstanding++;
    stabilize();
    _stab_outstanding--;
    assert(_stab_outstanding == 0);
  }
  delaycb(_stabtimer, &AnjaliOneHop::reschedule_stabilizer, (void *)0);
}

void
AnjaliOneHop::stabilize()
{
  notifyevent_args piginfo;
  notifyevent_args na;
  deadalive_event e;
  bool ok;

  na.v.clear();
  piginfo.v.clear();

  for (uint i = 0; i < piggyback.size(); i++) {
    piginfo.v.push_back(piggyback.front());
    piggyback.pop();
  }

  //piggy back events in stabilization to my successor 
  //if it is dead, piggy back events to my successors' successor
  //i keep doing it, until the events are eventually passed to 
  //somebody, so i can delete it from my queue
  while (!ok) {
    IDMap succ = loctable->succ(me.id+1);
    //ping the successor to see if it is alive
    ok = doRPC(succ.ip, &AnjaliOneHop::ping_handler, &piginfo, (void *)NULL);
    if (!ok) {
      loctable->del_node(succ);
      e.upordown = 0; 
      e.n = succ;
      na.v.push_back(e);
    }
  }

  while (!ok) {
    IDMap pred = loctable->pred(me.id+1);
    ok = doRPC(pred.ip, &AnjaliOneHop::ping_handler, &piginfo, (void *)NULL);
    if (!ok) {
      loctable->del_node(pred);
      e.upordown = 0;
      e.n = pred;
      na.v.push_back(e);
    }
  }

  //if either successor or predecessor has died, notify slice leader 
  ok = false;
  if (na.v.size() > 0) {
    while (!ok) {
      IDMap sliceleader = loctable->sliceleader(XXX);
      ok = doRPC(sliceleader.ip, &AnjaliOneHop::notifyevent_handler, &na, (void *)NULL);
      if (!ok) loctable->del_node(sliceleader);
    }
  }
}

void
AnjaliOneHop::ping_handler(notifyevent_args *args, void *ret) 
{
  for (uint i = 0; i < args.v.size(); i++) {
    if () //the node i receive this info from is from another slice or unit, stop adding on piggybacked list
      piggyback.push(args.v.size());
    if (args.v[i].upordown) //if up
      loctable->add_node(args.v[i]);
    else
      loctable->del_node(args.v[i]);
  }
}

void
AnjaliOneHop::notifyevent_handler(notifyevent_args *args, void *ret)
{
  //a slice leader get events from ordinary nodes from my own slice, 
  //aggregate and distribute to other slice leaders and my own unit leaders
  for (unit i = 0; i < args->v.size(); i++) 
    agg_events.push(args->v[i]);

  //each event has 5 bytes(4 bytes of ID + 1 byte of extra)
  //IP header is 20, so given maximum pkt size is 1500 bytes,
  //there is no point aggregating more than 296 events 
  if (agg_events.size() > 296) {
    vector<> v;
    v.clear();
    for (uint i = 0; i < 296; i++) {
      v.push_back(agg_events.front());
      agg_events.pop();
    }
    vector<IDMap> leaders;
    leaders = loctable->sliceleaders(); 
    notifyleaders(leaders,v); //this includes notify myself via local RPC
  }
}

void
AnjaliOneHop::notifyleaders(vector<IDMap> leaders, vector<deadalive_event> es)
{
  notifyevent_args na;
  assert(loctable->is_sliceleader());
  na.v = es;
  RPCSet rpcset;
  hash_map<unsigned, IDMap> contactmap;

  for (uint i = 0; i < leaders.size(); i++) {
    rpc = asyncRPC(leaders[i].ip, &AnjaliOneHop::notifyfromslice_handler, &na, (void *)NULL);
    rpcset.insert(rpc);
    contactmap[rpc] = leaders[i];
  }

  bool ok;
  for (uint i = 0; i < v.size(); i++) {
    rpc = rcvRPC(&rpcset, ok);
    //some slice/unit leaders have failed, what should be done? maybe nothing?
    if (!ok) {
      loctable->del_node(contactmap[rpc]);
      AODEBUG(2) << "notifysliceleaders: failure happened to " << contactmap[rpc].ip << ConsistentHash::printbits(contactmap[rpc].id) << endl;
    }
  }
}

void
AnjaliOneHop::notifyfromslice_handler(notifyevent_args *args, void *ret)
{
  //if i am a slice leader, i distribute this info to my unit leaders
  if (loctable->is_sliceleader()) {
    vector leaders = loctable->unitleaders();
    notifyleaders(leaders, args->v);
  }else{
    //if i am a unit leader, start pig back this info to successor and predecessor
    for (uint i = 0; i < args.v.size(); i++) 
      piggyback.push(args.v[i]);
  }
}

