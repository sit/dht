/*
 * Copyright (c) 2003-2005 [Anjali Gupta]
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

/* jy: (main additions)
 * all doRPC and xRPC have been changed to fd_xRPC that does failure detection 
 * (for a lossy/congested network)
 * for every lookup, check its correctness against global state
 * to fix(?) currently failure_detection is done with the failure of one packet
 */
#include "onehop.h"
#include "consistenthash.h"
#include "observers/onehopobserver.h"
#include <iostream>

#define TESTPRINT(me,he,tag) if (me.ip==1159 && he.ip==2047) printf("WUWU now %llu tag %s\n", now(),tag);

#define DEBUG_MSG(n,msg,s) if (p2psim_verbose && n.ip == OneHop::debug_node) printf("%llu %u,%qx %s knows debug from sender %u,%qx\n",now(), me.ip, me.id, msg, s.ip, s.id);

#define MAX_IDS_MSG 295
#define MAX_LOOKUP_TIME 4000

//XXX:Strange bug - delaycb gives join a 0ed argument

long OneHop::lookups = 0;
long OneHop::failed = 0;
long OneHop::two_failed = 0;
long OneHop::old_lookups = 0;
long OneHop::old_failed = 0;
long OneHop::old_two_failed = 0;
long OneHop::num_nodes = 0;
long OneHop::joins = 0;
long OneHop::crashes = 0;
long OneHop::same_lookups = 0;
long OneHop::same_failed = 0;
Time OneHop::act_interslice = 0;
Time OneHop::tot_interslice = 0;
long OneHop::nonempty_outers = 0;
long OneHop::nonempty_leaders = 0;
Time OneHop::act_intraslice = 0;
Time OneHop::tot_intraslice = 0;
Time OneHop::total_empty = 0;
Time OneHop::total_count = 0;
Time OneHop::exp_intraslice = 0;
bw OneHop::bandwidth = 0;
bw OneHop::leader_bandwidth = 0;
bw OneHop::messages = 0;
bw OneHop::leader_messages = 0;
bw OneHop::lookup_bandwidth = 0;
bw OneHop::old_bandwidth = 0;
bw OneHop::old_lookup_bandwidth = 0;
bw OneHop::old_leader_bandwidth = 0;
unsigned OneHop::start = 0;
Time OneHop::old_time = 0;
int OneHop::_publish_time = -1; //jy: control how frequently one publishes statistics
unsigned OneHop::num_violations = 0; //jy: anjali sends too many ids in one message, calculate how many violations there are
vector<double> OneHop::sliceleader_bw_avg;
unsigned OneHop::debug_node = 1;

OneHop::OneHop(IPAddress i , Args& a) : P2Protocol(i)
{
  _k = a.nget<uint>("slices",5,10);
  //number of units -- must be an odd number
  _u = a.nget<uint>("units",5,10);
  loctable = New OneHopLocTable(_k, _u);
  me.id = id();
  me.ip = ip();
  loctable->add_node(me);
  retries = 0; 
  _stab_timer = a.nget<uint>("stab",1000,10);
  leader_log.clear();
  outer_log.clear();
  _join_complete = false;
  _retry_timer = 1000;
  prev_slice_leader = false;
  _to_multiplier = 3; //jy  doRPC suffer from timeout if the dst is dead, the min of this value = 3
 // slice_size = 0xffffffffffffffff / _k;   
  slice_size = ((ConsistentHash::CHID)-1)/ _k;   
  //lookups = 0;
  //failed = 0;
  _leaderstab_running = false;
  low_to_high.clear();
  high_to_low.clear();
  sent_low = false;
  sent_high = false;
  if (me.ip == 1)
    OneHop::sliceleader_bw_avg.clear();
  last_stabilize = 0;
  _wkn.ip = 0;
}

void
OneHop::record_stat(IPAddress src, IPAddress dst, uint type, uint num_ids, uint num_else)
{
  assert(type <= 5);
  // printf("now %llu warning: %u sending too much %u\n", now(), me.ip, num_ids);
  //assert(num_ids < 300);
  if (num_ids > 300) {
    OneHop::num_violations += (num_ids/300);
    Node::record_bw_stat(type,num_ids,40 * (num_ids/300) + num_else);
    Node::record_inout_bw_stat(src,dst,num_ids,40 * (num_ids/300) + num_else);
  }else{
    Node::record_bw_stat(type,num_ids,num_else);
    Node::record_inout_bw_stat(src,dst,num_ids,num_else);
  }
}

//jy: check correctness of lookups
bool
OneHop::check_correctness(CHID k, IDMap n)
{
  vector<IDMap> ids = OneHopObserver::Instance(NULL)->get_sorted_nodes();
  IDMap tmp;
  tmp.id = k;
  uint idsz = ids.size();
  uint pos = upper_bound(ids.begin(), ids.end(), tmp, OneHop::IDMap::cmp) - ids.begin();
  if (now() == 4227297 && n.ip == 504)
    fprintf(stderr,"shit!\n");
  while (1) {
    if (pos >= idsz) pos = 0;
    IDMap hehe = ids[pos];
    if (Network::Instance()->alive(ids[pos].ip))
      break;
    pos++;
  }
  DEBUG(4) << now() << ":" << ip() << "," << printID(id())
  << ":key " << printID(k) << " correct? " 
    << (ids[pos].ip==n.ip?1:0) << " reply " << n.ip << "," << printID(n.id)
    << " real " << ids[pos].ip << "," << printID(ids[pos].id) << endl;
  if (ids[pos].ip == n.ip) 
    return true;
  else 
    return false;
}

void
OneHop::lookup(Args *args)
{
  if (!alive()) return;

  lookup_internal_args *la = New lookup_internal_args;
  la->k = args->nget<CHID>("key");
  la->start_time = now();
  la->hops = 0;
  la->timeouts = 0;
  la->timeout_lat = 0;
  la->attempts = 0;
  lookup_internal(la);
}

void
OneHop::lookup_internal(lookup_internal_args *la) 
{

  la->attempts++;

  if (!alive() || now()-la->start_time > MAX_LOOKUP_TIME) {
    record_lookup_stat(me.ip, me.ip, now()-la->start_time, false, false,
	la->hops,la->timeouts,la->timeout_lat);
    delete la;
    return;
  }

  if (!_join_complete) {
    DEBUG(1) << now() << ":" << me.ip << ","<< printID(me.id) 
      << ": failed at time "<<now()<< "coz join is incomplete last_join time " << last_join << endl;
    delaycb(1000, &OneHop::lookup_internal, la);
    /*
    record_lookup_stat(me.ip, me.ip, now()-la->start_time, false, false,
	la->hops,la->timeouts,la->timeout_lat);
    delete la;
    */
    return;
  }

  //DEBUG(1) << ip() << ":(slice " << slice(id()) << "): Lookup for " << k << "(slice " << slice(k) << ")" << endl;
  lookup_args a;
  lookup_ret r;
  a.key = la->k;
  a.sender = me;
  a.dead_nodes.clear();

  IDMap succ_node = me;
  while ((now()-la->start_time) < MAX_LOOKUP_TIME) {
    succ_node = loctable->succ(la->k);
    assert(succ_node.ip);

    record_stat(me.ip,succ_node.ip,TYPE_USER_LOOKUP,2);
    bool ok = doRPC(succ_node.ip,&OneHop::lookup_handler,&a,&r,
	TIMEOUT(me.ip,succ_node.ip));
    if (ok) record_stat(succ_node.ip,me.ip,TYPE_USER_LOOKUP,r.is_owner?0:1,1);

    if (me.ip!=succ_node.ip)
      la->hops++;

    if (!alive()) break;
    DEBUG(1) << now() << ":" << me.ip << "," << printID(me.id) 
	     << " lookup done from " << succ_node.ip << "/" 
	     << printID(succ_node.id) << " for key " << printID(a.key) 
	     << ", ok: " << ok << ", is_owner: " << r.is_owner;
    if( !r.is_owner ) {
      DEBUG(1) << ", correct_owner=" << printID(r.correct_owner.id);
    }
    DEBUG(1) << endl;
    if (ok) {
      if (r.is_owner) {
	break;
      }else{
	DEBUG_MSG(r.correct_owner, "lookup", succ_node);
	loctable->add_node(r.correct_owner);
      }
    }else{
      la->timeouts++;
      la->timeout_lat += TIMEOUT(me.ip, succ_node.ip);
      loctable->del_node(succ_node.id);
      test_inform_dead_args *aa = New test_inform_dead_args;
      aa->suspect= succ_node;
      aa->informed = me;
      aa->justdelete = true;
      delaycb(0,&OneHop::test_dead_inform,aa);
      //return;

      // please don't tell us about this guy anymore, he sucks
      a.dead_nodes.push_back(succ_node.id);

    }
  }

  
  if (!alive()) {
    delete la;
  } else if ((now()-la->start_time) > MAX_LOOKUP_TIME) {
    record_lookup_stat(me.ip, me.ip, now()-la->start_time, false, false,
		       la->hops,la->timeouts,la->timeout_lat);
    delete la;
  } else if (check_correctness(la->k,succ_node)) {
    record_lookup_stat(me.ip, succ_node.ip, now()-la->start_time, true, true,
		       la->hops,la->timeouts,la->timeout_lat);
    delete la;
  }else{
    record_lookup_stat(me.ip, succ_node.ip, now()-la->start_time, true, false,
	la->hops,la->timeouts,la->timeout_lat);
    delete la;
    //delaycb(100,&OneHop::lookup_internal, la);
  }
}


/* anjali's old lookup procedure
   it only tries two hops and declare failure
   why not try more hops?? 
void
OneHop::lookup(Args *args) {
  if (!alive()) return;
  if (!_join_complete) {
    //jy: this is a failure
    record_lookup_stat(me.ip, me.ip, 0, false, false, 1, 0, 0);
    return;
  }

  Time before0 = now(); //jy: record the start of lookup

  lookups++;
  CHID k = args->nget<CHID>("key");
  bool same = (slice(k) == slice(me.id));
  if (same) same_lookups++;

  //DEBUG(1) << ip() << ":(slice " << slice(id()) << "): Lookup for " << k << "(slice " << slice(k) << ")" << endl;
  assert(k);
  lookup_args *a = New lookup_args;
  lookup_ret r;
  a->key = k;
  a->sender = me;
  IDMap succ_node = loctable->succ(k);
  assert(succ_node.ip);

  record_stat(TYPE_USER_LOOKUP,2);
  bool ok = doRPC(succ_node.ip,&OneHop::lookup_handler,a,&r);
  if (ok) record_stat(TYPE_USER_LOOKUP,1);

  Time before1 = now(); //jy: record the end of first lookup hop
  if (!alive()) goto LOOKUP_DONE;

  lookup_bandwidth += 24;
  if (!ok) {
    failed++;
    two_failed++;
    if (same) same_failed++;
    DEBUG(5) << ip() << ":Lookup failed due to dead node "<< succ_node.ip << endl;
    IDMap *n = New IDMap();
    *n = succ_node;
    assert(n->ip>0);
    delaycb(0, &OneHop::test_dead, n);
    IDMap succ_node = loctable->succ(succ_node.id+1);

    record_stat(TYPE_USER_LOOKUP,2);
    bool ok = doRPC(succ_node.ip, &OneHop::lookup_handler, a, &r); 
    if (!alive()) goto LOOKUP_DONE;
    if (ok) record_stat(TYPE_USER_LOOKUP,1);
    
    lookup_bandwidth += 24;
    if (!ok) {
      IDMap *nn = New IDMap;
      *nn = succ_node;
      assert(nn->ip>0);
      delaycb(0,&OneHop::test_dead,nn);
    }
    else if (r.is_owner) {
      if (check_correctness(k,succ_node))
	record_lookup_stat(me.ip, succ_node.ip, now()-before0, true, true, 2, 1, now()-before1); //jy: record lookup stat
      else
	record_lookup_stat(me.ip, succ_node.ip, now()-before0, false, false, 2, 1, now()-before1); //jy: record lookup stat
      two_failed--;
    }
  }
  else {
    if (!r.is_owner) {
      failed++;
      two_failed++;
      lookup_bandwidth += 20;
      if (same) same_failed++;
      DEBUG(5) << ip() << ":Lookup failed:"<< succ_node.ip << "(" << succ_node.id << ") thinks " << r.correct_owner.ip << "(" << r.correct_owner.id << ")"  << endl;
      IDMap corr_owner = r.correct_owner;

      record_stat(TYPE_USER_LOOKUP,1);
      bool ok = doRPC(corr_owner.ip, &OneHop::lookup_handler, a, &r); 
      if (ok) record_stat(TYPE_USER_LOOKUP, 1);
      if (!alive()) goto LOOKUP_DONE;

      lookup_bandwidth += 48;
      if (ok && r.is_owner) {
        two_failed--;
        loctable->add_node(corr_owner);
        LogEntry *e = New LogEntry(corr_owner, ALIVE, now());
        leader_log.push_back(*e);
        delete e;
	if (check_correctness(k,corr_owner))
	  record_lookup_stat(me.ip, corr_owner.ip, now()-before0, true, true, 2, 0, 0); //jy: record lookup stat
	else
	  record_lookup_stat(me.ip, corr_owner.ip, now()-before0, false, false, 2, 0, 0); //jy: record lookup stat
      }else if (!ok) {
	record_lookup_stat(me.ip, corr_owner.ip, now()-before0, false, false, 2, 1, now()-before1); //jy: record lookup stat
      }else { //ok && !r.is_owner
	record_lookup_stat(me.ip, corr_owner.ip, now()-before0, false, false, 2, 0, 0); //jy: record lookup stat
      }
    }else{
      if (check_correctness(k,succ_node))
	record_lookup_stat(me.ip, succ_node.ip, now()-before0, true, true, 1, 0, 0); //jy: record lookup stat
      else
	record_lookup_stat(me.ip, succ_node.ip, now()-before0, false, false, 1, 0, 0); //jy: record lookup stat
    }
  }
LOOKUP_DONE:
  delete a;
}
*/

void 
OneHop::lookup_handler(lookup_args *a, lookup_ret *r) {

  for( int i = 0; i < a->dead_nodes.size(); i++ ) {
    loctable->del_node(a->dead_nodes[i]);
  }

  IDMap succ_node = loctable->succ(a->sender.id);
  if (succ_node.id != a->sender.id) {
      if (!alive()) return;
      DEBUG(5) << now() << ":" << ip() << "," << printID(id()) 
	<< ":Found new node " << a->sender.ip << " via lookup\n";
      DEBUG_MSG(a->sender,"lookup_handler",a->sender);
      loctable->add_node(a->sender);
      LogEntry *e = New LogEntry(a->sender, ALIVE, now());
      leader_log.push_back(*e);
      delete e;
  }
  CHID key = a->key;
  IDMap corr_succ = loctable->succ(key);
  if (corr_succ.id == me.id)
    r->is_owner = true;
  else {
    r->is_owner = false;
    r->correct_owner = corr_succ;
  }
}

void
OneHop::join(Args *args)
{
  if (!alive()) return;
  me.ip = ip();
  me.id = ConsistentHash::ip2chid(me.ip);
  OneHopObserver::Instance(NULL)->addnode(me); //jy
  last_join = now();
  //jy: get a random alive node from observer
  while (!_wkn.ip || _wkn.ip == me.ip)
    _wkn = OneHopObserver::Instance(NULL)->get_rand_alive_node();
 
  if (ip() == OneHop::debug_node) 
    DEBUG(0) << now() << ":" << me.ip << ","<< printID(me.id) << " start to join  wkn " << _wkn.ip << endl;
  loctable->add_node(me);

  if (args && args->nget<uint>("first",0,10)==1) { //jy: a dirty hack, one hop does not like many nodes join at once
    _join_complete = true;
    delaycb(_stab_timer/2, &OneHop::stabilize, (void *)0);
  }else if (_wkn.ip != ip()) {
    IDMap fr;
    fr.id = _wkn.id;
    fr.ip = _wkn.ip;
    join_leader(fr, fr, args);
  }
  else { 
    _join_complete = true;
    joins++;
    num_nodes++;
    stabilize((void *)0);
    publish((void *)0);
  }

  //who is my successor?
  if (alive()) {
    IDMap succ_node = loctable->succ(me.id+1);
    DEBUG(1) << now() << ":" << me.ip << "," << printID(me.id) << " succ_ip " 
      << succ_node.ip << " succ_id " << succ_node.id << " join_complete " << _join_complete<< endl;
  }
}

void
OneHop::publish(void *v) 
{
  if ((((now() - 1) % 100000) == 0) || (now() > 2000000 && now() < 2500000)) {

  Time interval = (now() - old_time)/1000;
  DEBUG(1) << "----------------------------------------------\n";
  DEBUG(1) << "GRAND STATS\n";
  DEBUG(1) << "Total number of lookups in the last cycle:" << lookups - old_lookups << endl;
  DEBUG(1) << "Total number of one hop failures in the last cycle:" << failed - old_failed<< endl;
  DEBUG(1) << "Fraction of lookup failures:" << (double)(failed - old_failed)/(double)(lookups - old_lookups) << endl;
  DEBUG(1) << "Fraction of two hop lookup failures:" << (double)(two_failed - old_two_failed)/(double)(lookups - old_lookups) << endl;
  DEBUG(1) << "Total number of same slice lookups:" << same_lookups << endl;
  DEBUG(1) << "Total number of same slice failures:" << same_failed << endl;
  DEBUG(1) << "Fraction of same slice failures:" << (double)same_failed/(double)same_lookups << endl;
  DEBUG(1) << "Number of intra-slice messages:" << tot_intraslice << endl;
  DEBUG(1) << "Number of intra-slice actually received:" << act_intraslice << endl;
  DEBUG(1) << "Exp number of received intra-slice mesgs:" << exp_intraslice << endl;
  DEBUG(1) << "Avg number of intra-slice recepients:" << (double)act_intraslice/(double)tot_intraslice << endl;
  DEBUG(1) << "Avg expected number of intra-slice recepients:" << (double)exp_intraslice/(double)tot_intraslice << endl;
  DEBUG(1) << "Avg number of empty units:" << (double)total_empty/(double)total_count << endl;
  DEBUG(1) << "Number of inter-slice messages:" << tot_interslice << endl;
  DEBUG(1) << "Number of inter-slice actually received:" << act_interslice << endl;
  DEBUG(1) << "Avg number of inter-slice recepients:" << (double)act_interslice/(double)tot_interslice << endl;
  DEBUG(1) << "Total number of alive and functioning nodes:" << num_nodes << endl;
  DEBUG(1) << "Total number of successful joins so far:" << joins << endl;
  DEBUG(1) << "Total number of crashes so far:" << crashes << endl;
  DEBUG(1) << "Total number of crashes with non-empty outers:" << nonempty_outers << endl;
  DEBUG(1) << "Total number of crashes with non-empty leaders:" << nonempty_leaders << endl;
  DEBUG(1) << "Average number of bytes used per second in the last cycle by normal nodes" << (double)(bandwidth - old_bandwidth)/(double)(num_nodes*interval) << endl;
  DEBUG(1) << "Average number of bytes used per second in the last cycle by slice leaders" << (double)(leader_bandwidth - old_leader_bandwidth)/(double)(_k*interval) << endl;
  DEBUG(1) << "Total maintenance traffic per second in the last cycle" << (double)(bandwidth + leader_bandwidth - old_bandwidth - old_leader_bandwidth)/(interval) << endl;
  DEBUG(1) << "Total lookup traffic per second in the last cycle" << (double)(lookup_bandwidth - old_lookup_bandwidth)/interval << endl;
  DEBUG(1) << "Average maintenance bandwidth so far " << (double) (bandwidth+leader_bandwidth)*1000/(double)now() << endl;
  DEBUG(1) << "Average lookup bandwidth so far " << (double) (lookup_bandwidth)*1000/(double)now() << endl;
  DEBUG(1) << "Average number of messages by normal nodes" << (double)messages*1000/((double)now()*num_nodes) << endl;
  DEBUG(1) << "Average number of messages by slice leaders" << (double)leader_messages*1000/((double)now()*_k) << endl;

  DEBUG(1) << "----------------------------------------------\n";

  old_lookups = lookups;
  old_failed = failed;
  old_two_failed = two_failed;
  old_bandwidth = bandwidth;
  old_leader_bandwidth = leader_bandwidth;
  old_lookup_bandwidth = lookup_bandwidth;
  old_time = now();
  }
  if (_publish_time>0)
    delaycb((Time)_publish_time, &OneHop::publish, (void *)0);

  
}
void
OneHop::crash(Args *args) 
{
  OneHopObserver::Instance(NULL)->delnode(me);
  DEBUG(1) << now() << ":" << me.ip << ","<< printID(me.id) << " crashed" << endl;
  if (is_slice_leader(me.id, me.id)) {
    DEBUG(1) << " -- Slice leader\n";
    if (join_time > 0 && Node::collect_stat() && ((now()-join_time) > 180000)) {
      double avg = (double)1000.0*node_live_outbytes/(double)(now()-join_time);
      if (avg < 0.01) {
	printf("%llu: me %u what?! avg %.2f too small last join %llu total_bytes_sent %u\n",
	    now(),ip(),avg, join_time, node_live_outbytes);
      }else{
	OneHop::sliceleader_bw_avg.push_back(avg);
      }
    }
  }
  //else DEBUG(1) << "\n";
  num_nodes--;
  crashes++;
  if (outer_log.size() > 0)
    nonempty_outers++;
  if (leader_log.size() > 0)
    nonempty_leaders++;

  /*jy: don't delete old loctable
  delete loctable;
  loctable = New OneHopLocTable(_k, _u);
  loctable->size();
  loctable->set_timeout(0); //no timeouts on loctable entries 
  */
  loctable->del_all();
  leader_log.clear();
  outer_log.clear();
  low_to_high.clear();
  high_to_low.clear();
  _join_complete = false;
  _wkn.ip = 0;
}

void
OneHop::join_leader(IDMap la, IDMap sender, Args *args) {

  IPAddress leader_ip = la.ip;
  DEBUG(1) << now() << ":" << ip() << "," << printID(id()) << " joining " << leader_ip << " in slice " << slice(id()) << endl;
  assert(leader_ip);
  join_leader_args ja;
  join_leader_ret* jr = New join_leader_ret (_k, _u);
  ja.key = id();
  ja.ip = ip();

  //send mesg to node, if it is slice leader will respond with
  //routing table, else will respond with correct slice leader
  record_stat( me.ip,leader_ip,ONEHOP_JOIN_LOOKUP,1);
  bool ok = doRPC (leader_ip, &OneHop::join_handler, &ja, jr, 
      TIMEOUT(me.ip,leader_ip));
  if (ok) record_stat(leader_ip,me.ip,ONEHOP_JOIN_LOOKUP,jr->table.size(),1);

  if (!alive()) {
    delete jr;
    return;
  }
  bool tmpok = false;
  if (ok && !jr->is_join_complete) {
    DEBUG(1) << now()<< ":" << ip() << "," << printID(id()) << " the leader " << leader_ip << " is still in the join process\n"; 
    ok = false;
    tmpok = true;
  }

  if (ok) {
    //Anjali: Fix below -- will cause n pain later, check timestamp before ignoring
    //if (jr->exists) return;
    if (jr->is_slice_leader) {
      //found correct slice leader or there is no leader, should have initial routing table
      for (uint i=0; i < jr->table.size(); i++) {
	DEBUG_MSG(jr->table[i],"join",la);
        loctable->add_node(jr->table[i]);
      }
      _join_complete = true;
      num_nodes++;
      joins++;
      LogEntry *e = New LogEntry(me, ALIVE, now());
      leader_log.push_back(*e);
      delete e;
      
      DEBUG(1) << now() << ":" << ip() <<"," << printID(id()) << ":Joined successfully" << endl;
      stabilize ((void *)0);   
    }
    else {
      //should contain updated slice leader info
      IDMap new_leader = jr->leader;
      DEBUG(1) << now() << ":" << ip() << "," <<  printID(id()) 
	<< " join_leader new " << new_leader.ip << "," << printID(id()) << endl;
      join_leader(new_leader, la, args);
    }
  }
  else {
    //the process failed somewhere, try to contact well known node again
    //if it could successfully inform, good, else schedule after some time 
    //bool dead_ok = false;
    //node is really dead, not just in the process of joining,
    //inform redirecting node
    if (!tmpok) {
      DEBUG(4) << now() << ":" << ip() << "," <<  printID(id()) 
	<< ": " << leader_ip << "failed repeating informing " << sender.ip 
	<< " that " << leader_ip << " has failed " << endl;

      //dead_ok = inform_dead (la, sender);
      test_inform_dead_args *aa = New test_inform_dead_args;
      aa->justdelete = false;
      aa->suspect = la;
      aa->informed = sender;
      delaycb(0,&OneHop::test_dead_inform,aa);
    }
    DEBUG(1) << now() << ":" << ip() << "," <<  printID(id()) 
      << "rescheduling join" << endl;
    delaycb (_retry_timer, &OneHop::join, (Args *)0);
  }
  delete jr;
}

void
OneHop::join_handler(join_leader_args *args, join_leader_ret *ret) 
{
  CHID node = args->key;
  //contacted looks into loctable and checks if it is the correct leader
  //or if there is no correct leader
  DEBUG(3) << now() << ":" << ip() << "," <<  printID(id()) 
    << ": Contacted by " << args->ip << " for join\n";
  if (!_join_complete) {
    ret->is_join_complete = false;
    return;
  }
  ret->is_join_complete = true;
  if (is_slice_leader(node, id()) || loctable->is_empty(node)) {
    ret->is_slice_leader = true;
    ret->table = loctable->get_all();
    IDMap newnode;
    newnode.id = node;
    newnode.ip = args->ip;
    DEBUG_MSG(newnode,"join_handler",newnode);
    loctable->add_node(newnode);
    LogEntry *e = New LogEntry(newnode, ALIVE, now());
    leader_log.push_back(*e);
    delete e;
    DEBUG(3) << now() << ":" << ip() << "," <<  printID(id()) << ":accepting " << newnode.ip << " for join\n";
  }
  else {
    ret->is_slice_leader = false;
    ret->leader = slice_leader(node);
    DEBUG(3) << now() << ":" << ip() << "," <<  printID(id()) 
      << ":not slice leader, not empty, so redirecting "<< args->ip << " to " << (ret->leader).ip <<endl;
  }
}

OneHop::~OneHop() {

  if (alive() && _join_complete) {
    DEBUG(1) << now() << ":" << ip() << "," <<  printID(id())
      <<":In slice " << slice(id()) << endl;
    if (is_slice_leader(me.id, me.id)) {
      DEBUG(1) << now() << ":" << ip() << "," <<  printID(id()) 
	<< ":Slice leader of slice " << slice(me.id) << endl;
    }
  }
  if (me.ip == 1) {
    if (OneHop::num_violations > 0) 
      printf("Number of violations of sending too big a message %u\n", OneHop::num_violations);

    Node::print_stats();

    printf("<-----STATS----->\n");
    sort(OneHop::sliceleader_bw_avg.begin(),OneHop::sliceleader_bw_avg.end());
    uint sz = OneHop::sliceleader_bw_avg.size();
    if (sz > 0)
      printf("SLICELEADER_BW:: 50p:%.2f 90p:%.2f 95p:%.2f 99p:%.2f sz:%u\n",
	  OneHop::sliceleader_bw_avg[(uint)(sz*0.5)],
	  OneHop::sliceleader_bw_avg[(uint)(sz*0.9)],
	  OneHop::sliceleader_bw_avg[(uint)(sz*0.95)],
	  OneHop::sliceleader_bw_avg[(uint)(sz*0.99)],sz);
    printf("<-----ENDSTATS----->\n");
  }

  delete loctable;
}
  
void
OneHop::stabilize(void* x)
{
  if (!alive()) return;
  if (!_join_complete) return;

  
  DEBUG(1) << now() << ":" << ip() << "," <<  printID(id())
    <<" stabilize last " << last_stabilize << " slice leader " 
    << slice(id()) << " unit leader " << unit(id()) << endl;
  last_stabilize = now();
  /*
  //if (now() < 2000000)
  //  countertime = now();
  if ((now() >= 2000000) && (now() < 2100000))
    //if (((now() - countertime) % 5000) == 0) {
    //  DEBUG(1) << ip() << "Stabilizing \n";
      if (is_slice_leader(me.id, me.id))
          DEBUG(1) << ip() << ":Slice leader of slice "<< slice(id()) << endl;
    //}
    */
      
  if (is_slice_leader(me.id, me.id) || is_unit_leader(me.id, me.id))
    leader_stabilize((void *)0);
 
  if (!alive()) return;

  notifyevent_args na, piggyback;
  na.sender = me; 
  piggyback.sender = me;
  piggyback.log.clear();
  bool ok = false;
  
  //I may have been slice leader some time, not any more. inform slice leader
  while ((!is_slice_leader(me.id, me.id)) && (leader_log.size () > 0)
      && (na.log.size() < MAX_IDS_MSG)) { //jy: limit size of one msg
    DEBUG_MSG(leader_log.front()._node,"stabilize non-leader leader->na log", me);
    na.log.push_back(leader_log.front());
    leader_log.pop_front();
  }
  while ((!is_slice_leader(me.id, me.id)) && (outer_log.size () > 0)
      && (na.log.size() < MAX_IDS_MSG)) { //jy: limit size of one msg
    DEBUG_MSG(outer_log.front()._node,"stabilize non-leader outer->na log", me);
    na.log.push_back(outer_log.front());
    outer_log.pop_front();
  }
  
  while ((low_to_high.size() > 0) 
      && (piggyback.log.size()< MAX_IDS_MSG)){ //jy: limit size of one msg
    DEBUG_MSG(low_to_high.front()._node,"stabilize non-leader low_to_high->piggyback log", me);
    piggyback.log.push_back(low_to_high.front());
    low_to_high.pop_front();
  }

  //successor ping
  while (!ok) {
    IDMap succ = loctable->succ(me.id+1);
   //ping the successor to see if it is alive and piggyback a log
    general_ret gr;
    piggyback.up = 1;
    bw data = 4*piggyback.log.size();

    ok = fd_xRPC(succ.ip,&OneHop::ping_handler,&piggyback,&gr, 
	ONEHOP_PING, piggyback.log.size());
    if (ok) record_stat(succ.ip,me.ip,ONEHOP_PING,0,1);

    DEBUG(4) << now() << ":" << ip() << "," << printID(me.id) 
      << " succ " << succ.ip << "," << printID(succ.id) << " ok? " << (ok?1:0) << endl;
    if (!alive()) return;
   
    //very ugly hack to fix accouting -- clean up 
    if (!((slice(succ.id) == slice(me.id)) && (unit(succ.id) == unit(me.id)))) {
        if (is_slice_leader(me.id, me.id)) {
          leader_bandwidth = leader_bandwidth - 20 - data;
          leader_messages--;
        }
        else { 
          bandwidth = bandwidth - 20 - data;
          messages--;
        }
    }
    

    if (!ok) {
      if (me.id != succ.id)
      loctable->del_node(succ.id);
      DEBUG(5) << now() << ":" << ip() << "," <<  printID(id())
	<<":PING! Informing " << slice_leader(me.id).ip <<" that successor "<< succ.ip << " is dead\n";
      LogEntry *e = New LogEntry(succ, DEAD, now());    
      na.log.push_back(*e);
      piggyback.log.push_back(*e);
      delete e;
    } 
    else if (!gr.has_joined) {
      if (me.id != succ.id)
        loctable->del_node(succ.id);
    }
    ok = ok && gr.has_joined;
    //if (ok && (!gr.correct))
   //   loctable->add_node(gr.act_neighbor);
  

  }
    
  //predecessor ping and piggyback
  piggyback.log.clear();
  while ((high_to_low.size() > 0) 
      && (piggyback.log.size() < MAX_IDS_MSG)) {//jy: limit max ids in one msg
    piggyback.log.push_back(high_to_low.front());
    high_to_low.pop_front();
  }
    
  ok = false;
  while (!ok) {
    IDMap pred = loctable->pred(me.id-1);
    DEBUG(4) << now() << ":" << ip() << "," <<  printID(id())
      << " pred " << pred.ip << "," << printID(pred.id) << " ok? " << (ok?1:0) << endl;
    general_ret gr;
    piggyback.up = 0; 
    bw data = 20+ 4*piggyback.log.size();

    ok = fd_xRPC(pred.ip, &OneHop::ping_handler, &piggyback, &gr, 
	ONEHOP_PING,piggyback.log.size());
    record_stat(pred.ip,me.ip,ONEHOP_PING, 0,1);

    if (!alive()) return;
    //very ugly hack to fix accouting -- change after deadline
    if (!((slice(pred.id) == slice(me.id)) && (unit(pred.id) == unit(me.id)))) {
        if (is_slice_leader(me.id, me.id)) {
          leader_bandwidth = leader_bandwidth - data;
          leader_messages--;
        }
        else {
          bandwidth = bandwidth - data;
          messages--;
        }
    }
    if (!ok) {
      if (me.id != pred.id)
      loctable->del_node(pred.id);
      DEBUG(5) << now() << ":" << ip() << "," <<  printID(id())
	<<":PING! Informing " << slice_leader(me.id).ip << " that predecessor "<< pred.ip << " is dead\n";
      LogEntry *e = New LogEntry(pred, DEAD, now());
      na.log.push_back(*e);
      piggyback.log.push_back(*e);
      delete e;
    }
    else if (!gr.has_joined) {
      if (me.id != pred.id)
      loctable->del_node(pred.id);
    }
    ok = ok && gr.has_joined;
    //if (ok && (!gr.correct))
    //  loctable->add_node(gr.act_neighbor);
  

  }
  
  //if either successor or predecessor has died, notify slice leader 
  ok = false;
  if (na.log.size() > 0) {
    while (!ok) {
      IDMap sliceleader = loctable->slice_leader(me.id);
      general_ret gr;

      DEBUG(1) << now() << ":" << ip() << "," << printID(id())
	<< " stabilize "<< " notifyevent log sz " <<  na.log.size() << "to sliceleader "<<sliceleader.ip << endl;
      ok = fd_xRPC(sliceleader.ip, &OneHop::notifyevent_handler,&na,&gr, 
	  ONEHOP_NOTIFY,na.log.size());
      if (ok) record_stat(sliceleader.ip,me.ip,ONEHOP_NOTIFY,0,1);

      if (!alive()) return;

      if (!ok) {
        LogEntry *e = New LogEntry(sliceleader, DEAD, now());
        na.log.push_back(*e);
        if (me.id != sliceleader.id)
        loctable->del_node(sliceleader.id);
        DEBUG(5) << now() << ":" << ip() <<"," << printID(id())
	  <<":PING! Informing " << slice_leader(me.id).ip << " that old slice leader "<< sliceleader.ip << " is dead\n";
        delete e;
      } 
      else if (!gr.has_joined) {
      if (me.id != sliceleader.id)
        loctable->del_node(sliceleader.id);
      }
      ok = ok && gr.has_joined;
      if (ok) { 
        if (gr.act_sliceleader.ip) {
	  DEBUG_MSG(gr.act_sliceleader,"stabilize",sliceleader);
          loctable->add_node(gr.act_sliceleader);
	}
      }
    }
  }

  delaycb(_stab_timer, &OneHop::stabilize, (void *)0);
}

void
OneHop::leader_stabilize(void *x) 
{
  if (_leaderstab_running) return;
  _leaderstab_running = true;
  if (!alive()) return;
  if (!_join_complete) return;

  /*
     if (is_slice_leader(me.id, me.id)) {
     DEBUG(1) << ip() << ":Slice size = " << slice_size << endl;
     DEBUG(1) << ip() << ":Unit size = " << loctable->unit_size << endl;
     DEBUG(1) << ip() << ":My Id = " << id() << endl;
     DEBUG(1) << ip() << "I am succ of (slice mid) " << slice(me.id)*slice_size + slice_size/2 << endl;
     DEBUG(1) << ip() << "I should also be succ of (unit mid) " << slice(me.id)*slice_size + unit(me.id)*(loctable->unit_size) + (loctable->unit_size)/2 << endl;
     DEBUG(1) << ip() << "Code claims unit succ is " << unit_leader(me.id).ip << " with id " << unit_leader(me.id).id << endl;
     DEBUG(1) << ip() << "My unit is " << unit(me.id) << " and claimed succ's unit is " << unit(unit_leader(me.id).id) << endl;
     assert(is_unit_leader(me.id, me.id));
     }
     */

  notifyevent_args send_unit;
  send_unit.sender = me;
  while ((inner_log.size() > 0) 
    && (send_unit.log.size() < MAX_IDS_MSG)) {
    send_unit.log.push_back(inner_log.front());
    inner_log.pop_front();
  }

  bool ok = false;
  if (send_unit.log.size() > 0) {
    while (!ok) {
      IDMap succ = loctable->succ(me.id+1);
      //ping the successor to see if it is alive and piggyback a log
      general_ret gr;
      bw data = 20 + 4*send_unit.log.size();

      ok = fd_xRPC(succ.ip, &OneHop::notify_rec_handler, &send_unit, &gr, 
	  ONEHOP_LEADER_STAB, send_unit.log.size());
      if (ok) record_stat(succ.ip,me.ip,ONEHOP_LEADER_STAB,0,1);

      if (!alive()) return;
      //very ugly hack to fix accouting -- change after deadline
      if (!((slice(succ.id) == slice(me.id)) && (unit(succ.id) == unit(me.id)))) {
        if (is_slice_leader(me.id, me.id)) {
          leader_bandwidth = leader_bandwidth - data;
          leader_messages--;
        }
        else {
          bandwidth = bandwidth - data;
          messages--;
        }
      }
      if (!ok) {
        LogEntry *e = New LogEntry(succ, DEAD, now());
        leader_log.push_back(*e);
        send_unit.log.push_back(*e);
        delete e;

        if (me.id != succ.id)
          loctable->del_node(succ.id);
      }
      if (ok && !gr.has_joined) {
        DEBUG(5) << now() << ":" << ip() << "," << printID(id())
	  << "Sending to an incompletely joined node\n";
        if (me.id != succ.id)
          loctable->del_node(succ.id);

      }
    }
    ok = false;
    while (!ok) {
      IDMap pred = loctable->pred(me.id-1);
      //ping the pred to see if it is alive and piggyback a log
      general_ret gr;
      bw data = 20+ 4*send_unit.log.size();

      ok = fd_xRPC(pred.ip, &OneHop::notify_rec_handler, &send_unit, &gr, 
	  ONEHOP_LEADER_STAB, send_unit.log.size());
      if (ok) record_stat(pred.ip,me.ip,ONEHOP_LEADER_STAB,0,1);

      if (!alive()) return;
      //very ugly hack to fix accouting -- change after deadline
      if (!((slice(pred.id) == slice(me.id)) && (unit(pred.id) == unit(me.id)))) {
        if (is_slice_leader(me.id, me.id)) {
          leader_bandwidth = leader_bandwidth - data;
          leader_messages--;
        }
        else {
          bandwidth = bandwidth - data;
          messages--;
        }
      }
      if (!ok) {
        LogEntry *e = New LogEntry(pred, DEAD, now());
        leader_log.push_back(*e);
        send_unit.log.push_back(*e);
        delete e;
        if (me.id != pred.id)
          loctable->del_node(pred.id);
      }
      if (ok && !gr.has_joined) {
        DEBUG(5) << now() << ":" << ip() << "," << printID(id())
	  << "Sending to an incompletely joined node\n";
        if (me.id != pred.id)
          loctable->del_node(pred.id);
      }
    }

    /*
       vector<IDMap> all_nodes = loctable->get_all();
       for (uint i=0; i < all_nodes.size(); i++) {
       if (!alive()) {
       DEBUG(1) << ip() << ":Unit leader: received events but died before sending them! Yikes!\n";
       return;
       }
       ok = false;

       if ((slice(all_nodes[i].id) == slice(me.id)) && (unit(all_nodes[i].id) == unit(me.id))) {
       general_ret gr; 
       ok = xRPC(all_nodes[i].ip, &OneHop::notify_rec_handler, &send_unit, &gr);
       if (!ok) {
       LogEntry *e = new LogEntry(all_nodes[i], DEAD, now());
       leader_log.push_back(*e);
       send_unit.log.push_back(*e);
       loctable->del_node(all_nodes[i],true);
       }
       if (ok && !gr.has_joined) {
       DEBUG(1) << ip() << "Sending to an incompletely joined node\n";
       loctable->del_node(all_nodes[i],true);
       }
       }


       }*/

  }
  if (is_slice_leader(me.id, me.id) && (leader_log.size() + outer_log.size() >= 5)) {
    notifyevent_args send_in, send_out;
    send_in.sender = me;
    send_out.sender = me;
    while ((leader_log.size() > 0) 
	&& (send_in.log.size() < MAX_IDS_MSG)) {
      send_in.log.push_back(leader_log.front());
      send_out.log.push_back(leader_log.front());
      leader_log.pop_front();
    }
    while ((outer_log.size() > 0) 
	&& (send_in.log.size() < MAX_IDS_MSG)) {
      send_in.log.push_back(outer_log.front());
      outer_log.pop_front();
    }
    if (send_out.log.size() > 0) tot_interslice++;
    if (send_in.log.size() > 0) {
      tot_intraslice++;
      exp_intraslice += _u ;
    }

    vector<IDMap> all_nodes = loctable->get_all();
    if (send_in.log.size() > 0) {
      for (uint i=0; i < all_nodes.size(); i++) {
        if (!alive()) {
          DEBUG(3) << now() << ":" << ip() << "," << printID(id())
	    << "Received events but died before sending them! Yikes!\n";
          return;
        }

        //all logs are already incorporated, do not send to myself
        //send mesg to all unit leaders
        if ((slice(me.id) == slice(all_nodes[i].id)) && is_unit_leader(all_nodes[i].id, all_nodes[i].id)) {
          general_ret gr;

          ok = fd_xRPC(all_nodes[i].ip, &OneHop::notify_unit_leaders, &send_in, &gr, 
	      ONEHOP_LEADER_STAB, send_in.log.size());
	  if (ok) record_stat(all_nodes[i].ip,me.ip,ONEHOP_LEADER_STAB,0,1);

	  if (!alive()) return;
          if (ok) {
            if (gr.has_joined) {
              act_intraslice++;
              //DEBUG(2) << ip() << ":Sent intra-slice to " << all_nodes[i].id << " in unit " << unit(all_nodes[i].id) << endl;
            }
            if ((!gr.has_joined) && (me.id != all_nodes[i].id)) {
              loctable->del_node(all_nodes[i].id);
	      DEBUG(5) << now() << ":" << ip() << "," << printID(id())
		<< "Sending to an incompletely joined node -- failed\n";
            }
          }
          else {
            LogEntry *e = New LogEntry(all_nodes[i], DEAD, now());
            leader_log.push_back(*e);
            send_in.log.push_back(*e);
            send_out.log.push_back(*e);
            delete e;
            if (me.id != all_nodes[i].id)
              loctable->del_node(all_nodes[i].id);
          }
        }

        if (me.id != all_nodes[i].id) {
          if (send_out.log.size() > 0) {
            if ((is_slice_leader(slice(all_nodes[i].id)*slice_size, all_nodes[i].id))) {
              general_ret gr;
	      
              ok = fd_xRPC(all_nodes[i].ip, &OneHop::notify_other_leaders, &send_out, &gr, 
		  ONEHOP_LEADER_STAB, send_out.log.size());
	      if (ok) record_stat(all_nodes[i].ip,me.ip,ONEHOP_LEADER_STAB,0,1);

	      if (!alive()) return;
              if (ok) {
                act_interslice++;
                if (!gr.has_joined) {
                  if (me.id != all_nodes[i].id)
                    loctable->del_node(all_nodes[i].id);
		  DEBUG(5) << now() << ":" << ip() << "," << printID(id())
                  << "Sending to an incompletely joined node\n";
                }
              }
              if (!ok) {
                LogEntry *e = New LogEntry(all_nodes[i], DEAD, now());
                outer_log.push_back(*e);
                send_in.log.push_back(*e);
                delete e;
                if (me.id != all_nodes[i].id)
                  loctable->del_node(all_nodes[i].id);
              }
            }
          }
        }
      }
      for (int i=0; i < _u; i++) {
        CHID n = slice(me.id)*slice_size + i*loctable->unit_size;
        if (loctable->is_empty_unit (n)) 
          total_empty++;
      }
      total_count++; 

    }
  }
  _leaderstab_running = false;
}

void
OneHop::ping_handler(notifyevent_args *args, general_ret *ret) 
{

  ret->correct = true;
  if (_join_complete)
    ret->has_joined = true;
  else ret->has_joined = false;

  IDMap succ_node = loctable->succ(args->sender.id);
  if (succ_node.id != args->sender.id) {
      if (!alive()) return;
      DEBUG(5) << now() << ":" << ip() << "," << printID(id())
      << ":Found new node " << args->sender.ip << " via ping\n";
      DEBUG_MSG(args->sender,"ping_handler directly add sender",args->sender);
      loctable->add_node(args->sender);
      LogEntry *e = New LogEntry(args->sender, ALIVE, now());
      leader_log.push_back(*e);
      delete e;
  }

  if ((slice(args->sender.id) == slice(me.id)) && (unit(args->sender.id) == unit(me.id))) {
    for (uint i=0; i < args->log.size(); i++) {
      if (args->sender.id >= me.id) { 
	high_to_low.push_back(args->log[i]);
	sent_low = false;
      }
      else {
        low_to_high.push_back(args->log[i]);
        sent_high = false;
      }
    
    
      if (args->log[i]._state == DEAD) {
	if (args->log[i]._node.ip == ip()) {
	  DEBUG(3) << now() << ":" << ip() << "," << printID(id())
	  << ":Panic! People think I am dead, but I'm not\n";
	  //exit(-1);
	  LogEntry *e = New LogEntry(me, ALIVE, now());
	  leader_log.push_back(*e);
	  delete e;

	if (args->sender.id >= me.id)
	  high_to_low.pop_back();
	else low_to_high.pop_back();

	}
	else {
	  loctable->del_node(args->log[i]._node.id);
	}
      }
      else {
	DEBUG_MSG(args->log[i]._node, "ping_handler", args->sender);
	loctable->add_node(args->log[i]._node); 
      }
    }
  }
  ret->correct = true;
  //now that all logs have been absorbed, and I know latest info
  //check if I am really the right neighbor
  /*if (args->up == 1) { //I am supposed to be the successor
    //who succ
    IDMap succ_node = loctable->succ(args->sender.id + 1);
    if (succ_node.id != me.id) {//I am not the successor
      assert(ConsistentHash::betweenleftincl(args->sender.id, me.id, succ_node.id));
      ret->act_neighbor = succ_node;
      ret->correct = false;
    }
  }
  else {
    IDMap pred_node = loctable->pred(args->sender.id - 1);
    if (pred_node.id != me.id) {
      assert(ConsistentHash::betweenrightincl(me.id, args->sender.id, pred_node.id));
      ret->act_neighbor = pred_node;
      ret->correct = false;
    }
  }
  */
             
}
  
void
OneHop::notifyevent_handler(notifyevent_args *args, general_ret *ret)
{

  if (_join_complete)
    ret->has_joined = true;
  else ret->has_joined = false;

  bool ok = false;
  if (!alive()) {
    DEBUG(3) << now() << ":" << ip() << "," << printID(id())
    << "Received events but died before sending them! Yikes!\n";
    return;
  }

  
  bool me_leader = is_slice_leader(me.id, me.id);
  general_ret gr;
  ret->act_sliceleader.id = 0;
  ret->act_sliceleader.ip = 0;
  if (!me_leader) {
    //not slice leader, but still got message, forward to correct slice leader
    //must forward message to the correct slice leader
    DEBUG(1) << now() << ":" << ip() << "," << printID(id())
    << ":I am not slice leader of "<< slice(id()) << ", still I got this message!\n";
    while (!ok) {

      IDMap sliceleader = loctable->slice_leader(me.id);
      DEBUG(1) << now() << ":" << ip() << "," << printID(id())
      << ":Forwarding to " << sliceleader.ip << endl ;

      ok = fd_xRPC(sliceleader.ip, &OneHop::notifyevent_handler, args, &gr, 
	  ONEHOP_NOTIFY, args->log.size());
      if (ok) record_stat(sliceleader.ip,me.ip,ONEHOP_NOTIFY, 0,1);

      if (!alive()) return;
      if (!ok) {
        LogEntry *e = New LogEntry(sliceleader, DEAD, now());
        leader_log.push_back(*e);
        args->log.push_back(*e);
        if (me.id != sliceleader.id)
        loctable->del_node(sliceleader.id);
	DEBUG(5) << now() << ":" << ip() << "," << printID(id())
        <<":PING! Informing " << slice_leader(me.id).ip << " that old slice leader "<< sliceleader.ip << " is dead\n";
        delete e;
      }
      else if (!gr.has_joined) {
      if (me.id != sliceleader.id)
        loctable->del_node(sliceleader.id);
      }
      ok = ok && gr.has_joined;
      if (ok) { 
        ret->act_sliceleader = sliceleader;
        LogEntry *e = New LogEntry(sliceleader, ALIVE, now());
        leader_log.push_back(*e);
        delete e;
      }
    }   
  }
  for (uint i=0; i < args->log.size(); i++) {
    if (args->log[i]._state == DEAD) {
      if (!alive()) return;
      if (args->log[i]._node.ip == ip()) {
	DEBUG(5) << now() << ":" << ip() << "," << printID(id())
        << ":Panic! People think I am dead, but I'm not\n";
        LogEntry *e = New LogEntry(me, ALIVE, now());
        leader_log.push_back(*e);
        delete e;
      }
      else {
        if (!alive()) return;
        loctable->del_node(args->log[i]._node.id);
        if (me_leader || (is_slice_leader(me.id, me.id))) {
          LogEntry *e = New LogEntry(args->log[i]._node, DEAD, now());
          leader_log.push_back(*e);
          delete e;
        }
      }
    }
    else {
      if (!alive()) return;
      DEBUG_MSG(args->log[i]._node,"notifyevent_handler",args->sender);
      loctable->add_node(args->log[i]._node);
      if ((me_leader) || is_slice_leader(me.id, me.id)) {
        LogEntry *e = New LogEntry(args->log[i]._node, ALIVE, now());
        leader_log.push_back(*e);
        delete e;
      }
    }
  }

  IDMap succ_node = loctable->succ(args->sender.id);
  if (succ_node.id != args->sender.id) {
      if (!alive()) return;
      DEBUG(5) << now() << ":" << ip() << "," << printID(id())
      << ":Found New node " << args->sender.ip << " via notify event\n";
      DEBUG_MSG(args->sender,"notifyevent_handler directly add sender", args->sender);
      loctable->add_node(args->sender);
      LogEntry *e = New LogEntry(args->sender, ALIVE, now());
      leader_log.push_back(*e);
      delete e;
  }
}
  

void 
OneHop::notify_rec_handler(notifyevent_args *args, general_ret *ret)
{
  if (_join_complete)
    ret->has_joined = true;
  else ret->has_joined = false;
  if (!alive()) return;
 
  DEBUG_MSG(args->sender, "notify_rec_handler directly add sender", args->sender);
  loctable->add_node(args->sender);
 
    
  if (args->log.size() < 1)
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    <<":PANIC! Got empty log\n"; 

  for (uint i=0; i < args->log.size(); i++) {
    if (!alive()) return;
    if (args->sender.id >= me.id) { 
      high_to_low.push_back(args->log[i]);
      sent_low = false;
    }
    else {
      low_to_high.push_back(args->log[i]);
      sent_high = false;
    }

    if (args->log[i]._state == DEAD) {
      if (args->log[i]._node.ip == ip()) {
	DEBUG(5) << now() << ":" << ip() << "," << printID(id())
        << ":Panic! People think I am dead, but I'm not\n";
        //exit(-1);
        LogEntry *e = New LogEntry(me, ALIVE, now());
        leader_log.push_back(*e);
        delete e;
      if (args->sender.id >= me.id)
        high_to_low.pop_back();
      else low_to_high.pop_back();


      }
      else {
        loctable->del_node(args->log[i]._node.id);
      }
    }
    else {
      DEBUG_MSG(args->log[i]._node,"notify_rec_handler",args->sender);
      loctable->add_node(args->log[i]._node);
    }
  }
}

void
OneHop::notify_other_leaders(notifyevent_args *args, general_ret *ret) 
{
  if (_join_complete)
    ret->has_joined = true;
  else ret->has_joined = false;

  if (!alive()) return;
  IDMap succ_node = loctable->succ(args->sender.id);
  if (succ_node.id != args->sender.id) {
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    << ":Found new node " << args->sender.ip << " via slice leader ping\n";
    DEBUG_MSG(args->sender,"notify_other_leaders directly add sender", args->sender);
    loctable->add_node(args->sender);
    LogEntry *e = New LogEntry(args->sender, ALIVE, now());
    outer_log.push_back(*e);
    delete e;
  }

  if (args->log.size() < 1)
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    <<":PANIC! Got empty log\n"; 
  for (uint i=0; i < args->log.size(); i++) {
    outer_log.push_back(args->log[i]);
  }

  notifyevent_args send_in;
  send_in.sender = me;
  leader_stabilize((void *)0);
}

void
OneHop::notify_unit_leaders(notifyevent_args *args, general_ret *ret) 
{
  if (_join_complete)
    ret->has_joined = true;
  else ret->has_joined = false;
  
  if (!alive()) return;
  if (args->log.size() < 1)
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    <<":PANIC! Got empty log\n"; 

  IDMap succ_node = loctable->succ(args->sender.id);
  if (succ_node.id != args->sender.id) {
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    << ":Found new node " << args->sender.ip << " via same slice leader/unit leader ping\n";
    DEBUG_MSG(args->sender,"notify_unit_leader",args->sender);
    loctable->add_node(args->sender);
    LogEntry *e = New LogEntry(args->sender, ALIVE, now());
    leader_log.push_back(*e);
    args->log.push_back(*e);
    delete e;
  }
  if (!_join_complete) 
    DEBUG(5) << now() << ":" << ip() << "," << printID(id())
    << "Wow! Join not complete, still got this message\n";
  
  for (uint i=0; i < args->log.size(); i++) {
    if (args->log[i]._state == DEAD) {
      if (!alive()) return;
      if (args->log[i]._node.ip == ip()) {
	DEBUG(5) << now() << ":" << ip() << "," << printID(id())
        << ":Panic! People think I am dead, but I'm not\n";
        LogEntry *e = New LogEntry(me, ALIVE, now());
        leader_log.push_back(*e);
        inner_log.push_back(*e);
        delete e;
      }
      else {
        inner_log.push_back(args->log[i]);
        loctable->del_node(args->log[i]._node.id);
      }
    }
    else {
      inner_log.push_back(args->log[i]);
      DEBUG_MSG(args->log[i]._node,"notify_unit_leaders",args->sender);
      loctable->add_node(args->log[i]._node);
    }
  }
  leader_stabilize((void *)0);
}

void
OneHop::test_dead_inform(test_inform_dead_args *a)
{
  if (a->justdelete) {
    DEBUG(4) << now() << ":" << ip() << "," << printID(id())
      << " test_dead_inform from " << a->informed.ip << " about " 
      << a->suspect.ip << endl;
    loctable->del_node(a->suspect.id);
  }else{
    bool ok = fd_xRPC (a->suspect.ip, &OneHop::test_dead_handler, 
	(void *)NULL, (void *)NULL, ONEHOP_INFORMDEAD,0);
    if (ok) {
      record_stat(a->suspect.ip,me.ip,ONEHOP_INFORMDEAD,0);
      loctable->add_node(a->suspect);
      delete a;
      return;
    } else 
      loctable->del_node(a->suspect.id);

    inform_dead_args ia;
    ia.ip = a->suspect.ip;
    ia.key = a->suspect.id;
    ok = fd_xRPC(a->informed.ip,&OneHop::inform_dead_handler,
	&ia, (void *)NULL, ONEHOP_INFORMDEAD,1);
    if ((ok)&&(a->informed.ip!=me.ip)) record_stat(a->informed.ip,me.ip,ONEHOP_INFORMDEAD);
  }
  delete a;
}

void
OneHop::test_dead_handler(void *x, void *y)
{
  //do nothing
}

bool
OneHop::inform_dead (IDMap dead, IDMap recv) {
  IPAddress recv_ip = recv.ip;
  inform_dead_args ia;
  ia.ip = dead.ip;
  ia.key = dead.id;

  bool ok = fd_xRPC (recv_ip, &OneHop::inform_dead_handler, &ia, (void *)NULL, 
      ONEHOP_INFORMDEAD, 1);
  if (ok) record_stat(recv_ip,me.ip,ONEHOP_INFORMDEAD);
  return ok;
}

void 
OneHop::inform_dead_handler (inform_dead_args *ia, void *ir) 
{
  IDMap dead;
  dead.id = ia->key;
  dead.ip = ia->ip;
  LogEntry *e = New LogEntry(dead, DEAD, now());
  leader_log.push_back(*e);
  delete e;
  if (me.id != dead.id)
    loctable->del_node(dead.id);
}

OneHop::IDMap
OneHop::unit_leader(CHID node) {
  return loctable->unit_leader(node);
}


OneHop::IDMap
OneHop::slice_leader(CHID node) {
  return loctable->slice_leader(node);
}

bool 
OneHop::is_slice_leader(CHID node, CHID explead) {
  return loctable->is_slice_leader(node, explead);
}

bool
OneHop::is_unit_leader(CHID node, CHID explead) {
  return loctable->is_unit_leader(node, explead);
}

ConsistentHash::CHID 
OneHop::slice (CHID node) { 
  return loctable->slice(node);
}

ConsistentHash::CHID 
OneHop::unit (CHID node) { 
  return loctable->unit(node);
}

OneHop::IDMap
OneHopLocTable::succ(ConsistentHash::CHID id) {
  assert (ring.repok ());
  ohidmapwrap *ptr = ring.closestsucc(id);
  assert(ptr);
  IDMap n;
  n.ip = ptr->ip;
  n.id = ptr->id;
  return n;
}

OneHop::IDMap
OneHopLocTable::pred(ConsistentHash::CHID id) {
  assert (ring.repok ());
  ohidmapwrap *ptr = ring.closestpred(id);
  assert(ptr);
  OneHop::IDMap n;
  n.ip = ptr->ip;
  n.id = ptr->id;
  return n;
}

bool
OneHopLocTable::is_slice_leader (CHID node, CHID explead) {
  return (explead == slice_leader(node).id);
}

bool
OneHopLocTable::is_unit_leader (CHID node, CHID explead) {
  return (explead == unit_leader(node).id);
}
/*
bool OneHopLocTable::is_unit_leader(CHID node, CHID explead) {
  vector<IDMap> uls = unit_leaders(node);

  //DEBUG(2) << "Number of unit leaders found = " << uls.size() << endl;
  for (uint i=0; i < uls.size(); i++) {
    if (explead == uls[i].id)
      return true;
  }
  return false;
}
*/
        
OneHop::IDMap 
OneHopLocTable::slice_leader(CHID node) {
  IDMap pot_succ;
  pot_succ.id = 0;
  pot_succ.ip = 0;
  if (is_empty(node)) return pot_succ;
  pot_succ = succ(slice(node)*slice_size + slice_size/2);
  if ((pot_succ.ip == 0) || (slice(node) != slice(pot_succ.id))){
    pot_succ = pred(slice(node)*slice_size + slice_size/2);
  }
  return pot_succ;
}

OneHop::IDMap 
OneHopLocTable::unit_leader(CHID node) {
  IDMap pot_succ;
  pot_succ.id = 0;
  pot_succ.ip = 0;
  if (is_empty_unit(node)) 
    return pot_succ;
  pot_succ = succ(slice(node)*slice_size + unit(node)*unit_size + unit_size/2);
  if ((pot_succ.ip == 0) || (slice(node) != slice(pot_succ.id)) || unit(node) != unit(pot_succ.id)){
    pot_succ = pred(slice(node)*slice_size + unit(node)*unit_size + unit_size/2);
  }
  return pot_succ;
}

vector<OneHop::IDMap>
OneHopLocTable::unit_leaders(CHID node) {
  vector<IDMap> ret_vec;
  IDMap pot_succ;
  pot_succ.id = 0;
  pot_succ.ip = 0;
  CHID beg = slice(node)*slice_size;
  CHID def;
  for (int i=0; i < _u; i++) {
    CHID n = beg + i*unit_size;
    if (!is_empty_unit (node)) {
      def = n + unit_size/2;
      pot_succ = succ(def);
      if ((pot_succ.ip == 0) || (unit(def) != unit(pot_succ.id)) || (slice(def) != slice(pot_succ.id))){
        pot_succ = pred(def);
      }

      if(! ((pot_succ.ip == 0) || (unit(def) != unit(pot_succ.id)) || (slice(def) != slice(pot_succ.id))))
        ret_vec.push_back(pot_succ);
    }
    else 
      DEBUG(2) << "Empty unit\n";
  }
  return ret_vec;
}

bool
OneHopLocTable::is_empty(CHID node) {
  //find beginning of slice
  CHID beg = slice(node)*slice_size;
  CHID end = beg+slice_size;
  IDMap pot_succ = succ(beg);
  if ((pot_succ.ip == 0) || !ConsistentHash::betweenleftincl(beg,end,pot_succ.id)) {
    return true;
  }
  else return false;
}

bool
OneHopLocTable::is_empty_unit(CHID node) {
  //find beginning of unit
  CHID beg = slice(node)*slice_size + unit(node)*unit_size;
  CHID end = beg+unit_size;
  IDMap pot_succ = succ(beg);
  if ((pot_succ.ip == 0) || !ConsistentHash::betweenleftincl(beg,end,pot_succ.id)) {
    return true;
  }
  else return false;
}


void
OneHopLocTable::print() {
  ohidmapwrap *i = ring.first();
  while (i) {
    DEBUG(3) << i->ip << ":" << i->id << endl;
    i = ring.next(i);
  }
}

void
OneHopLocTable::del_all() 
{
  ohidmapwrap *next;
  ohidmapwrap *cur;
  for (cur = ring.first(); cur; cur = next) {
    next = ring.next(cur);
    ring.remove(cur->id);
    bzero(cur, sizeof(*cur));
    delete cur;
  }
  assert (ring.repok ());
}

void
OneHopLocTable::add_node(IDMap n)
{
  ohidmapwrap *elm = ring.search(n.id);
  if (!elm) {
    elm = New ohidmapwrap(n.ip,n.id);
    ring.insert(elm);
  }
}

void
OneHopLocTable::del_node(CHID id)
{
  ohidmapwrap *elm = ring.remove(id);
  if (elm) 
    delete elm;
}

vector<OneHop::IDMap>
OneHopLocTable::get_all()
{
  vector<OneHop::IDMap> v;
  v.clear();

  ohidmapwrap *currp; 
  currp = ring.first();
  OneHop::IDMap n;
  while (currp) { 
    n.ip = currp->ip;
    n.id = currp->id;
    v.push_back(n);
    currp = ring.next(currp);
  }
  return v;
}
void
OneHop::initstate()
{
  vector<IDMap> ids = OneHopObserver::Instance(NULL)->get_sorted_nodes();
  for (uint i = 0; i < ids.size(); i++) {
    loctable->add_node(ids[i]);
  }
}
string
OneHop::printID(CHID id)
{
  char buf[128];
  sprintf(buf,"%qx ",id);
  return string(buf);
}


/*
void
OneHop::reschedule_stabilizer(void *x)
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
  delaycb(_stabtimer, &OneHop::reschedule_stabilizer, (void *)0);
}
*/




