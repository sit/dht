/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu).
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

#include "parse.h"
#include "network.h"
#include "args.h"
#include "protocols/protocolfactory.h"
#include "p2psim/threadmanager.h"
#include <iostream>
using namespace std;

string Node::_protocol = ""; 
Args Node::_args;
Time Node::_collect_stat_time = 0;
bool Node::_collect_stat = false;
bool Node::_replace_on_death = true;
// static stat data structs:
vector<unsigned long> Node::_bw_stats;
vector<uint> Node::_bw_counts;
vector<Time> Node::_correct_lookups;
vector<Time> Node::_incorrect_lookups;
vector<Time> Node::_failed_lookups;
vector<double> Node::_correct_stretch;
vector<double> Node::_incorrect_stretch;
vector<double> Node::_failed_stretch;
vector<uint> Node::_correct_hops;
vector<uint> Node::_incorrect_hops;
vector<uint> Node::_failed_hops;
vector<double> Node::_num_timeouts;
vector<Time> Node::_time_timeouts;
vector<uint> Node::_num_joins;
vector<Time> Node::_last_joins;
vector<Time> Node::_time_sessions;
//vector<double> Node::_per_node_avg;
vector<double> Node::_per_node_in;
vector<double> Node::_per_node_out;
uint Node::totalin = 0;
uint Node::totalout = 0;
vector< vector<double> > Node::_special_node_in;
vector< vector<double> > Node::_special_node_out;
double Node::maxinburstrate = 0.0;
double Node::maxoutburstrate = 0.0;


Node::Node(IPAddress i) : _queue_len(0), _ip(i), _alive(true), _token(1) 
{
  _track_conncomp_timer = _args.nget<uint>("track_conncomp_timer",0,10);
  if (ip()==1) {
    Node::_special_node_in.resize(3);
    Node::_special_node_out.resize(3);
    for (uint i = 0; i < 3; i++) {
      Node::_special_node_in[i].clear();
      Node::_special_node_out[i].clear();
    }
    if  (_track_conncomp_timer > 0)
      delaycb(_track_conncomp_timer, &Node::calculate_conncomp, (void*)NULL);
  }
  _num_joins_pos = -1;
  _prev_ip = 0;
  _first_ip = _ip;

  join_time = 0;
  //node_live_bytes = 0;
  node_live_inbytes = 0;
  node_live_outbytes = 0;
  node_lastburst_live_outbytes = 0;
  node_lastburst_live_inbytes = 0;
  node_last_inburstime = node_last_outburstime = join_time;

}

Node::~Node()
{
}

Node *
Node::getpeer(IPAddress a)
{
  return Network::Instance()->getnode(a);
}

unsigned
Node::rcvRPC(RPCSet *hset, bool &ok)
{
  int na = hset->size() + 1;
  Alt *a = (Alt *) malloc(sizeof(Alt) * na); // might be big, take off stack!
  Packet *p;
  unsigned *index2token = (unsigned*) malloc(sizeof(unsigned) * hset->size());

  int i = 0;
  for(RPCSet::const_iterator j = hset->begin(); j != hset->end(); j++) {
    assert(_rpcmap[*j]);
    a[i].c = _rpcmap[*j]->channel();
    a[i].v = &p;
    a[i].op = CHANRCV;
    index2token[i] = *j;
    i++;
  }
  a[i].op = CHANEND;

  if((i = alt(a)) < 0) {
    cerr << "interrupted" << endl;
    assert(false);
  }
  assert(i < (int) hset->size());

  unsigned token = index2token[i];
  assert(token);
  hset->erase(token);
  _deleteRPC(token);
  if( !p ) {
    // if there's no packet, then this must be a wakeup 
    // from a non-network source (like a condition variable) 
    ok = true;
  } else {
    ok = p->ok();
    delete p;
  }
  free(a);
  free(index2token);
  return token;
}


void
Node::_deleteRPC(unsigned token)
{
  assert(_rpcmap[token]);
  delete _rpcmap[token];
  _rpcmap.remove(token);
}


void
Node::parse(char *filename)
{
  ifstream in(filename);
  if(!in) {
    cerr << "no such file " << filename << endl;
    taskexitall(0);
  }

  string line;

  hash_map<string, Args> xmap;
  while(getline(in,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // read protocol string
    _protocol = words[0];
    words.erase(words.begin());

    // if it has no arguments, you still need to register the prototype
    // if(!words.size())
      // xmap[protocol];

    // this is a variable assignment
    while(words.size()) {
      vector<string> xargs = split(words[0], "=");
      words.erase(words.begin());
      if (_args.find (xargs[0]) == _args.end())
	_args.insert(make_pair(xargs[0], xargs[1]));
    }

    break;
  }

  in.close();

}

bool
Node::collect_stat()
{
  if (_collect_stat) 
    return true;
  if (now() >= _collect_stat_time) {
    _collect_stat = true;
    return true;
  }else
    return false;
}

#define BURSTTIME 100000
void 
Node::record_in_bytes(uint b) { 
  node_live_inbytes += b;
  if ((now()-node_last_inburstime) > BURSTTIME) {
    double burstrate = (double)(1000*(node_live_inbytes-node_lastburst_live_inbytes))/(double)((now()-node_last_inburstime));
    if (burstrate > Node::maxinburstrate)
      Node::maxinburstrate = burstrate;
    node_last_inburstime = now();
    node_lastburst_live_inbytes = node_live_inbytes;
  }
}

void 
Node::record_out_bytes(uint b) { 
  node_live_outbytes += b;
  if ((now()-node_last_outburstime) > BURSTTIME) {
    double burstrate = (double)(1000*(node_live_outbytes-node_lastburst_live_outbytes))/(double)((now()-node_last_outburstime));
    if (burstrate > Node::maxoutburstrate) 
      Node::maxoutburstrate = burstrate;
    node_last_outburstime = now();
    node_lastburst_live_outbytes = node_live_outbytes;
  }
}

void
Node::record_inout_bw_stat(IPAddress src, IPAddress dst, uint num_ids, uint num_else)
{
  if (src == dst) 
    return;

  Node *n = Network::Instance()->getnode(src);
  if (n && n->alive())
    n->record_out_bytes(20 + 4*num_ids + num_else);

  n = Network::Instance()->getnode(dst);
  if (n && n->alive())
    n->record_in_bytes(20 + 4*num_ids + num_else);
}

void 
Node::record_bw_stat(stat_type type, uint num_ids, uint num_else)
{
  if( !collect_stat() ) {
    return;
  }

  while( _bw_stats.size() <= type ) {
    _bw_stats.push_back(0);
    _bw_counts.push_back(0);
  }
  _bw_stats[type] += 20 + 4*num_ids + num_else;
  _bw_counts[type]++;
}

void 
Node::record_lookup_stat(IPAddress src, IPAddress dst, Time interval, 
			 bool complete, bool correct, uint num_hops, 
			 uint num_timeouts, Time time_timeouts)
{

  if( !collect_stat() ) {
    return;
  }

  // get stretch as well
  double stretch;
  if( complete && correct ) {
    Time rtt = 2*Network::Instance()->gettopology()->latency( src, dst );
    if( rtt > 0 && interval > 0 ) { 
      stretch = ((double) interval)/((double) rtt);
    } else {
      if( interval == 0 ) {
	stretch = 1;
      } else {
	stretch = interval; // is this reasonable?
      }
    }
  } else {
    // the stretch should be the interval divided by the median ping time in
    // the topology.  fake for now.  ***TODO***!!!
    stretch = ((double) interval)/100.0;
  }

  // Stretch can be < 1 for recursive direct-reply routing 
  // assert( stretch >= 1.0 );

  if( complete && correct ) {
    _correct_lookups.push_back( interval );
    _correct_stretch.push_back( stretch );
    _correct_hops.push_back( num_hops );
  } else if( !complete ) {
    _failed_lookups.push_back( interval );
    _failed_stretch.push_back( stretch );
    _failed_hops.push_back( num_hops );
  } else {
    _incorrect_lookups.push_back( interval );
    _incorrect_stretch.push_back( stretch );
    _incorrect_hops.push_back( num_hops );
  }

  // timeout stuff
  _num_timeouts.push_back( num_timeouts );
  _time_timeouts.push_back( time_timeouts );

}

void
Node::check_num_joins_pos()
{
  if( _num_joins_pos == -1 ) {
    _num_joins_pos = _num_joins.size();
    _num_joins.push_back(0);
    // initialize people starting from the stattime
    _last_joins.push_back(_collect_stat_time);
  }
}

void
Node::record_join()
{

  // do this first to make sure state is initialized for this node
  join_time = now();
  //node_live_bytes = 0;
  node_live_inbytes = 0;
  node_live_outbytes = 0;
  node_lastburst_live_outbytes = 0;
  node_lastburst_live_inbytes = 0;
  node_last_inburstime = node_last_outburstime = join_time;

  check_num_joins_pos();
  if( !collect_stat() ) {
    return;
  }

    assert( _num_joins[_num_joins_pos] == 0 || !_last_joins[_num_joins_pos] );
  _num_joins[_num_joins_pos]++;
  _last_joins[_num_joins_pos] = now();

}

void
Node::record_crash()
{

  if( !collect_stat() ) {
    return;
  }

  if (join_time > 0) {
    Time duration = now() - join_time;
    //this is a hack, don't screw the distribution with nodes whose lifetime
    //is too short
    if (duration >= 600000) { //old value is 180000  
      //_per_node_avg.push_back((double)1000.0*node_live_bytes/(double)duration);
      _per_node_out.push_back((double)1000.0*node_live_outbytes/(double)duration);
      _per_node_in.push_back((double)1000.0*node_live_inbytes/(double)duration);
      if ((_special) && (_special < 4)){
	Node::_special_node_out[_special-1].push_back((double)1000.0*node_live_outbytes/(double)duration);
	Node::_special_node_in[_special-1].push_back((double)1000.0*node_live_inbytes/(double)duration);
	ADEBUG(4) << "special crashed IN: " << node_live_inbytes << " OUT: " 
	  << node_live_outbytes << " DURATION: " << duration << " AVG_IN: " 
	  << ((double)1000.0*node_live_outbytes/(double)duration)
	  << " AVG_OUT: " << ((double)1000.0*node_live_inbytes/(double)duration) << endl;
      }
    }
  }
  check_num_joins_pos();
  assert( _num_joins_pos >= 0 );
  Time session = now() - _last_joins[_num_joins_pos];
  _last_joins[_num_joins_pos] = 0;
  _time_sessions.push_back( session );
}

void
Node::print_dist_stats(vector<double> v)
{
  sort(v.begin(),v.end());
  uint sz = v.size();
  double allavg = 0.0;
  for (uint i = 0; i < sz; i++)
    allavg += v[i];
  if (sz > 0) {
    printf("1p:%.3f 5p:%.3f 10p:%.3f 50p:%.3f 90p:%.3f 95p:%.3f 99p:%.3f 100p:%.3f avg:%.3f\n", 
      v[(uint)(sz*0.01)], v[(uint)(sz*0.05)],v[(uint)(sz*0.1)],
      v[sz/2], v[(uint)(sz*0.9)], 
      v[(uint)(sz*0.95)], v[(uint)(sz*0.99)], 
      v[sz-1], allavg/sz);
  }else
    printf("\n");
}

void
Node::print_stats()
{

  if( !collect_stat() ) {
    cout << "No stats were collected by time " << now() 
	 << "; collect_stat_time=" << _collect_stat_time << endl;
    return;
  }

  // add up the time everyone has been alive (including the last session)
  Time live_time = 0;
  for( uint i = 0; i < _time_sessions.size(); i++ ) {
    live_time += _time_sessions[i];
  }
  for( uint i = 0; i < _last_joins.size(); i++ ) {
    // if this person joined at 0 and never failed, or was otherwise
    // alive at the end
    //    if( _num_joins[i] == 0 && _last_joins[i] == 0 ) {
      // that means this one never died
    //  live_time += now() - _collect_stat_time;
    //  printf( "yup\n" );
    //    } else 
    if( _last_joins[i] != 0 ) {
      live_time += now() - _last_joins[i];
    }
  }

  cout << "\n<-----STATS----->" << endl;

  // first print out bw stats
  unsigned long total = 0;
  cout << "BW_PER_TYPE:: ";
  for( uint i = 0; i < _bw_stats.size(); i++ ) {
    cout << i << ":" << _bw_stats[i] << " ";
    total += _bw_stats[i];
  }
  cout << endl;
  double total_time = ((double) (now() - _collect_stat_time))/1000.0;
  // already accounts for nodes in live_time . . .
  double live_time_s = ((double) live_time)/1000.0;
  uint num_nodes = Network::Instance()->size();
  double overall_bw = ((double) total)/(total_time*((double) num_nodes));
  double live_bw = ((double) total)/live_time_s; 
  printf( "BW_TOTALS:: time(s):%.3f live_time(s/node):%.3f nodes:%d overall_bw(bytes/node/s):%.3f live_bw(bytes/node/s):%.3f\n", 
	  total_time, live_time_s/((double) num_nodes), num_nodes, 
	  overall_bw, live_bw );

  /*print out b/w distribution
  sort(_per_node_avg.begin(),_per_node_avg.end());
  uint sz = _per_node_avg.size();
  double allavg = 0;
  for (uint i = 0; i < sz; i++) 
    allavg += _per_node_avg[i];

  if (sz > 0) {
    printf("BW_PERNODE:: 50p:%.3f 90p:%.3f 95p:%.3f 99p:%.3f 100p:%.3f avg:%.3f\n", 
      _per_node_avg[sz/2], _per_node_avg[(uint)(sz*0.9)], 
      _per_node_avg[(uint)(sz*0.95)], _per_node_avg[(uint)(sz*0.99)], 
      _per_node_avg[sz-1], allavg/sz);
  }
  */

  //print out b/w distribution of out b/w
  cout <<  "BW_PERNODE_IN:: ";
  if (_per_node_in.size())
    print_dist_stats(_per_node_in);

  cout << "BW_PERNODE:: ";
  if (_per_node_out.size())
    print_dist_stats(_per_node_out);

  for (uint i = 0; i < 3; i++) {
    printf("BW_SPE%uNODE_IN:: ",i+1);
    print_dist_stats(_special_node_in[i]);
  }

  for (uint i = 0; i < 3; i++) {
    printf("BW_SPE%uNODE:: ",i+1);
    print_dist_stats(_special_node_out[i]);
  }

  // then do lookup stats
  double total_lookups = _correct_lookups.size() + _incorrect_lookups.size() +
    _failed_lookups.size();
  printf( "LOOKUP_RATES:: success:%.3f incorrect:%.3f failed:%.3f\n",
	  ((double)_correct_lookups.size())/total_lookups,
	  ((double)_incorrect_lookups.size())/total_lookups,
	  ((double)_failed_lookups.size())/total_lookups );
  cout << "CORRECT_LOOKUPS:: ";
  print_lookup_stat_helper( _correct_lookups, _correct_stretch, 
			    _correct_hops );
  cout << "INCORRECT_LOOKUPS:: ";
  print_lookup_stat_helper( _incorrect_lookups, _incorrect_stretch, 
			    _incorrect_hops );
  cout << "FAILED_LOOKUPS:: ";
  print_lookup_stat_helper( _failed_lookups, _failed_stretch, _failed_hops );
  // now overall stats (put them all in one container)
  for( uint i = 0; i < _incorrect_lookups.size(); i++ ) {
    _correct_lookups.push_back( _incorrect_lookups[i] );
    _correct_stretch.push_back( _incorrect_stretch[i] );
    _correct_hops.push_back( _incorrect_hops[i] );
  }
  for( uint i = 0; i < _failed_lookups.size(); i++ ) {
    _correct_lookups.push_back( _failed_lookups[i] );
    _correct_stretch.push_back( _failed_stretch[i] );
    _correct_hops.push_back( _failed_hops[i] );
  }
  cout << "OVERALL_LOOKUPS:: ";
  print_lookup_stat_helper( _correct_lookups, _correct_stretch, 
			    _correct_hops );

  cout << "TIMEOUTS_PER_LOOKUP:: ";
  print_lookup_stat_helper( _time_timeouts, _num_timeouts, 
			    _correct_hops /* this isn't used */,
			    true );

  cout << "WORST_BURST:: in:" << maxinburstrate << " out:" << maxoutburstrate << endl;
  cout << "<-----ENDSTATS----->\n" << endl;

}

void 
Node::print_lookup_stat_helper( vector<Time> times, vector<double> stretch,
				vector<uint> hops, bool timeouts )
{

  assert( times.size() == stretch.size() );

  // sort first, ask questions later
  sort( times.begin(), times.end() );
  sort( stretch.begin(), stretch.end() );
  sort( hops.begin(), hops.end() );

  Time time_med, time_10, time_90;
  double stretch_med, stretch_10, stretch_90;
  uint hops_med, hops_10, hops_90;
  if( times.size() == 0 ) {
    time_med = 0;
    time_10 = 0;
    time_90 = 0;
    stretch_med = 0;
    stretch_10 = 0;
    stretch_90 = 0;
    hops_med = 0;
    hops_10 = 0;
    hops_90 = 0;
  } else {
    if( times.size() % 2 == 0 ) {
      time_med = (times[times.size()/2] + times[times.size()/2-1])/2;
      stretch_med = (stretch[times.size()/2] + stretch[times.size()/2-1])/2;
      hops_med = (hops[times.size()/2] + hops[times.size()/2-1])/2;
    } else {
      time_med = times[(times.size()-1)/2];
      stretch_med = stretch[(times.size()-1)/2];
      hops_med = hops[(times.size()-1)/2];
    }
    time_10 = times[(uint) (times.size()*.1)];
    stretch_10 = stretch[(uint) (times.size()*.1)];
    hops_10 = hops[(uint) (times.size()*.1)];
    time_90 = times[(uint) (times.size()*.9)];
    stretch_90 = stretch[(uint) (times.size()*.9)];
    hops_90 = hops[(uint) (times.size()*.9)];
  }

  // also need the means
  Time time_total = 0;
  double stretch_total = 0;
  uint hops_total = 0;
  for( uint i = 0; i < times.size(); i++ ) {
    time_total += times[i];
    stretch_total += stretch[i];
    hops_total += hops[i];
  }
  double time_mean, stretch_mean, hops_mean;
  if( times.size() == 0 ) {
    time_mean = 0;
    stretch_mean = 0;
    hops_mean = 0;
  } else {
    time_mean = ((double) time_total)/((double) times.size());
    stretch_mean = ((double) stretch_total)/((double) times.size());
    hops_mean = ((double) hops_total)/((double) times.size());
  }

  if( timeouts ) {
    printf( "time_timeout_10th:%llu time_timeout_mean:%.3f time_timeout_median:%llu time_timeout_90th:%llu ",
	    time_10, time_mean, time_med, time_90 );
    
    printf( "num_timeout_10th:%.3f num_timeout_mean:%.3f num_timeout_median:%.3f num_timeout_90th:%.3f\n",
	    stretch_10, stretch_mean, stretch_med, stretch_90 );

  } else {

    printf( "lookup_10th:%llu lookup_mean:%.3f lookup_median:%llu lookup_90th:%llu ",
	    time_10, time_mean, time_med, time_90 );
    
    printf( "stretch_10th:%.3f stretch_mean:%.3f stretch_median:%.3f stretch_90th:%.3f ",
	    stretch_10, stretch_mean, stretch_med, stretch_90 );

    printf( "hops_10th:%u hops_mean:%.3f hops_median:%u hops_90th:%u ",
	    hops_10, hops_mean, hops_med, hops_90 );
    
    cout << " numlookups:" << times.size() << endl;

  }

}

//void function
void
Node::add_edge(int *matrix, int sz)
{
  return;
}
//network health monitor
void
Node::calculate_conncomp(void *)
{
  if (Node::collect_stat()) {
    const set<Node*> *l = Network::Instance()->getallnodes();
    uint sz = l->size();
    int *curr, *old, *tmp;

    curr = (int *)malloc(sizeof(int) * sz * sz);
    old = (int *)malloc(sizeof(int) * sz * sz);

    assert(old && curr);
    for (uint i = 0; i < sz * sz; i++) {
      curr[i] = 99999;
      old[i] = 99999;
    }
    int alive = 0;
    for (set<Node*>::iterator i = l->begin(); i != l->end(); ++l) {
      if(!(*i)->alive())
        continue;
      old[((*i)->first_ip()-1)*sz + (*i)->first_ip()-1] = 0;
      (*i)->add_edge(old,sz);
      alive++;
    }

    for (uint k = 0; k < sz; k++) {
      for (uint i = 0; i < sz; i++) {
	for (uint j = 0; j < sz; j++) {
	  if (old[i * sz + j] <= old[i * sz + k] + old[k * sz + j]) 
	    curr[i * sz + j] = old[i * sz + j];
	  else
	    curr[i * sz + j] = old[i * sz + k] + old[k * sz + j];
	}
      }
      tmp = old;
      old = curr;
      curr = tmp;
    }

    vector<uint> *path = new vector<uint>;
    u_int allp = 0;
    u_int failed = 0;
    path->clear();
    for (uint i = 0; i < sz; i++) {
      for (uint j = 0; j < sz; j++) {
	if (old[i*sz + i] == 0 && old[j*sz + j] == 0) {
	  if (old[i*sz + j] < 99999) {
	    path->push_back(old[i*sz + j]);
	    allp += old[i*sz + j];
	  }else{
	    failed++;
	  }
	}
      }
    }
    sort(path->begin(),path->end());
    assert(path->size()>0);
    printf("%llu alive %d avg %.2f 10-p %u 50-p %u 90-p %u longest %u failed %.3f\n", now(), alive, 
	(double)allp/(double)path->size(),(*path)[(u_int)(0.1*path->size())], 
	(*path)[(u_int)(0.5*path->size())], 
	(*path)[(u_int)(0.9*path->size())], 
	(*path)[path->size()-1], (float) (failed/(failed+path->size())));

    delete path;
    free(old);
    free(curr);
  }
  delaycb(_track_conncomp_timer, &Node::calculate_conncomp, (void*)NULL);
}
// Called by NetEvent::execute() to deliver a packet to a Node,
// after Network processing (i.e. delays and failures).
void
Node::packet_handler(Packet *p)
{
  if(p->reply()){
    // RPC reply, give to waiting thread.
    send(p->channel(), &p);
  } else {
    // RPC request, start a handler thread.
    ThreadManager::Instance()->create(Node::Receive, p);
  }
}

//
// Send off a request packet asking Node::Receive to
// call fn(args), wait for reply.
// Return value indicates whether we received a reply,
// i.e. absence of time-out.
//
bool
Node::_doRPC(IPAddress dst, void (*fn)(void *), void *args, Time timeout)
{
  return _doRPC_receive(_doRPC_send(dst, fn, 0, args, timeout));
}


RPCHandle*
Node::_doRPC_send(IPAddress dst, void (*fn)(void *), void (*killme)(void *), void *args, Time timeout)
{
  Packet *p = New Packet;
  p->_fn = fn;
  p->_killme = killme;
  p->_args = args;
  p->_src = ip();
  p->_dst = dst;
  p->_timeout = timeout;
  Node *n = getpeer (ip());
  p->_queue_delay = n->queue_delay ();

  // where to send the reply, buffered for single reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 1);

  Network::Instance()->send(p);

  return New RPCHandle(c, p);
}


bool
Node::_doRPC_receive(RPCHandle *rpch)
{
  Packet *reply = (Packet *) recvp(rpch->channel());
  bool ok = reply->_ok;
  delete reply;
  delete rpch;
  return ok;
}

//
// Node::got_packet() invokes Receive() when an RPC request arrives.
// The reply goes back directly to the appropriate channel.
//
void
Node::Receive(void *px)
{
  Packet *p = (Packet *) px;
  assert(Network::Instance()->getnode(p->dst()));

  // make reply
  Packet *reply = New Packet;
  reply->_c = p->_c;
  reply->_src = p->_dst;
  reply->_dst = p->_src;
  reply->_timeout = p->_timeout;
  Node *s = Network::Instance()->getnode(reply->src());
  reply->_queue_delay = s->queue_delay ();

  if (Network::Instance()->alive(p->dst())) {
      //      && Network::Instance()->gettopology()->latency(p->_src, p->_dst, p->reply()) != 100000 ) {
    (p->_fn)(p->_args);
    reply->_ok = true;
  } else {
    reply->_ok = false;  // XXX delete reply for timeout?
  }

  // send it back, potentially with a latency punishment for when this node was
  // dead.
  Network::Instance()->send(reply);

  // ...and we're done
  taskexit(0);
}

string
Node::header()
{
  char buf[128];
  sprintf(buf,"%llu %s(%u,%u,%qx) ", now(), proto_name().c_str(), _first_ip, 
      _ip,_id);
  return string(buf);
}

IPAddress
Node::set_alive(bool a)
{ 
  if(!a && _replace_on_death) {
    _prev_ip = _ip;
  } else if(a && _replace_on_death && _prev_ip) {
    _ip = Network::Instance()->unused_ip();
    Network::Instance()->map_ip(_first_ip, _ip);
    assert(!Network::Instance()->getnode(_first_ip)->alive());
  }

  _alive = a;
  return _ip;
}

#include "bighashmap.cc"
