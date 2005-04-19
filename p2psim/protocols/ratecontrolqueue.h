#ifndef RATECONTROL_QUEUE__
#define RATECONTROL_QUEUE__

#include "protocols/accordion.h"
#include <algorithm>
#include <queue>
#include <iostream>

#define QDEBUG(x) if(p2psim_verbose>=(x)) cout << _node->header() << " qsz " << _qq.size() << " q " << _quota << " last " << _last_update << " total " << _total_bytes
#define DROPABLE_PRIORITY 3

struct q_elm {
  public:
    uint _priority;
    int _sz; //pkt size
    int _rsz; //return pkt size
    uint _type;
    Time _timeout;
    IPAddress _dst;
    void (*_fn)(void *);
    int (*_cb)(bool b, void *);
    void (*_killme)(void *);
    void *_t;
};

template<class BT, class AT, class RT>
class QueueThunk {
  public:
    BT *_target;
    BT *_src;
    void (BT::*_fn)(AT *, RT *);
    int (BT::*_cb)(bool,AT *, RT *);
    AT *_args;
    RT *_ret;
    static void handle(void *xa) {
      QueueThunk *t = (QueueThunk *)xa;
      (t->_target->*(t->_fn))(t->_args,t->_ret);
      t->_target->notifyObservers();
    }
    static void killme(void *xa) {
      delete (QueueThunk *)xa;
    }
    static int callback(bool b, void *xa) {
      QueueThunk *t = (QueueThunk *)xa;
      return (t->_src->*(t->_cb))(b, t->_args,t->_ret);
    }
};


class RateControlQueue {

  public:

    RateControlQueue(Accordion *, double, int, Time, void (*fn)(void *));

    template<class BT, class AT, class RT>
    bool do_rpc(IPAddress dst, void (BT::* fn)(AT *, RT *), int (BT::* cb)(bool b, AT *, RT *), 
	AT *args, RT *ret, uint p, uint type, int sz, int rsz, Time to) {

      if (!_running) {
	_running = true;
	if (_fixed_stab)
	  _node->delaycb(_fixed_stab, &RateControlQueue::detect_empty, (void*)0, this);
	else
	  _node->delaycb(_delay_interval, &RateControlQueue::detect_empty, (void*)0, this);
      }
      if (!_start_time) 
	_start_time = now();

      QueueThunk<BT,AT,RT> *t = New QueueThunk<BT,AT,RT>;
      t->_target = dynamic_cast<BT*>(Network::Instance()->getnode(dst));
      t->_src = dynamic_cast<BT*>(_node);
      t->_fn = fn;
      t->_cb = cb;
      t->_args = args;
      t->_ret = ret;

      q_elm *qe = New q_elm;
      qe->_sz = sz;
      qe->_rsz = rsz;
      qe->_dst = dst;
      qe->_timeout = to;
      qe->_priority = p;
      qe->_t = t;
      qe->_fn = QueueThunk<BT,AT,RT>::handle;
      qe->_cb = QueueThunk<BT,AT,RT>::callback;
      qe->_killme = QueueThunk<BT,AT,RT>::killme;
      qe->_type = type;

      int more = 0;
      if (_last_update > 0) 
	more = (int) ((now() - _last_update)*_rate);
      _quota += more;
      _last_update = now();

      if ((!_fixed_stab) && _node->ip()!=dst && (_quota - sz - rsz < (_burst/2)) && p >= DROPABLE_PRIORITY) {
	QDEBUG(4) << " drop to dst " << qe->_dst << " priority " << p << endl;
	if (args)
	  delete args;
	if (ret) 
	  delete ret;
	qe->_killme(qe->_t);
	delete qe;
	return false;
      }

      if (_node->ip()!=dst) {
	_quota -= (qe->_sz + qe->_rsz);
	if (_quota < _burst)
	  _quota = _burst;
      }

      QDEBUG(5) << " sub " << qe->_sz << " sub " << qe->_rsz << endl;
      _node->delaycb(0,&RateControlQueue::send_one_rpc, (void *)qe, this);
      return true;
    }

    void detect_empty(void *x);
    void send_one_rpc(void *x);
    void stop_queue();
    bool empty() { 
      /*
      if (_last_out_update) {
	_outquota += ((int) ((now() - _last_out_update)*_rate));
	_outquota -= (_node->total_outbytes()-_last_out);
	_last_out = _node->total_outbytes();
	_last_out_update= now();
	if (_outquota < 0)
	  return false;
      }else{
	_last_out_update= now();
	_last_out = _node->total_outbytes();
      }
      */
      return (_qq.size() == 0 && (_quota) > (-_burst/2));
    }
    int quota() { return _quota;}
    uint total_bytes() { return _total_bytes;}
    uint size() { return _qq.size();}

    bool critical() { 
      if (_fixed_stab) {
	return false;
      }else if (_qq.size() > 0 || _quota < (_burst/2)) {
	return true; 
      }else 
	return false;
    }
    bool very_critical();


   protected :
    void (*_empty_cb)(void *);
    Accordion *_node;
    int _burst;

    uint _last_out;
    Time _last_out_update;
    int _outquota;

    uint _delay_interval;
    priority_queue<q_elm*, vector<q_elm*>, less<q_elm*> > _qq;
    double _rate;
    bool _running;
    int _quota;
    Time _last_update;
    Time _start_time;
    unsigned long long _total_bytes;
    Time _fixed_stab;
    
};


#endif
