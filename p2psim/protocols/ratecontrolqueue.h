#ifndef RATECONTROL_QUEUE__
#define RATECONTROL_QUEUE__

#include "p2psim/node.h"
#include <algorithm>
#include <queue>

#define QDEBUG(x) if(p2psim_verbose>=(x)) cout << _node->header() << " qsz " << _qq.size() << " quota " << _quota << " last " << _last_update
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

    RateControlQueue(Node *, double, int, void (*fn)(void *));

    template<class BT, class AT, class RT>
    bool do_rpc(IPAddress dst, void (BT::* fn)(AT *, RT *), int (BT::* cb)(bool b, AT *, RT *), 
	AT *args, RT *ret, uint p, uint type, int sz, int rsz, Time to) {

      
      if (!_running) {
	_running = true;
	_node->delaycb(_delay_interval, &RateControlQueue::delay_send, (void*)0, this);
      }
      if (!_start_time) 
	_start_time = now();


      QueueThunk<BT,AT,RT> *t = new QueueThunk<BT,AT,RT>;
      t->_target = dynamic_cast<BT*>(Network::Instance()->getnode(dst));
      t->_src = dynamic_cast<BT*>(_node);
      t->_fn = fn;
      t->_cb = cb;
      t->_args = args;
      t->_ret = ret;

      q_elm *qe = new q_elm;
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

      if ((p>0 && _qq.size() > 2) || ((_quota + more - sz - rsz < (_burst/2)) && (p >= DROPABLE_PRIORITY))) {
	QDEBUG(4) << " drop to dst " << qe->_dst << " priority " << p << endl;
	if (args)
	  delete args;
	if (ret) 
	  delete ret;
	qe->_killme(qe->_t);
	delete qe;
	return false;
      }

      if ((_qq.size() == 0) && ((_quota + more - sz - rsz) > _burst)) {
	int oldq = _quota;
	_quota = _quota + more - qe->_sz - qe->_rsz;
	QDEBUG(5) << " add " << more << " sub " << qe->_sz << " sub " << qe->_rsz << " to oldq " << oldq << endl;
	_last_update = now();
	QDEBUG(5) << " immediately sendrpc (" << qe->_type << "," << p 
	  << ") to " << dst << endl;
	_node->delaycb(0,&RateControlQueue::send_one_rpc, (void *)qe, this);
	return true;
      } else {
	_qq.push(qe);
	/*
	if (_qq.size() > 2)
	  fprintf(stderr,"shit!!! %llu %u,%u sz %u quota %d \n", now(), _node->first_ip(), _node->ip(), _qq.size(),_quota);
	  */
	QDEBUG(5) << " delay sendrpc (" << qe->_type << "," << p << ") to " 
	  << dst << endl;
	return false;
      }

    }
    void delay_send(void *x);
    void send_one_rpc(void *x);
    void stop_queue();
    bool empty() { return (_qq.size() == 0 && (((now()-_last_update) *_rate) +  _quota) > 0) ;}
    int quota() { return _quota;}
    uint size() { return _qq.size();}
    bool critical() { if (_qq.size() > 0 || _quota < (_burst/2)) return true; return false;}


   protected :
    void (*_empty_cb)(void *);
    Node *_node;
    int _burst;
    uint _delay_interval;
    priority_queue<q_elm*, vector<q_elm*>, less<q_elm*> > _qq;
    double _rate;
    bool _running;
    int _quota;
    Time _last_update;
    Time _start_time;
    unsigned long long _total_bytes;
    
};


#endif
