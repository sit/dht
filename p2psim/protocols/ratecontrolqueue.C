#include <dmalloc.h>
#include "chord.h"
#include "ratecontrolqueue.h"


bool operator< (const q_elm& a, const q_elm& b) { return (b._priority < a._priority); }

RateControlQueue::RateControlQueue(Node *n, double rate, double bigrate, int burst, void (*fn)(void *))
{
  _node = n;
  _rate = rate/1000.0;
  _big_rate = bigrate/1000.0;
  _delay_interval = (uint)(40/_rate);
  _burst = -1*burst;
  _running = false;
  _empty_cb = fn;
  _quota = 0;
  _big_quota = 0;
  _last_update = 0;
  _big_last_update = 0;
  _running = false;
  _total_bytes = 0;
  _start_time = 0;
}

void
RateControlQueue::detect_empty(void *x)
{
  if (!_node->alive()) {
    stop_queue();
    return;
  }

  assert(_running);

  if (!_start_time)
    _start_time = now();

  int oldq = _quota;
  int more = 0;
  if (_last_update > 0) {
    _quota += (int) ((now() - _last_update)*_rate);
  }
  _last_update = now();
  QDEBUG(5) << " adding " << more << " to " << oldq << endl;

  if (empty()) {
    QDEBUG(5) << " detect_empty detect empty queue " << endl;
    _empty_cb(_node);
  }

  if (_quota < -40) {
    Time ttt = (Time)((-0.5*_quota)/_rate);
    QDEBUG(5) << " reschedule detect_empty ttt " << ttt << endl;
    _node->delaycb(ttt, &RateControlQueue::detect_empty, (void *)0, this);
  } else {
    _node->delaycb(_delay_interval, &RateControlQueue::detect_empty, (void *)0, this);
  }
}

void
RateControlQueue::send_one_rpc(void *x)
{
  q_elm *qe = (q_elm *)x;

  QDEBUG(5) << " sending rpc to " << qe->_dst << endl;
  _node->record_bw_stat(qe->_type,0,qe->_sz-PKT_OVERHEAD);
  _node->record_inout_bw_stat(qe->_dst,0,qe->_sz-PKT_OVERHEAD);
  _total_bytes += qe->_sz;
  bool b = _node->_doRPC(qe->_dst, qe->_fn, qe->_t, qe->_timeout);

  int sz;
  if (qe->_cb) 
    sz = (qe->_cb)(b, qe->_t);
  else
    sz = PKT_OVERHEAD;

  int oldq = _quota;
  _quota += (qe->_rsz-sz);
  _total_bytes += sz;
  if (sz > 0)
    _node->record_bw_stat(qe->_type,0,sz-PKT_OVERHEAD);
  assert((_total_bytes + _quota) < (now()-_start_time)*_rate);
  QDEBUG(5) << " (send_rpc) adding " << (qe->_rsz-sz) << " from old value " << oldq << endl;
  qe->_killme(qe->_t);
  delete qe;
}

void
RateControlQueue::stop_queue()
{
  q_elm *qe;
  while (_qq.size() > 0) {
    qe = _qq.top();
    _qq.pop();
    delete qe;
  }
  _running = false;
  QDEBUG(5) << " stopped total bytes " << _total_bytes << " live time " << (now()-_start_time) << 
    " avg bytes " << (double)(_total_bytes*1000)/(now()-_start_time) << endl;
  _quota = 0;
  _big_quota = 0;
  _total_bytes = 0;
  _start_time = 0;
  _last_update = 0;
  _big_last_update = 0;
}
