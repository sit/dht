#include "chord.h"
#include "ratecontrolqueue.h"


bool operator< (const q_elm& a, const q_elm& b) { return (b._priority < a._priority); }

RateControlQueue::RateControlQueue(Node *n, double rate, int burst, void (*fn)(void *))
{
  _node = n;
  _rate = rate/1000.0;
  _delay_interval = (uint)(200/_rate);
  _burst = -1*burst;
  _running = false;
  _empty_cb = fn;
  _quota = 0;
  _last_update = 0;
  _running = false;
}

void
RateControlQueue::delay_send(void *x)
{
  if (!_node->alive())
    return;

  assert(_running);
  _quota += (int)((now()-_last_update)*_rate);

  _last_update = now();

  if (!_node->alive()) {
    stop_queue();
    return;
  }

  q_elm *qe;
  while ((_quota > _burst) && (_qq.size() > 0)){
    qe = _qq.top();
    _qq.pop();
    _quota -= qe->_sz;
    if (_qq.size() > 10)
      QDEBUG(4) << " delay_send sendrpc (" << qe->_type << "," << qe->_priority
      << ") to " << qe->_dst << endl;;
    _node->delaycb(0,&RateControlQueue::send_one_rpc, (void *)qe, this);
  }
  if (empty()) {
    QDEBUG(4) << " delay_send detect empty queue " << endl;
    _empty_cb(_node);
  }
  _node->delaycb(_delay_interval, &RateControlQueue::delay_send, (void *)0, this);
}

void
RateControlQueue::send_one_rpc(void *x)
{
  q_elm *qe = (q_elm *)x;

  _node->record_bw_stat(qe->_type,0,qe->_sz);
  bool b = _node->_doRPC(qe->_dst, qe->_fn, qe->_t, qe->_timeout);

  if (qe->_cb) {
    int sz = (qe->_cb)(b, qe->_t);
    _quota -= sz;
    _node->record_bw_stat(qe->_type,0,sz);
  }else{
    _quota -= PKT_OVERHEAD;
  }
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
  _quota = 0;
}
