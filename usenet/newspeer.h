#ifndef _NEWSPEER_H_
#define _NEWSPEER_H_

struct aios;
struct timecb_t;

enum peerstate {
  HELLO_WAIT,
  MODE_CHANGE,
  DHT_CHECK,
  FEEDING
};

class newspeer {
  str hostname;
  u_int16_t port;

  int s;
  ptr<aios> aio;
  timecb_t *conncb;
  peerstate state;

  // what remote host desires
  vec<str> patterns;
  bool dhtok;
  bool streamok;

  // message ids of articles to send to peer
  vec<str> outq;

  void feed_connected (int t, int s);
  void process_line (const str data, int err);

  void send_article (str id);
  void flush_queue ();

  void reset ();
  
 public:
  static ptr<newspeer> alloc (str h, u_int16_t p);
  newspeer (str h, u_int16_t p);
  ~newspeer ();

  bool add_pattern (str p);
  bool desired (str group);
  
  void start_feed (int t = 60);
  void queue_article (str id);
};

#endif /* _NEWSPEER_H_ */
