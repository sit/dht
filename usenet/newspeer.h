#ifndef _NEWSPEER_H_
#define _NEWSPEER_H_

struct aios;

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
  peerstate state;

  // what remote host desires
  vec<str> patterns;
  bool dhtok;
  bool streamok;

  // message ids of articles to send to peer
  vec<str> outq;

  void feed_connected (int t, int s);
  void process_line (const str data, int err);

  void flush_queue ();
  
 public:
  static ptr<newspeer> alloc (str h, u_int16_t p);
  newspeer (str h, u_int16_t p);
  ~newspeer ();

  bool add_pattern (str p);
  bool desired (str group);
  
  void start_feed (int t);
  void queue_article (str id);
};

#endif /* _NEWSPEER_H_ */
