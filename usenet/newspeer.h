#ifndef _NEWSPEER_H_
#define _NEWSPEER_H_

/*
 * Information about each peer is kept in a peerinfo object,
 * which gates whether or not an article is desired by a
 * given peer.
 *
 * Separately, we maintain a table of open connections to
 * peers.  This is mostly so that if the peer configuration
 * changes, we can continue to process out-bound feeds to
 * that peer until completion.
 */

class peerinfo {
  vec<str> patterns;

public:
  const str hostname;
  const u_int16_t port;
  const str peerkey;

  peerinfo (str h, u_int16_t p);
  ~peerinfo ();

  bool add_pattern (str p);
  bool desired (str group);
};

struct aios;
struct timecb_t;

enum peerstate {
  HELLO_WAIT,
  MODE_CHANGE,
  DHT_CHECK,
  FEEDING
};

class newspeer {
  int s;
  ptr<aios> aio;
  timecb_t *conncb;
  peerstate state;

  // what remote host desires
  bool dhtok;
  bool streamok;

  // message ids of articles to send to peer
  vec<str> outq;

  u_int64_t fedoutbytes_;

  void feed_connected (int t, int s);
  void process_line (const str data, int err);

  void send_article (str id);
  void flush_queue ();

  void reset ();
  
 public:
  const str hostname;
  const u_int16_t port;

  newspeer (str h, u_int16_t p);
  ~newspeer ();

  void start_feed (int t = 60);
  void queue_article (str id);

  static u_int64_t totalfedbytes ();
  u_int64_t fedoutbytes (bool reset = false);

};

#endif /* _NEWSPEER_H_ */
