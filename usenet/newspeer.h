struct aios;
class newspeer {
  str hostname;
  u_int16_t port;

  // sinaddr?
  int s;
  ptr<aios> aio;

  // what remote host desires
  vec<str> patterns;

  void feed_connected (int t, int s);
  
 public:
  static ptr<newspeer> alloc (str h, u_int16_t p);
  newspeer (str h, u_int16_t p);
  ~newspeer ();

  bool add_pattern (str p);
  bool desired (str group);
  
  void start_feed (int t);
  void send_article ();
};
