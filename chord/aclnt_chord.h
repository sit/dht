#ifndef ACLNT_CHORD_H
#define ACLNT_CHORD_H

class rpccb_chord : public rpccb_msgbuf {
 protected:
  rpccb_chord (ref<aclnt> c, xdrsuio &x, aclnt_cb _cb, callback<bool>::ptr u_tmo,
	       void *out, xdrproc_t outproc, const sockaddr *d, 
	       ptr<bool> del, ptr<void> in, int procno) :
    rpccb_msgbuf (c, x, wrap (this, &rpccb_chord::finish_cb, _cb, del), out, 
		  outproc, d), 
    utmo (u_tmo), deleted (del), c (c), procno (procno), in (in), s(*d) {};

  int rexmits;
  timecb_t *tmo;
  long sec, nsec;
  callback<bool>::ptr utmo;
  ptr<bool> deleted;
  ptr<aclnt> c;
  int procno;
  ptr<void> in;
  const sockaddr s;

private:
  void timeout_cb (ptr<bool> del);
  void finish_cb (aclnt_cb cb, ptr<bool> del, clnt_stat err);

 public:
  void reset_tmo ();
  void timeout () { finish (RPC_TIMEDOUT); };
  static rpccb_chord *alloc (ptr<aclnt> c,
			     aclnt_cb cb,
			     callback<bool>::ptr u_tmo,
			     ptr<void> in,
			     void *out,
			     int procno,
			     struct sockaddr *dest);
  void send (long sec, long nsec);
};

#endif
