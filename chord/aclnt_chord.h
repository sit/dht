#ifndef ACLNT_CHORD_H
#define ACLNT_CHORD_H

class rpccb_chord : public rpccb_msgbuf {
 protected:
  rpccb_chord (ref<aclnt> c, xdrsuio &x, aclnt_cb _cb, cbv u_tmo,
	       void *out, xdrproc_t outproc, const sockaddr *d, 
	       ptr<bool> del) :
    rpccb_msgbuf (c, x, wrap (this, &rpccb_chord::finish_cb, _cb, del), out, 
		  outproc, d), 
    utmo (u_tmo), deleted (del)  {};

  int rexmits;
  timecb_t *tmo;
  long sec, nsec;
  cbv utmo;
  ptr<bool> deleted;

 public:
  void timeout () { finish (RPC_TIMEDOUT); };
  static rpccb_chord *alloc (ptr<aclnt> c,
			     aclnt_cb cb,
			     cbv u_tmo,
			     ptr<void> in,
			     void *out,
			     int procno,
			     struct sockaddr *dest);
  void timeout_cb (ptr<bool> del);
  void finish_cb (aclnt_cb cb, ptr<bool> del, clnt_stat err);
  void send (long sec, long nsec);
};

#endif
