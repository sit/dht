#ifndef _DEBRUIN_H_
#define _DEBRUIN_H_

// N.G. de Bruijn (http://www.win.tue.nl/~wsdwnb/)

class debruijn : public stabilizable {
  ptr<vnode> myvnode;
  ptr<locationtable> locations;
  chordID myID;  
  chordID mydoubleID;

 public:
  debruijn (ptr<vnode> v, ptr<locationtable> locs, chordID myID);

  void print ();
  // void stats ();

  void stabilize();
  chordID closestsucc (const chordID &x);
  chordID closestpred (const chordID &x);

  bool isstable () { return true; };
  bool continous_stabilizing () { return true; };
  bool backoff_stabilizing () { return false; };
  void do_backoff () { return; };
  void do_continuous () { stabilize (); };
  void finddoublesucc_cb (chordID s, route path, chordstat status);
};

#endif /* _DEBRUIN_H_ */
