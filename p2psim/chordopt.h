#ifndef __CHORDFINGER_H
#define __CHORDFINGER_H

#include "chord.h"

/* in addition to the basic ring traversing chord
   ChordOpt implements successor list
   and finger table */

class ChordOpt: public Chord {
  public:
    ChordOpt(Node *n) : Chord(n) {};
    ~ChordOpt() {};

    struct get_successor_list_args {
      Chord::CHID m; //number of successors wanted
    };
    
    struct get_successor_list_ret {
      vector<IDMap> v;
    };

    //RPC handlers
    void get_successor_list_handler(get_successor_list_args *, get_successor_list_ret *);

  protected:
    void fix_successor_list();
    void fix_fingers();
    void stabilize();
};

#endif
