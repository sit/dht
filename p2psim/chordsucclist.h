#ifndef __CHORDSUCCLIST_H
#define __CHORDSUCCLIST_H

#include "chord.h"

#define SUCC_NUM 1  //successor list contains SUCC_NUM elements
/* in addition to the basic ring traversing chord
   ChordSuccList implements successor list*/

class ChordSuccList: public Chord {
  public:
    ChordSuccList(Node *n) : Chord(n) {};
    ~ChordSuccList() {};

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
    void stabilize();
};

#endif
