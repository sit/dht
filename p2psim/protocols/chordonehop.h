#ifndef __CHORDONEHOP_H
#define __CHORDONEHOP_H

#include "chord.h"

class ChordOneHop: public Chord {
  public:
    ChordOneHop(Node *n, Args& a);
    ~ChordOneHop();
    string proto_name() { return "ChordOneHop";}

    struct deladd_args {
      IDMap n;
    };

    void join(Args *);
    void stabilize();
    bool stabilized(vector<ConsistentHash::CHID> lid) {return false;};
    void reschedule_stabilizer(void *x);
    void init_state(vector<IDMap> ids);

    void del_handler(deladd_args *, void *);
    void add_handler(deladd_args *, void *);
    void getloctable_handler(void *, get_successor_list_ret *);

    virtual vector<IDMap> find_successors(CHID key, uint m, bool is_lookup);
};
#endif //__CHORDONEHOP_H

