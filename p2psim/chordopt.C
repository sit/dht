#include  "chordopt.h"
#include <iostream>
using namespace std;

#define SUCC_NUM 1  //successor list contains SUCC_NUM elements

void
ChordOpt::get_successor_list_handler(get_successor_list_args *args, get_successor_list_ret *ret)
{
  IDMap succ = loctable->succ(SUCC_NUM);
  ret->v.clear();
  ret->v.push_back(succ);
}

void
ChordOpt::fix_successor_list()
{
  IDMap succ = loctable->succ(1);
  if (succ.ip == 0) return;

  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  doRPC(succ.ip, &ChordOpt::get_successor_list_handler, &gsa, &gsr);

  for (int i = 0; i < (gsr.v).size(); i++) {
    loctable->add_node(gsr.v[i]);
  }
}

void
ChordOpt::fix_fingers()
{
}

void
ChordOpt::stabilize()
{/*
  super.stabilize();
fix_successor_list();
  build_fingers();
  */
}
