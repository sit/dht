#ifndef __THREADMANAGER_H
#define __THREADMANAGER_H

#include "threaded.h"
#include <lib9.h>
#include <thread.h>
#include <map>
using namespace std;

class ThreadManager {
public:
  static ThreadManager* Instance();
  Threaded *deleteme_get(int);

  int create(Threaded*, void (*)(void*), void*, int ss = mainstacksize);

private:
  ThreadManager();
  ~ThreadManager();

  static ThreadManager* _instance;
  map<int, Threaded*> _threadmap;
};


#endif // __THREADMANAGER_H
