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

  int create(void (*)(void*), void*, int ss = mainstacksize);

private:
  ThreadManager();
  ~ThreadManager();

  static ThreadManager* _instance;
};


#endif // __THREADMANAGER_H
