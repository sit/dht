#include "threadmanager.h"
#include <lib9.h>
#include <thread.h>

ThreadManager *ThreadManager::_instance = 0;


ThreadManager*
ThreadManager::Instance()
{
  if(!_instance)
    _instance = new ThreadManager();
  return _instance;
}

ThreadManager::ThreadManager()
{
}

ThreadManager::~ThreadManager()
{
}

int
ThreadManager::create(Threaded *t, void (*fn)(void*), void *args, int ss)
{
  int tid = ::threadcreate(fn, args, ss);
  return tid;
}
