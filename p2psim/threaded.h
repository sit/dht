#ifndef __THREADED_H
#define __THREADED_H

#include <lib9.h>
#include <thread.h>
#include <map>
using namespace std;

class Threaded {
public:
  Threaded();
  virtual ~Threaded();
  static void Run(void*);
  Channel *exitchan() { return _exitchan; }

protected:
  void thread();
  void thread(Threaded*);
  Channel *_exitchan;
  int _thread;

private:
  virtual void run() = 0;
};

#endif // __THREADED_H
