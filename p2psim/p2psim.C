#include "p2psim.h"
#include "eventqueue.h"

Time
now() {
  return EventQueue::Instance()->time();
}
