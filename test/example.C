#include "testmaster.h"

testslave slaves[] = {
  { "127.0.0.1", 3344 }, // id: 0
  { "127.0.0.1", 3345 }, // id: 1
  { "", 0 }
};
char buf[10] = "xxxxxxxxx";


void 
done(int *n, int ok)
{
  if(!ok)
    fatal << "error\n";

  if(!--*n)
    exit(0);
}



// compares fetched data with stored data
void
retrieve_cb(ptr<dhash_block> b)
{
  warn << "retrieved : " << b->data << "\n";
  if(str(b->data) == str(buf)) {
    warn << "test OK\n";
    exit(-1);
  } else {
    warn << "test failed\n";
    exit(0);
  }
}


// callback immediately retrieves stored block through other node
void
insert_cb(testmaster *tm, dhash_stat status, ptr<insert_info> i)
{
  if(status != DHASH_OK)
    fatal << "test failed\n";
  (*tm)[1]->retrieve(i->key, wrap(retrieve_cb));
}


void
start_test(testmaster *tm)
{
  // int n = 2;
  (*tm)[0]->insert(buf, 10, wrap(insert_cb, tm));
  // tm->block(0, 1, wrap(&done, &n));
  // tm->unblock(0, 1, wrap(&done, &n));
}

int
main(int argc, char *argv[])
{
  testmaster *tm = New testmaster();
  tm->setup(slaves, wrap(start_test, tm));
  amain();
}
