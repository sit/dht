#include "vivalditest.h"
#include "packet.h"
#include "p2psim.h"
#include "euclidean.h"
#include <stdio.h>
#include <algorithm>
using namespace std;

vector<VivaldiTest*> VivaldiTest::_all;

VivaldiTest::VivaldiTest(Node *n)
  : Protocol(n), _neighbors(0)
{
}

VivaldiTest::~VivaldiTest()
{
}

void
VivaldiTest::join(Args *args)
{

  int vo = args->nget<int>("vivaldi-algorithm");
  int dim = args->nget<int>("model-dimension");
  if (dim < 0) {
    cerr << "dimension must be specified (and positive)\n";
    exit (0);
  }

  switch(vo){
  case 1: _vivaldi = new Vivaldi1(node(),dim); break;
  case 2: _vivaldi = new Vivaldi2(node(),dim); break;
  case 3: _vivaldi = new Vivaldi3(node(),dim); break;
  case 4: _vivaldi = new Vivaldi4(node(),dim); break;
  case 5: _vivaldi = new Vivaldi5(node(),dim); break;
  case 6: _vivaldi = new Vivaldi6(node(),dim); break;
  case 7: _vivaldi = new Vivaldi7(node(),dim); break;
  case 8: _vivaldi = new Vivaldi8(node(),dim); break;
  case 9: _vivaldi = new Vivaldi9(node(),dim); break;
  case 10: _vivaldi = new Vivaldi10(node(),dim); break;
  default:
    fprintf(stderr, "VivaldiTest: bad Vivaldi algorithm %s\n",
            (*args)["vivaldi-algorithm"].c_str());
    exit(1);
  }

  _neighbors = atoi((*args)["neighbors"].c_str());
  _all.push_back(this);

  if(_neighbors > 0){
    int i;
    for(i = 0; i < _neighbors; i++){
      _nip.push_back(_all[random() % _all.size()]->node()->ip());
    }
  }


  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

char *
VivaldiTest::ts()
{
  static char buf[50];
  sprintf(buf, "%lu Vivaldi(%u)", now(), node()->ip());
  return buf;
}

Vivaldi::Coord
VivaldiTest::real()
{
  Vivaldi::Coord c;
  Euclidean *t =
    dynamic_cast<Euclidean*>(Network::Instance()->gettopology());
  if(t){
    Euclidean::Coord rc = t->getcoords(node()->ip());
    c.init2d(rc.first, rc.second);
  } else 
    c.init2d(0,0);
  return c;
}

// Calculate this node's error: sqrt of avg of squares
// of differences between synthetic and real distances to
// all other nodes.
double
VivaldiTest::error()
{
  Topology *t = (Network::Instance()->gettopology());
  double sum = 0;
  uint sum_sz = 0;
  Vivaldi::Coord vc = _vivaldi->my_location();
  for(unsigned i = 0; i < _all.size(); i++){
    Vivaldi::Coord vc1 = _all[i]->_vivaldi->my_location();
    double vd = dist(vc, vc1);
    double rd = t->latency(node(), _all[i]->node());
    if (rd > 0.0) {
      double rerr = (fabs(vd - rd)/rd);
      sum += rerr;
      sum_sz++;
    }
  }
  return (sum / sum_sz);
}

// return 5th, 50th, 95th percentiles of node error
void
VivaldiTest::total_error(double &e05, double &e50, double &e95)
{
  cerr << "-------------\n";
  unsigned n = _all.size();
  vector<double> a;
  for(unsigned i = 0; i < n; i++){
    double e = _all[i]->error();
    a.push_back(e);
    cerr << "err " << i << " : " << e << "\n";
  }
  sort(a.begin(), a.end());
  if(n > 5){
    e05 = a[n / 20];
    e50 = a[n / 2];
    e95 = a[n - (n / 20)];
  } else if(n > 0) {
    e05 = a[0];
    e50 = a[n / 2];
    e95 = a[n-1];
  }
  cerr << "------------\n";
}

void
VivaldiTest::print_all_loc()
{
  unsigned int n = _all.size();
  Vivaldi::Coord vc;
  for (uint i = 0; i < n; i++) {
    vc = _all[i]->_vivaldi->my_location();
    printf("COORD %u: ", (unsigned)now ());
    for (uint j = 0; j < vc._v.size(); j++)
      printf ("%.1f ", vc._v[j]);
    printf ("\n");
  }
}

void
VivaldiTest::status()
{
  static int first = 1;
  if (first) {
    first = 0;
    printf("# %s nnodes=%d %s neighbors=%d\n",
           typeid(*(this->_vivaldi)).name(),
           _all.size(),
           typeid(*(Network::Instance()->gettopology())).name(),
           _neighbors);
  }

  Vivaldi::Coord rc = real();
  Vivaldi::Coord vc = _vivaldi->my_location();
  double e05, e50, e95;
  total_error(e05, e50, e95);
  printf("vivaldi %u %u %d %.5f %.5f %.5f %.5f %.5f %.5f %.5f %.5f\n",
         (unsigned) now(),
         node()->ip(),
         _vivaldi->nsamples(),
         error(),
         e05,
         e50,
         e95,
         rc._v[0],
         rc._v[1],
         vc._v[0],
         vc._v[1]);
  fflush(stdout);
}

void
VivaldiTest::tick(void *)
{

  IPAddress dst;
  if(_neighbors > 0){
    dst = _nip[random() % _neighbors];
  } else {
    dst = _all[random() % _all.size()]->node()->ip();
  }

  Vivaldi::Coord c;
  Time before = now();
  doRPC(dst, &VivaldiTest::handler, (void*) 0, &c);
  if ((now() - before) > 0)
    _vivaldi->sample(dst, c, (now() - before) / 2.0);

  if((random() % (10 * _all.size())) == 0) {
    status();
    print_all_loc();
  }

  delaycb(1000, &VivaldiTest::tick, (void *) 0);
}

void
VivaldiTest::handler(void *args, Vivaldi::Coord *ret)
{
  *ret = _vivaldi->my_location();
}








