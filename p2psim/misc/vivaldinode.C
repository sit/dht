#include "vivaldinode.h"
#include "p2psim/network.h"
#include "topologies/euclidean.h"

VivaldiNode::VivaldiNode(IPAddress ip) : P2Protocol (ip)
{

  _nsamples = 0;
  _dim = args().nget<uint>("model-dimension", 3, 10);
  _adaptive = args().nget<uint>("adaptive", 0, 10);
  long timestep_scaled = args().nget<uint>("timestep", 5000, 10);
  _timestep = ((double)timestep_scaled)/1000000;
  _pred_err  = -1;
  _window_size = args().nget<uint>("window-size", -1, 10);

  // Start out at a random point/origin
  for (int i = 0; i < _dim; i++) 
    // _c._v.push_back(random() % 200000 - 1000000);
  _c._v.push_back (0.0);
}

VivaldiNode::~VivaldiNode()
{
}

// latency should be one-way, i.e. RTT / 2
// who: remote node IP
// c: remote node coord
// latency: measured RPC time
// err_r: remote node's error
void
VivaldiNode::sample(IPAddress who, Coord c, double e, double latency)
{
  assert (c.dim () > 0);
  algorithm(Sample(c, latency, e, who));
  _nsamples += 1;
}


vector<double> 
VivaldiNode::get_weights (vector<Sample> v)
{
  //calcuate weights
  double sum = 0.0;
  for (unsigned int i = 0; i < v.size (); i++) {
    if (v[i]._error > 0) sum += 1.0/v[i]._error;
    else sum += 1.0/1000.0;
  }

  vector<double> weights;
  //  cerr << "weights: ";
  for (unsigned int i = 0; i < v.size (); i++) {
    if (v[i]._error > 0) 
      weights.push_back (v.size()*(1.0/v[i]._error)/sum);
    else
      weights.push_back (v.size()*(1.0/1000.0)/sum);
    //    cerr << "(" << weights.back () << ", " << v[i]._error << ") ";
  }

  return weights;
}

void
VivaldiNode::update_error (vector<Sample> samples)
{

  vector<double> weights = get_weights(samples);

  double sum = 0.0;
  for (unsigned int i = 0; i < samples.size (); i++) {
    double expect = dist (_c, samples[i]._c);
    double actual = samples[i]._latency;
    double rel_error = fabs(expect - actual)/actual;
    sum += weights[i]*rel_error;
  }
  _pred_err = sum/samples.size ();
}

VivaldiNode::Coord
VivaldiNode::net_force(Coord c, vector<Sample> v)
{
  Coord f(v[0]._c.dim());
  
  //calcuate weights
  double sum = 0.0;
  for (unsigned int i = 0; i < v.size (); i++) {
    if (v[i]._error > 0) sum += 1.0/v[i]._error;
    else sum += 1.0;
  }
  vector<double> weights;
  //  cerr << "weights: ";
  for (unsigned int i = 0; i < v.size (); i++) {
    if (v[i]._error > 0) 
      weights.push_back (v.size()*(1.0/v[i]._error)/sum);
    else
      weights.push_back (1.0);
    //    cerr << "(" << weights.back () << ", " << v[i]._error << ") ";
  }
  //  cerr << "\n";

  for(unsigned i = 0; i < v.size(); i++){
    //    double noise = (double)(random () % (int)v[i]._latency) / 10.0;
    double noise = 0;
    double actual = v[i]._latency + noise;
    double expect = dist (c, v[i]._c);
    //    cerr << "force " << c << " " << actual << " " << expect << "\n";
    if(actual >= 0){

      double grad = expect - actual;
      Coord dir = (v[i]._c - c);
      double l = length(dir);
      while (l < 0.0001) { //nodes are on top of one another
	for (uint j = 0; j < dir._v.size(); j++) //choose a random direction
	    dir._v[j] += (double)(random () % 10 - 5) / 10.0;
	l = length (dir);
      }
      double unit = weights[i]/(l);
      VivaldiNode::Coord udir = dir * unit * grad;
      f = f + udir;
    }
  }
  return f;
}


// the current implementation
void
VivaldiNode::algorithm(Sample s)
{

  //reject timeouts and self pings
  if (s._latency > 1000000 ||
      s._latency < 1000) return;

  _samples.push_back(s);
  if ((int)_samples.size () < _window_size) return;

  update_error (_samples);

  Coord f = net_force(_c, _samples);

  _curts = _curts - 0.025;
  double t;
  if (_adaptive)
    t = (_curts > _timestep) ? _curts : _timestep;
  else 
    t = _timestep;

  // apply the force to our coordinates
  _c = _c + (f * t);

  _samples.clear ();

}

VivaldiNode::Coord
VivaldiNode::real_coords ()
{
  Coord ret;

  Topology *t = Network::Instance ()->gettopology ();
  Euclidean *e = dynamic_cast<Euclidean *>(t);
  if (e) {
    pair<int, int> c = e->getcoords (ip ());
    ret.init2d (c.first, c.second);
  }
  return ret;
}

ostream& operator<< (ostream &s, VivaldiNode::Coord &c) 
  {
    return s << c._v[0] << " " << c._v[1];
  }
