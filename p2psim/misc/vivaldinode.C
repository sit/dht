#include "vivaldinode.h"
#include "p2psim/network.h"
#include "topologies/euclidean.h"
#include "math.h"

static int usinght = -1;
#define PI 3.1415927

double spherical_dist_arc (VivaldiNode::Coord a, VivaldiNode::Coord b);
VivaldiNode::Coord  cross (VivaldiNode::Coord a, VivaldiNode::Coord b);
VivaldiNode::Coord rotate_arb (VivaldiNode::Coord axis, VivaldiNode::Coord vec,
			       double angle);

VivaldiNode::VivaldiNode(IPAddress ip) : P2Protocol (ip)
{
  if(usinght == -1){
    usinght = args().nget<uint>("using-height-vectors", 0, 10);
    printf("using-height-vectors = %d\n", usinght);
  }
  _nsamples = 0;
  _dim = args().nget<uint>("model-dimension", 3, 10);
  _adaptive = args().nget<uint>("adaptive", 0, 10);
  long timestep_scaled = args().nget<uint>("timestep", 5000, 10);
  _timestep = ((double)timestep_scaled)/1000000;
  _pred_err  = -1;
  _window_size = args().nget<uint>("window-size", (uint) -1, 10);
  _model = args()["model"];
  _radius = args().nget<uint>("radius", (uint) 20000, 10);
  _initial_triangulation = args().nget<uint>("triangulate", (uint) 0, 10);
  _num_init_samples = args().nget<uint>("num_init_samples", (uint) 0, 10);

  if (_model == "sphere")
    _model_type = MODEL_SPHERE;
  else 
    _model_type = MODEL_EUCLIDEAN;

  // Start out at a random point
  // for (int i = 0; i < _dim; i++) 
  //   _c._v.push_back (random() % 200000 - 1000000);
  // _c._ht = random() % 200000 - 1000000);

  if (_model_type == MODEL_EUCLIDEAN) {
    // Start out at the origin 
    for (int i = 0; i < _dim; i++)
      _c._v.push_back (0.0);
  } else if (_model_type == MODEL_SPHERE) {
    for (int i = 0; i < _dim; i++)
      _c._v.push_back (random ());
    //now unitize it
    _c = _c / length (_c);
    // now make it as long as the radius of the circle
    _c = _c * _radius;
  } else {
    assert (0);
  }

  _c._ht = 0;
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
  for (unsigned int i = 0; i < v.size (); i++) {
    if (v[i]._error > 0) 
      weights.push_back (v.size()*(1.0/v[i]._error)/sum);
    else
      weights.push_back (1.0);
  }

  for(unsigned i = 0; i < v.size(); i++){
    double actual = v[i]._latency;
    double expect = dist (c, v[i]._c);
    if(actual >= 0){

      double grad = expect - actual;
      Coord dir = (v[i]._c - c);
      double l = length(dir);
      while (l < 0.0001) { //nodes are on top of one another
	for (uint j = 0; j < dir._v.size(); j++) //choose a random direction
	    dir._v[j] += (double)(random () % 10 - 5) / 10.0;
	if (usinght)
	  dir._ht += (double)(random () % 10) / 10.0;
	l = length (dir);
      }
      double unit = weights[i]/(l);
      VivaldiNode::Coord udir = dir * unit * grad;
      f = f + udir;
    }
  }
  f._ht = -f._ht;

  return f;
}

VivaldiNode::Coord
VivaldiNode::my_location () 
{
  return _c; 
}

// the current implementation
void
VivaldiNode::algorithm(Sample s)
{

  //reject timeouts and self pings
  if (s._latency > 1000000 ||
      s._latency < 1000) return;

  if (_initial_triangulation && _init_samples.size () < _num_init_samples)
    {
      if ((ip () % 2 > 0) && (s._who % 2 > 0)) return;
      cerr << ip () << " initing with " << s._who << "\n";
      _init_samples.push_back (s);
      if (_init_samples.size () == _num_init_samples)
	initial_triangulation (_init_samples);
      return;
    }

  _samples.push_back(s);
  if ((int)_samples.size () < _window_size) return;

  update_error (_samples);

  _curts = _curts - 0.025;
  double t;
  if (_adaptive)
    t = (_curts > _timestep) ? _curts : _timestep;
  else 
    t = _timestep;

  // apply the force to our coordinates
  // cout << "move from " << _c << " with force " << f;



  if (_model_type == MODEL_SPHERE) {
    assert (_samples.size () == 1); // XXX only work with one sample for now
    Coord s = _samples[0]._c;

    //get the great-circle distance (as radians)
    double theta_predicted = spherical_dist_arc (_c, s); 
    double theta_actual = (_samples[0]._latency) / (_radius);

    if(theta_actual >= 0){
      //find the vector we'll rotate around
      Coord rot = cross (_c, s);

      //calculate how far to rotate
      double theta_correct = t * (theta_predicted - theta_actual);

      //rotate ourselves towards (or away) from the node we talked to
      Coord new_pos = rotate_arb (rot, _c, theta_correct);
      _c = new_pos;
    } else {
      cerr << "rejecting invalid measurement\n";
    }
  } else  { // EUCLIDEAN
    Coord f = net_force(_c, _samples);
    _c = _c + (f * t);
  }

  if (usinght && _c._ht <= 1000) // 1000 is 1ms
    _c._ht = 1000; 

  _samples.clear ();
}

void
VivaldiNode::initial_triangulation (vector<Sample> isamps)
{
  double prev_f = RAND_MAX;
  long iterations = 0;

  while (fabs(_pred_err - prev_f) > 0.0001
	 && iterations++ < 10000) {
    Coord f = net_force (_c, isamps);
    _c = _c + (f * 0.05); // XXX what timestep?
    prev_f = _pred_err;
    update_error (isamps);
    if (ip () == 1) cerr << ip () << " " << _pred_err << "\n";
  }

  cerr << ip () << ": " << _pred_err << "\n";
  return;
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

ostream&
operator<< (ostream &s, VivaldiNode::Coord &c) 
{
  for (uint i = 0; i < c._v.size(); i++){
    if (i)
      s << ",";
    s << c._v[i];
  }
  if (usinght)
    s << ",ht=" << c._ht;
  return s;
}


VivaldiNode::Coord
operator-(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  VivaldiNode::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back (a._v[i] - b._v[i]);
  if (usinght)
    c._ht = a._ht+b._ht;
  return c;
}

VivaldiNode::Coord
operator+(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  VivaldiNode::Coord c;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    c._v.push_back(a._v[i] + b._v[i]);
  if (usinght)
    c._ht = a._ht+b._ht;
  return c;
}

VivaldiNode::Coord
operator/(VivaldiNode::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] /= x;
  if (usinght)
    c._ht /= x;
  return c;
}

VivaldiNode::Coord
operator*(VivaldiNode::Coord c, double x)
{
  for (unsigned int i = 0; i < c._v.size (); i++) 
    c._v[i] *= x;
  if (usinght)
    c._ht *= x;
  return c;
}

double
operator* (VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  assert (a.dim () == b.dim ());
  double ret = 0.0;
  for (int i = 0; i < a.dim (); i++)
    ret += a[i]*b[i];
  return ret;
}

double
length(VivaldiNode::Coord c)
{
  double l = 0.0;
  for (unsigned int i = 0; i < c._v.size (); i++) 
    l += c._v[i]*c._v[i];
  l = sqrt(l);
  if (usinght)
    l += c._ht;
  return l;
}

VivaldiNode::Coord 
cross (VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  assert (a.dim () == b.dim ());

  VivaldiNode::Coord ret(a.dim ());
  ret[0] = a[1]*b[2] - a[2]*b[1];
  ret[1] = a[2]*b[0] - a[0]*b[2];
  ret[2] = a[0]*b[1] - a[1]*b[0];
  return ret;
}

VivaldiNode::Coord
rotate_arb (VivaldiNode::Coord axis, VivaldiNode::Coord vec,
	    double angle)
{
  //unitize axis
  VivaldiNode::Coord a = axis / (length (axis));
  
  //calculate the rotation matrix
  // do the multiply
  assert (a.dim () == vec.dim ());
  VivaldiNode::Coord ret (vec.dim());
  
  //helpful intermediates
  double cos_a = cos (angle);
  double sin_a = sin (angle);
  //let's just write out all nine components of the rotation
  // matrix. I know that this is slow. Hopefully -O2 will eat this up.
  // This is from Strang, Intro. To Linear Algebra, p. 371
  double M_11 = cos_a + (1 - cos_a)*a[0]*a[0];
  double M_12 = (1 - cos_a)*a[0]*a[1] - sin_a*a[2];
  double M_13 = (1 - cos_a)*a[0]*a[2] + sin_a*a[1];
  double M_21 = (1 - cos_a)*a[0]*a[1] + sin_a*a[2];
  double M_22 = cos_a + (1 - cos_a)*a[1]*a[1];
  double M_23 = (1 - cos_a)*a[1]*a[2] - sin_a*a[0];
  double M_31 = (1 - cos_a)*a[0]*a[2] - sin_a*a[1];
  double M_32 = (1 - cos_a)*a[1]*a[2] + sin_a*a[0];
  double M_33 = cos_a + (1 - cos_a)*a[2]*a[2];

  //now let's write out the result of the matrix
  // multiplication in terms of the variables above

  ret[0] = M_11*vec[0] + M_12*vec[1] + M_13*vec[2];
  ret[1] = M_21*vec[0] + M_22*vec[1] + M_23*vec[2];
  ret[2] = M_31*vec[0] + M_32*vec[1] + M_33*vec[2];
  
  return ret;

}
double
flatearth_dist(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  double d = 0.0;
  assert (a._v.size () == b._v.size ());
  for (unsigned int i = 0; i < a._v.size (); i++) 
    d += (a._v[i] - b._v[i])*(a._v[i] - b._v[i]);
  d = sqrt(d);
  if (usinght)
    d += a._ht + b._ht;
  return d;
}


// return the angle between to points on a sphere in radians
double 
VivaldiNode::spherical_dist_arc (VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  //coords are in cartesian space, vectors to points on a sphere
  // of radius RADIUS
  assert (a._v.size () == 3);

  //vectors are constructed to be RADIUS long, so 
  // we don't calculate the norm
  double cos_angle = (a * b)/(_radius*_radius); 
  
  return acos (cos_angle);
}

double 
VivaldiNode::spherical_dist (VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  double arc_dist = spherical_dist_arc (a, b);
  //  cerr << "dist from " << a << " to " << b << " is " << dist << "\n";
  return arc_dist * _radius;
}


double
VivaldiNode::dist(VivaldiNode::Coord a, VivaldiNode::Coord b)
{
  if (_model_type == MODEL_SPHERE)
    return spherical_dist (a, b);
  else 
    return flatearth_dist (a, b);
    
}
