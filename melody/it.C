/* insert tool */

/* theory of operation:
   walk directory tree:
    open root,
     for each dir, add to stack
     loop
   for each file found, try to guess it's band and song name
   start a lookup for band and album info
   continue

   once band and album info return, insert into melody
*/
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h>
#include "async.h"
#include "http.h"
#include "rxx.h"
#include <string.h>
#include "cs_output.h"

void tourl(str in, strbuf *out) {
  unsigned int i;
  char b[3];
  for(i=0; i<in.len(); i++) {
    if(in[i] == 0)
      return;
    else if(((in[i] >= 47) && (in[i] <= 57)) || /* / and numbers */
       ((in[i] >= 65) && (in[i] <= 90)) || /* A-Z */
       ((in[i] >= 97) && (in[i] <= 122))) /* a-z */
      *out << str(in+i, 1);
    else {
      sprintf(b, "%%%02x", in[i]);
      *out << b;
    }
  }
}

void fixsemicolon(str in, strbuf *out) {
  unsigned int i;
  char b[3];
  for(i=0; i<in.len(); i++) {
    if(in[i] == ';') {
      sprintf(b, "%%%02x", in[i]);
      *out << b;
    } else {
      *out << str(in+i, 1);
    }
  }
}

void tr(str in, strbuf out) {
  unsigned int i;
  for(i=0; i<in.len(); i++) {
    if(in[i] == '_') out << " ";
    else if((i<in.len()-1) &&(in[i] == '2') && (in[i+1] == '0')) i++;
    else out << str(in+i, 1);
  }
}

void sanitize(str in, strbuf *out) {
  unsigned int i;
  for(i=0; i<in.len(); i++) {
    if(((in[i] >= 47) && (in[i] <= 57)) || /* / and numbers */
       ((in[i] >= 65) && (in[i] <= 90)) || /* A-Z */
       ((in[i] >= 97) && (in[i] <= 122))) /* a-z */
      *out << str(in+i, 1);
    else {
      *out << " ";
    }
  }
}

class weblookup {
  int fd;
  rxx *filter;
  vec<str> *out;
  callback<void>::ptr done;
  strbuf buf;
  strbuf *webpage;
  strbuf query;
  str host;
  short port;
  bool post;
  int retries;

  void connected(int fd);
  void recvreply();
public:
  weblookup(str host, short port, strbuf query, rxx *filter, vec<str> *out, strbuf *webpage, callback<void>::ptr done, bool post=false);
  ~weblookup();
};

class inserter : public data_sender {
  str host;
  int port;
  vec<vec<str>*> todo;
  vec<str> todo_file;
  vec<str> loc;
  bool sleeping;
  strbuf webpage;
  vec<str> out;
  rxx *filt;
  cs_output *cs_out;
  FILE *f;
  int s;
  weblookup *foo;
  bool go;

  void send();
  void dirloop();
  void dirloop_gotdir();
  void adddir(str dir);
  void adddir_made();
  void addfile();
  void addfile_connected(int s);
public:
  inserter(str h, int p) : host(h), port(p), sleeping(true) {};
  void add(FILE *af, str localpath, vec<str> *webpath); // only use once
  void wakeup();
  void stop();
};

void
inserter::add(FILE *af, str localpath, vec<str> *webpath) {
  f = af;
  for(unsigned int i=0; i<webpath->size(); i++)
    printf("/%s", (*webpath)[i].cstr());
  //    cout << "/" << (*webpath)[i];
  //  cout << "   " << localpath << "\n";
  printf("   %s\n", localpath.cstr());

  todo_file.push_back(localpath);
  todo.push_back(webpath);
  if(sleeping)
    send();
}

void
inserter::send() {
  warn << "send\n";
  sleeping = false;
  loc.clear();
  dirloop();
}

void
inserter::dirloop() {
  warn << "dirloop\n";
  unsigned int i;
  strbuf tmp, tmp2;
  if(todo.size() == 0) {
    sleeping = true;
    return;
  }
  if(todo.front()->size() != 0) {
    tmp << "\"(";
    tourl(todo.front()->front(), &tmp);
    tmp << ";[\\dabcdef]+)/\"";
    warn << "looking for " << todo.front()->front() << " with " << tmp << "\n";
    filt = New rxx(str(tmp));
    for(i=0; i<loc.size(); i++)
      tmp2 << "/" << loc[i];
    tmp2 << "/";

    webpage.tosuio()->clear();
    out.clear();
    vNew weblookup(host, port, tmp2, filt, &out, &webpage, 
		   wrap(this, &inserter::dirloop_gotdir));
  } else {
    // we're here! send file.
    addfile();
  }
}

void
inserter::dirloop_gotdir() {
  strbuf tmp;

  warn << "dirloop_gotdir\n";
  delete filt;
  warn << "got ";
  if(out.size() > 0) {
    warnx << out.front() << "\n";
    todo.front()->pop_front();
    fixsemicolon(out.front(), &tmp);
    loc.push_back(tmp);
    dirloop();
  } else {
    warnx << " nothing!\n";
    adddir(todo.front()->pop_front());
  }
}

void
inserter::adddir(str dir) {
  warn << "adddir\n";
  unsigned int i;
  strbuf tmp, tmp2;
  tmp << "Location: http://.+";
  tmp2 << "POST /add_dir HTTP/1.0\r\nHost: " << host << "\r\nUser-Agent: it\r\nReferer: http://" << host << ":" << port;
  for(i=0; i<loc.size(); i++) {
    tmp << "/" << loc[i];
    tmp2 << "/" << loc[i];
  }    
  tmp2 << "/";
  tmp << "/(.+)\r";
  tmp2 << "\r\n\r\nadd_dir=" << dir;

  
  filt = New rxx(str(tmp));
  webpage.tosuio()->clear();
  out.clear();
  foo = New weblookup(host, port, tmp2, filt, &out, &webpage, 
		      wrap(this, &inserter::adddir_made), true);
  warn << (int)foo << "filter: " << tmp << "\n";
  warn << tmp2 << "\n";
}

void
inserter::adddir_made() {
  warn << "adddir_made\n";
  delete filt;
  if(out.size() == 0) {
    warn << (int)foo << " didn't make dir? " << webpage << "\n";
    exit(1);
  }
  warn << (int)foo << " made " << out.front() << "\n" << webpage << "\n";
  loc.push_back(out.front());
  dirloop();
}

void
inserter::addfile() {
  warn << "addfile\n";
  webpage.tosuio()->clear();
  tcpconnect (host, port, wrap(this, &inserter::addfile_connected));
}

static rxx pathf("(?:[^/]*/)*([^/]+)");

void
inserter::addfile_connected(int as) {
  warn << "addfile_connected\n";

  s = as;

  unsigned int i;
  long length = 140 - 14 + 32 - 4; //some escaped chars

  if(s < 0) {
    warn << "addfile failed connect. retrying\n";
    addfile();
    return;
  }

  //  f = fopen(todo_file.front(), "r");
  if(f == NULL) { perror("addfile_connected"); exit(1); }
  if(fseek(f, 0, SEEK_END)) { perror("fseek"); exit(1); }
  length += ftell(f);
  if(fseek(f, 0, SEEK_SET)) { perror("fseek"); exit(1); }
  if(!pathf.search(todo_file.front())) { warn << "bad pathf\n"; exit(1); }
  length += strlen(pathf[1]);

  webpage << "POST /dstore HTTP/1.0\r\nHost: " << host << "\r\nUser-Agent: it\r\nReferer: http://" << host << ":" << port;
  for(i=0; i<loc.size(); i++)
    webpage << "/" << loc[i];
  webpage << "/";
  webpage << "\r\nContent-Type: multipart/form-data; boundary=------garbagefakeboundry\r\nContent-Length: " << length << "\r\n\r\n";
  webpage << "------garbagefakeboundry\r\nContent-Disposition: form-data; name=\"file\"; filename=\"" << pathf[1] << "\"\r\nContent-Type: application/octet-stream\r\n\r\n";

  warn << webpage;

  cs_out = New cs_output(s, wrap(this, &inserter::stop), this, wrap(this, &inserter::stop));
  cs_out->take(str(webpage), webpage.tosuio()->resid(), this);
  go = true;
  wakeup();
}

void
inserter::wakeup() {
 start:
  char *outend = "\r\n------garbagefakeboundry\r\n";
  char buf[4096];
  int res;
  bool more = true;

  warn << "ft " << ftell(f) << "\n";
  if(!go) {
    warn << "nogo\n";
    return;
  }

  if(!feof(f)) {
    res = fread(buf, 1, 4096, f);
    warn << "res " << res << "\n";
    more = cs_out->take(buf, res, this);
  } else {
    warn << "csout done\n";
    cs_out->take(outend);
    cs_out->done();
    go = false;
    return;
  }
  if(ferror(f)) {
    perror("fread");
    exit(1);
  }
  if(more) {
    warn << "loop\n";
    goto start;
  }
  warn << "e1\n";
}

void
inserter::stop() {
  warn << "stop\n";
  fclose(f);
  close(s);
  todo.pop_front();
  todo_file.pop_front();

  // FIXME
  if(todo.size() == 0)
    exit(0);
  send();
}

weblookup::weblookup(str h, short p, strbuf q, rxx *f, vec<str> *o, strbuf *wb, callback<void>::ptr d, bool pt) {
  query << q;
  warn << "host: " << h << "q: " << q << " " << (int)this << "\n";
  host = h;
  port = p;
  filter = f;
  out = o;
  webpage = wb;
  fd = 0;
  done = d;
  post = pt;
  tcpconnect (host, port, wrap(this, &weblookup::connected));
  retries = 0;
}

void
weblookup::connected(int f) {
  //  warn << "f: " << f << " " << (int)this << "\n";
  fd = f;
  if(f < 0) {
    retries++;
    if(retries > 5)
      exit(1);
    warn << "weblookup failed connect " << host << ":" << port << query << ". retrying\n";
    tcpconnect(host, port, wrap(this, &weblookup::connected));
    return;
  }

  strbuf tmp;
  if(post)
    tmp << query;
  else
    tmp << "GET " << query << " HTTP/1.0\r\nHost: " << host << "\r\nReferer: http://" << host << ":" << port << "/\r\nUser-Agent: it\r\n\r\n";
  tmp.tosuio()->output(fd);
  fdcb (fd, selread, wrap (this, &weblookup::recvreply));
}

static rxx redir("Location: (.*)\r\n");

void
weblookup::recvreply ()
{
  switch (buf.tosuio()->input (fd)) {
  case -1:
    if (errno != EAGAIN) {
      warn << query << ": " << strerror (errno) << "\n";
      delete this;
    }
    break;
  case 0:
    // FIXME handle redirect, because then google fixes spelling mistakes.
    if(!post && redir.search(buf)) {
      vNew weblookup(host, 80, redir[1], filter, out, webpage, done); // FIXME bad mem ref redir?
    } else {
      *webpage << buf;
      while(filter->search(buf)) {
	out->push_back(str((*filter)[1]));
	buf.tosuio()->rembytes(filter->end(1));
      }
      (*done)();
    }
    delete this;
    break;
  }
}

weblookup::~weblookup() {
  if (fd >= 0) {
    fdcb (fd, selread, NULL);
    fdcb (fd, selwrite, NULL);
    close (fd);
  }
}

void usage(void) {
  fprintf(stderr, "%s: localfile destdir host port\n", progname.cstr());
  exit(1);
}

static rxx getfile("([^/]+)$");
static rxx getpath("^/([^/]*)");

int main(int argc, char **argv) {
  FILE *f;
  vec<str> path;

  setprogname (argv[0]);
  if(argc < 5)
    usage();

  if(!getfile.search(argv[1]))
    usage();

  if(!(f = fopen(argv[1], "r"))) {
    perror(argv[1]);
    usage();
  }

  while(getpath.search(argv[2])) {
    if(getpath[1].len() > 0)
      path.push_back(getpath[1]);
    argv[2] += getpath.end(1);
  }

  inserter *ins = New inserter(argv[3], atoi(argv[4]));
  ins->add(f, getfile[1], &path);

  amain();
}
