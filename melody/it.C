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
    if(((in[i] >= 47) && (in[i] <= 57)) || /* / and numbers */
       ((in[i] >= 65) && (in[i] <= 90)) || /* A-Z */
       ((in[i] >= 97) && (in[i] <= 122))) /* a-z */
      *out << str(in+i, 1);
    else {
      sprintf(b, "%%%02x", in[i]);
      *out << b;
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

struct id3 {
  char tag[3];
  char title[30];
  char artist[30];
  char album[30];
  char year[4];
  char comment[30];
  char genre;
};

class dirstack;
class song {
  str filename;
  strbuf spaced;
  struct id3 id3;

public:
  song(str fn, const char *path);

  str artistf, albumf, titlef;
  str artistd, albumd, titled;
  str artist, album, title;
  dirstack *ds_webpage;
  vec<str> genre;
};

int min(size_t a, unsigned int b) {
  if(a<b) return a;
  return b;
} // FIXME this is crap

int splen(char *st, int len) {
  while((len > 0) && (*(st+len-1) == ' '))
    len--;
  return len;
}

static rxx s0("(.+)\\.(mp3|MP3)");
static rxx s1("(.+) *-+ *(.+)\\.(mp3|MP3)");
static rxx s2("[\\[\\{\\(](.+)[\\]\\}\\)] *(.+)\\.(mp3|MP3)");
static rxx s3("[0-9+] *-+ *(.+) *-+ *(.+)\\.(mp3|MP3)");

song::song(str fn, const char *path) : filename(fn), artistf(""), albumf(""), titlef(""), artistd(""), albumd(""), titled(""), artist(""), album(""), title("") {
  FILE *id3tmp = fopen(path, "r");
  if(id3tmp == NULL) { perror("can't open file"); exit(1); }
  if(fseek(id3tmp, -128, SEEK_END)) { perror("fseek"); exit(1); }
  if(fread(&id3, 128, 1, id3tmp) != 1) { warnx << path; perror("fread"); return; }
  if(fclose(id3tmp) != 0) { perror("fclose"); exit(1); }

  if(!strncmp(id3.tag, "TAG", 3)) {
    artistf = str(id3.artist, splen(id3.artist, 30));
    titlef = str(id3.title, splen(id3.title, 30));
    //    warn << id3.title << " " << id3.artist << " " << id3.album << " " << id3.year << " " << id3.comment << "\n";
  } else {
    tr(fn, spaced);

    if(s3.search(spaced)) {
      artistf = s3[1];
      titlef = s3[2];
    } else if(s2.search(spaced)) {
      artistf = s2[1];
      titlef = s2[2];
    } else if(s1.search(spaced)) {
      artistf = s1[1];
      titlef = s1[2];
    } else if(s0.search(spaced)) {
      titlef = s0[1];
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
  void add(str localpath, vec<str> *webpath);
  void wakeup();
  void stop();
};

void
inserter::add(str localpath, vec<str> *webpath) { // FIXME finish
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
    tmp << "\"(" << todo.front()->front() << ";[\\dabcdef]+)/\"";
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
  if(out.size() > 0) {
    todo.front()->pop_front();
    tourl(out.front(), &tmp);
    loc.push_back(str(tmp));
    dirloop();
  } else
    adddir(todo.front()->pop_front());
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
  tmp << "/(.+)\r\n";
  tmp2 << "\r\n\r\nadd_dir=" << dir;

  filt = New rxx(str(tmp));
  webpage.tosuio()->clear();
  out.clear();
  foo = New weblookup(host, port, tmp2, filt, &out, &webpage, 
		      wrap(this, &inserter::adddir_made), true);
  warn << (int)foo << "filter: " << tmp << "\n";
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

  f = fopen(todo_file.front(), "r");
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
  send();
}

class dirstack {
public:
  str name;
  dirstack *parent;
  vec<str> bandv;
  strbuf webpage;
  str album;
  bool albume;
  bool ready;
  vec<callback<void>::ptr> flophouse;

  void mkready();
  void sleep(callback<void>::ptr cb);
  dirstack(str dir, dirstack *p) : name(dir), parent(p), albume(false), ready(false) {};
  void getpath(strbuf tmp) { 
    if(parent) parent->getpath(tmp);
    tmp << name << "/";
  };
};

void
dirstack::mkready() {
  ready = true;
  while(flophouse.size() > 0) {
    (*flophouse.front())();
    flophouse.pop_front();
  }
}

void
dirstack::sleep(callback<void>::ptr cb) {
  flophouse.push_back(cb);
}

class walker {
  str path;
  inserter *ins;
  dirstack *dirs;
  strbuf garbage;

  void pathdispatch(DIR *dirp);
  void dirvisit(str dir);
  void guess(str foo, str path, dirstack *cur);
  void guess_gotband(song *a, dirstack *cur, str path, bool found);
  void guess_gotalbum(song *a, dirstack *cur, str path, bool found);
  void find_band(song *s, dirstack *cur, str path);
  void find_album(song *s, dirstack *cur, str path);
  void find_genre(song *s, cbv done);
  void insert(str path, song *a);
  void got_band(dirstack *d);
  void fetchbandweb(song *a, dirstack *fd, cbv done);
  void guess_final(str path, song *s);
public:
  walker(str ld, inserter *in);
};

walker::walker(str ld, inserter *in) : path(ld), ins(in), dirs(NULL) {
  DIR *dirp = opendir(path);
  if(dirp == NULL) { perror("walker::walker()"); exit(1); }

  pathdispatch(dirp);
  closedir(dirp);
}

void
walker::pathdispatch(DIR *dirp) {
  struct dirent *de;
  struct stat sb;
  strbuf pathtmp, pathtmp2;
  pathtmp << path << "/";
  if(dirs) dirs->getpath(pathtmp);

  while((de = readdir(dirp)) != NULL) {
    pathtmp2.tosuio()->clear();
    pathtmp2 << pathtmp << de->d_name;
    if(stat(str(pathtmp2).cstr(), &sb) != 0) { perror("walker::pthdspatch() stat"); continue; }
    switch(sb.st_mode & S_IFMT) {
    case S_IFREG:
      //      fprintf(stderr, "found file: %s %s\n", str(pathtmp).cstr(), de->d_name);
      guess(de->d_name, pathtmp2, dirs);
      break;
    case S_IFDIR:
      if(strcmp(de->d_name, ".") &&
	 strcmp(de->d_name, "..")) {
	//	fprintf(stderr, "found dir: %s %s\n", str(pathtmp).cstr(), de->d_name);
	dirvisit(de->d_name);
      }
      break;
    default:
      fprintf(stderr, "eh? %d %d %d %s\n", 
	      DT_REG, DT_DIR, de->d_type, de->d_name);
    }
  }
}

static rxx gband("Arts/Music/Bands_and_Artists/./(.+)\\?tc=1");

void
walker::dirvisit(str dir) {
  dirstack *tmp;
  strbuf pathtmp;
  pathtmp << path << "/";
  if(dirs) dirs->getpath(pathtmp);
  pathtmp << dir << "/";

  DIR *dirp = opendir(str(pathtmp).cstr());
  if(dirp == NULL) { perror("walker::dirvisit()"); return; }

  tmp = New dirstack(dir, dirs); // FIXME memleak
  dirs = tmp;

  strbuf google, g;
  tourl(dir, &google);
  g << "/search?btnG=Google+Search&hl=en&cat=gwd%2FTop&q=" << google;
  vNew weblookup("www.google.com", 80, g, &gband, &(dirs->bandv), &(dirs->webpage), 
		 wrap(this, &walker::got_band, dirs));

  pathdispatch(dirp);
  closedir(dirp);

  dirs = tmp->parent;
}

void
walker::guess(str foo, str path, dirstack *cur) {
  // search order:
  // 1 id3
  // 2 filename
  // 3 dirs + filename
  song *a = New song(foo, path);

  find_band(a, cur, path);

#if 0
  if(!a->songe) {
    // screwed. we can't even find a title???
    insert(path, a);
  } else if(!a->bande) {
    // only sorta found a title. check dirs
    find_band(a, cur, path);
  } else {
    strbuf google;
    google << "http://www.google.com/search?hl=en&lr=&ie=UTF-8&oe=UTF8&cat=gwd%2FTop&q=" << a->artistf;
    vNew weblookup(google, 80, &gband, &(dirs->bandv), &(dirs->webpage), 
		   wrap(this, &walker::got_band, dirs));
    find_genre(a, wrap(this, &walker::insert, path, a));
  }
#endif
}

void
walker::find_band(song *s, dirstack *cur, str path) {
  if(!cur) { guess_gotband(s, cur, path, false); return; }
  if(!cur->ready) { cur->sleep(wrap(this, &walker::find_band, s, cur, path)); return; }
  if(cur->bandv.size() == 0) { find_band(s, cur->parent, path); return; }
  s->artistd = cur->bandv[0]; // FIXME what about other names/???
  s->ds_webpage = cur;
  guess_gotband(s, cur, path, true);
}

void
walker::guess_gotband(song *a, dirstack *cur, str path, bool found) {
  if(found)
    find_album(a, cur, path);
  //    find_genre(a, wrap(this, &walker::find_album, a, cur, path));
  else if(a->artistf.len() > 0) {
    dirstack *fake = New dirstack("none", NULL); // FIXME memleak
    fetchbandweb(a, fake, wrap(this, &walker::find_album, a, cur, path));
  } else
    // screwed again
    guess_final(path, a);
}

void
walker::fetchbandweb(song *a, dirstack *fd, cbv done) {
  strbuf google, g;
  tourl(a->artistf, &google);
  g << "/search?btnG=Google+Search&hl=en&cat=gwd%2FTop&q=" << google;
  a->ds_webpage = fd;
  vNew weblookup("www.google.com", 80, g, &gband, &(fd->bandv), &(fd->webpage), done);
//  		 wrap(this, walker::find_genre, a, done));
}

void
walker::find_album(song *s, dirstack *cur, str path) { // FIXME look on web??
  if(!cur) { guess_gotalbum(s, cur, path, false); return; }
  if(!cur->ready) { cur->sleep(wrap(this, &walker::find_album, s, cur, path)); return; }
  if(!cur->albume) { find_album(s, cur->parent, path); return; }
  s->albumd = cur->album;
  guess_gotalbum(s, cur, path, true);
}

void
walker::guess_gotalbum(song *a, dirstack *cur, str path, bool found) {
  //  if(!found)
  //    a->album = "misc";
  // do we care about "found"?
  find_genre(a, wrap(this, &walker::guess_final, path, a));
}

static rxx gbandlink("href=\"?http://(.+)(?::80)?(/Top/Arts/Music/Bands_and_Artists/./.+)\"?\\>Arts");
static rxx ggenre("Arts/Music/Styles/.+/(.+)/\"?>");

void
walker::find_genre(song *s, cbv done) {
  if(s->ds_webpage && gbandlink.search(str(s->ds_webpage->webpage))) {
    vNew weblookup(gbandlink[1], 80, gbandlink[2], &ggenre, &(s->genre), &garbage, done); // FIXME host will be bad mem reference?
  } else {
    warn << "can't find band link in:" << s->ds_webpage->webpage << "\n";
    exit(1);
    done();
  }
}

void
walker::guess_final(str path, song *s) { // FIXME what path is this?
  // FIXME compare strings
  if(s->artistf.len() > 0)
    s->artist = s->artistf;
  else if(s->artistd.len() > 0)
    s->artist = s->artistd;
  else
    s->artist = "unknown";

  if(s->albumf.len() > 0)
    s->album = s->albumf;
  else if(s->albumd.len() > 0)
    s->album = s->albumd;
  else
    s->album = "misc";

  if(s->titlef.len() > 0)
    s->title = s->titlef;
  else if(s->titled.len() > 0)
    s->title = s->titled;

  if(s->genre.size() == 0)
    s->genre.push_back(str("misc"));

#if 0
}

void
walker::insert(str path, song *s) {
#endif
  vec<str> *wp = New vec<str>();
  wp->push_back(s->genre[s->genre.size()-1]);
  wp->push_back(s->artist);
  //  wp->push_back(s->album);
  ins->add(path, wp);
}

void
walker::got_band(dirstack *d) {
  d->mkready();
}

weblookup::weblookup(str h, short p, strbuf q, rxx *f, vec<str> *o, strbuf *wb, callback<void>::ptr d, bool pt) {
  query << q;
  //  warn << "q: " << q << " " << (int)this << "\n";
  host = h;
  port = p;
  filter = f;
  out = o;
  webpage = wb;
  fd = 0;
  done = d;
  post = pt;
  tcpconnect (host, port, wrap(this, &weblookup::connected));
  // FIXME retries?
}

void
weblookup::connected(int f) {
  //  warn << "f: " << f << " " << (int)this << "\n";
  fd = f;
  if(f < 0) {
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
  fprintf(stderr, "%s: localdir host port\n", progname.cstr());
  exit(1);
}

int main(int argc, char **argv) {
  setprogname (argv[0]);
  if(argc < 4)
    usage();

  vNew walker(argv[1], New inserter(argv[2], atoi(argv[3])));
  amain();
}
