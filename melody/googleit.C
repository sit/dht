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
  song(str fn, dirstack *cur, const char *path);
  void use_filename();

  bool id3tagged;
  dirstack *d;
  const char *pth;
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
  while((len > 0) && ((*(st+len-1) == ' ') || (*(st+len-1) == 0)))
    len--;
  return len;
}

static rxx s0("^(.+)\\.(mp3|MP3)");
static rxx s1("^([^-]+) +-+ *(.+)\\.(mp3|MP3)");
static rxx s2("^[\\[\\{\\(](.+)[\\]\\}\\)] *(.+)\\.(mp3|MP3)");
static rxx s3("^[0-9+] *-+ *(.+) *-+ *(.+)\\.(mp3|MP3)");

static rxx nospace(" *(.+) *");

void
song::use_filename() {
    tr(filename, spaced);

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

static rxx pathfs("(?:[^/]*/)*([^/]+)");

song::song(str fn, dirstack *cur, const char *path) : filename(fn), id3tagged(false), d(cur), pth(path), artistf(""), albumf(""), titlef(""), artistd(""), albumd(""), titled(""), artist(""), album(""), title("") {
  artistf = fn;
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
  dirstack *dirs;
  strbuf garbage;

  void pathdispatch(DIR *dirp);
  void dirvisit(str dir);
  void guess(str foo, str path, dirstack *cur);
  void guess_gotband(song *a, dirstack *cur, str path, bool found);
  void guess_gotalbum(song *a, dirstack *cur, str path, bool found);
  void find_album(song *s, dirstack *cur, str path);
  void find_genre(song *s, cbv done);
  void insert(str path, song *a);
  void got_band(dirstack *d);
  void fetchbandweb(song *a, dirstack *fd, cbv done);
  void guess_final(str path, song *s);
public:
  void find_band(song *s, dirstack *cur, str path);
  walker() {};
};

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
  song *a = New song(foo, cur, path);

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

static rxx gbandlink("href=\"?http://([^/\\>]+?google.com)(?::80)?(/[^\\>]*?/Music/Styles/[^\\>]+/Bands_and_Artists/[^\\>]+?)\"?\\>");
static rxx ggenre("/Music/Styles/([^/]+)/[^\\>]+?\"?>");
static rxx gbandlink2("href=\"?http://([^/\\>]+?google.com)(?::80)?(/[^\\>]+?/Bands_and_Artists/[^\\>]+?)\"?\\>");
static rxx ggenre2("Arts/Music/Styles/(?:By_Decade/|)([^/]+)/[^\\>]*?\"?>Arts");
static rxx gbandlink3("href=\"?http://([^/\\>]+?google.com)(?::80)?(/Top/Arts/Movies/Titles/[^\\>]+?)\"?\\>");

void
walker::find_genre(song *s, cbv done) {
  if(s->ds_webpage && gbandlink.search(str(s->ds_webpage->webpage))) {
    //    vNew weblookup(gbandlink[1], 80, gbandlink[2], &ggenre, &(s->genre), &garbage, done); // FIXME host will be bad mem reference?
    while(ggenre.search(s->ds_webpage->webpage)) {
      s->genre.push_back(str(ggenre[1]));
      s->ds_webpage->webpage.tosuio()->rembytes(ggenre.end(1));
    }
    (*done)();
  } else if(s->ds_webpage && gbandlink3.search(str(s->ds_webpage->webpage))) {
    s->genre.push_back("soundtrack");
    (*done)();
  } else if(s->ds_webpage && gbandlink2.search(str(s->ds_webpage->webpage))) {
    vNew weblookup(gbandlink2[1], 80, gbandlink2[2], &ggenre2, &(s->genre), &garbage, done); // FIXME host will be bad mem reference?
  } else {
    warn << "can't find band link in:" << s->ds_webpage->webpage << "\n";
    if(s->id3tagged) {
      s->id3tagged = false;
      s->use_filename();
      find_band(s, s->d, s->pth);
    } else
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

  for(unsigned int i =0; i<s->genre.size(); i++)
    warnx << s->genre[i] << "\n";
  printf("%s", s->genre[s->genre.size()-1].cstr());
  exit(0);
    
#if 0
}

void
walker::insert(str path, song *s) {
#endif
  vec<str> *wp = New vec<str>();
  warn << " insert " << s->genre[0] << ", " << s->artist << "\n";
  strbuf tmp;
  sanitize(s->genre[0], &tmp);
  wp->push_back(tmp);
  wp->push_back(s->artist);
  //  wp->push_back(s->album);
}

void
walker::got_band(dirstack *d) {
  d->mkready();
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
  fprintf(stderr, "%s: localdir host port\n", progname.cstr());
  exit(1);
}

int main(int argc, char **argv) {
  setprogname (argv[0]);
  if(argc != 2)
    usage();

  song *s = New song(argv[1], NULL, argv[1]);
  walker *w = New walker();
  w->find_band(s, NULL, "");

  amain();
}
