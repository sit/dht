#include <vector>
#include <string>
using namespace std;

// returns a vector of words in line separated by all characters in delims,
// defaulting to whitespace
vector<string>
split(string line, string delims)
{
  string::size_type bi, ei;
  vector<string> words;

  bi = line.find_first_not_of(delims);
  while(bi != string::npos) {
    ei = line.find_first_of(delims, bi);
    if(ei == string::npos)
      ei = line.length();
    words.push_back(line.substr(bi, ei-bi));
    bi = line.find_first_not_of(delims, ei);
  }

  return words;
}
