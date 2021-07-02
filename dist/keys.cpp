#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
using namespace std;
int main(){
  ofstream outfile;
  outfile.open ("keys.txt");
  //assert(outfile);
  uint32_t n = 1000000;
  //for(auto &el : baseline)
  for(uint32_t i = 0; i < n ; i++){
    outfile << to_string(rand() % 742829742) <<"\n";
  }
  outfile.close();
  return 0;
}