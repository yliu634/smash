#include <input/input_types.h>
#include <gperftools/profiler.h>
#include <Othello/othello_cp_dp.h>
#include <Smash/node.h>
//#include "Ludo/ludo_cp_dp.h"
#include "CuckooPresized/cuckoo_cp_dp.h"
#include "VacuumFilter/vacuum_filter.h"

uint64_t NK_MAX = 20000000;
int version = 0;
Hasher64<string> h(0x19900111L);

template<class K, class Sample, class CP, class DP, class UpdateResult>
void test(uint64_t Nk, Distribution distribution) {
  for (int repeatCnt = 0; repeatCnt < 1; ++repeatCnt) {
    ostringstream oss;
    
    const char *typeName = typeid(Sample) == typeid(Tuple5) ? "5-tuple" :
                           typeid(Sample) == typeid(IPv4) ? "IPv4" :
                           typeid(Sample) == typeid(IPv6) ? "IPv6" :
                           typeid(Sample) == typeid(MAC) ? "MAC" :
                           typeid(Sample) == typeid(ID) ? "ID" :
                           typeid(Sample) == typeid(URL) ? "URL"
                                                         : typeid(K).name();
    
    const char *cpName = typeid(UpdateResult) == typeid(OthelloUpdateResult) ? "Othello" : "Ludo";
    
    oss << Nk << " " << (distribution ? "uniform" : "Zipfian") << " "
        << typeName << " repeat#" << repeatCnt << " version#" << version << " " << cpName;
    string logName = "../dist/logs/" + oss.str() + ".log";
    
    ifstream testLog(logName);
    string lastLine, tmp;
    
    while (getline(testLog, tmp)) {
      if (tmp.size() && tmp[0] == '|') lastLine = tmp;
    }
    testLog.close();

//    if (lastLine.size() >= 3 && lastLine[2] == '-') continue;
    
    InputBase::distribution = distribution;
    InputBase::bound = Nk;
    
    uint seed = h(logName);
    InputBase::setSeed(seed);
    
    TeeOstream tos(logName, GREEN);
    Clocker clocker(oss.str(), &tos);
    
    vector<K> keys(Nk);
    vector<uint16_t> values(Nk);
    uint64_t mask = (1 << 9) - 1;
    
    for (uint64_t i = 0; i < Nk; i++) {
      keys[i] = Sample::enumerate(i);
      values[i] = i & mask;
    }
    
    {
      Clocker add(cpName + string(" build"));
      CP c(Nk, false, keys, values);
      for (int i = 0; i < Nk; ++i) {
        K k = Sample::enumerate(i);
        uint16_t out;
        if (!(c.lookUp(k, out) && (out & mask) == (i & mask))) {
          c.lookUp(k, out);
          Clocker::count("error 0");
        }
      }
      
      for (uint64_t i = 0; i < Nk; ++i) {
        K k = Sample::enumerate(i);
        uint16_t out1, out2;
        if (!(c.lookUp(k, out1) && (out1 & mask) == (i & mask))) {
          c.lookUp(k, out1);
          Clocker::count("error 1");
        }
      }
      
      add.stop();
    }
    
    {
      CP c(100, false);
      
      uint16_t mask = (1 << 9) - 1;
      
      {
        Clocker add(cpName + string(" add"));
        for (uint64_t i = 0; i < Nk; ++i) {
          const K k = Sample::enumerate(i);
          UpdateResult result = c.insert(k, i & mask, false);
          
          if (result.status == 2) {
            Clocker::count("Triggered rebuild");
          } else if (result.status == 1) {
            Clocker::count("Insert to fallback");
          } else if (result.status == -1) {
            Clocker::count("error 2");
          }
          
          if (c.nKeys != i + 1) {
            Clocker::count("error -1");
          }

//          if (i == Nk / 10) c.triggerRebuild();

//        if(result.status) this_thread::sleep_for(1ms);
        }
        
        for (uint64_t i = 0; i < Nk; ++i) {
          K k = Sample::enumerate(i);
          uint16_t out;
          if (!(c.lookUp(k, out) && (out & mask) == (i & mask))) {
            Clocker::count("error 3");
            c.lookUp(k, out);
          }
        }
        
        c.checkIntegrity();
      }
      
      if (c.size() != Nk) {
        Clocker::count("error 4");
        c.size();
      }
      
      {
        Clocker cl("remove");
//        c.triggerRebuild();
        for (uint64_t i = 0; i < Nk; i += 2) {
          const K k = Sample::enumerate(i);
          c.remove(k);
        }
        
        for (uint64_t i = 0; i < Nk; ++i) {
          K k = Sample::enumerate(i);
          if (c.isMember(k) ^ (i % 2)) {
            Clocker::count("error 5");
            c.isMember(k);
            c.remove(k);
            c.isMember(k);
          }
        }
        
        for (uint64_t i = 1; i < Nk; i += 2) {
          K k = Sample::enumerate(i);
          uint16_t out1, out2;
          if (!(c.lookUp(k, out1) && (out1 & mask) == (i & mask))) {
            Clocker::count("error 6");
            c.lookUp(k, out1);
          }
        }
        
        if (c.size() != Nk / 2) {
          Clocker::count("error 7");
          c.size();
        }
      }
      
      for (uint64_t i = 0; i < Nk; i += 2) {
        const K k = Sample::enumerate(i);
        UpdateResult result = c.insert(k, i & mask, false);
        if (result.status == 2) {
          Clocker::count("Triggered rebuild");
        } else if (result.status == 1) {
          Clocker::count("Insert to fallback");
        } else if (result.status == -1) {
          Clocker::count("error 8");
        }
        
        if (result.status) this_thread::sleep_for(20ms);
        
        for (uint64_t ii = 0; ii < Nk; ii++) {
          if (ii > i && ii % 2 == 0) continue;
          
          K k = Sample::enumerate(ii);
          uint16_t out1, out2;
          if (!(c.lookUp(k, out1) && (out1 & mask) == (ii & mask))) {
            Clocker::count("error 9a");
            c.lookUp(k, out1);
          }
        }
      }
      
      for (uint64_t i = 0; i < Nk; i++) {
        K k = Sample::enumerate(i);
        uint16_t out1, out2;
        if (!(c.lookUp(k, out1) && (out1 & mask) == (i & mask))) {
          Clocker::count("error 9");
          c.lookUp(k, out1);
        }
      }
      
      for (uint64_t i = 0; i < Nk; ++i) {
        const K k = Sample::enumerate(i);
        c.changeValue(k, mask - i & mask);
        
        uint16_t out;
        if (!(c.lookUp(k, out) && (out & mask) == (mask - i & mask))) {
          Clocker::count("error 10");
          c.lookUp(k, out);
        }
      }
      
      for (uint64_t i = 0; i < Nk; ++i) {
        const K k = Sample::enumerate(i);
        uint16_t out;
        if (!(c.lookUp(k, out) && (out & mask) == (mask - i & mask))) {
          Clocker::count("error 11");
          c.lookUp(k, out);
        }
      }
    }
  }
}

void testVacuum() {
  cuckoofilter::VacuumFilter<uint64_t, 5, cuckoofilter::BetterTable> filter(NK_MAX);
  
  {
    Clocker c("insert");
    for (uint64_t i = 0; i < NK_MAX; ++i)
      if (filter.Add(i)) {
        Clocker::count("e0");
        filter.Add(i);
      }
  }
  
  {
    Clocker c("check");
    for (uint64_t i = 0; i < NK_MAX; ++i)
      if (filter.Contain(i)) {   // return 0 means "in"
        Clocker::count("e1");
      }
  }
  
  {
    Clocker c("del");
    for (uint64_t i = 0; i < NK_MAX; i += 2)
      if (filter.Delete(i)) {   // return 0 means "in"
        Clocker::count("e2");
      }
  }
  
  {
    Clocker c("check fn");
    for (uint64_t i = 1; i < NK_MAX; i += 2)
      if (filter.Contain(i)) {   // return 0 means "in"
        Clocker::count("e3");
      }
  }
  
  {
    Clocker c("check fp");
    for (uint64_t i = NK_MAX; i < 5 * NK_MAX; i++)
      if (!filter.Contain(i)) {   // return 0 means "in"
        Clocker::count("fp");
      }
    Clocker::count("total", 4 * NK_MAX);
  }
}

int main(int argc, char **argv) {
  commonInit();
  
  struct Bucket {  // only as parameters and return values for easy access. the storage is compact.
    uint8_t seed;
    Locations values[4];
  };
  
  cout << sizeof(Bucket) << endl;
  
  char tmp[] = {1, 2, 3, 4};
  int *test = (int *) tmp;
  uint16_t *test2 = (uint16_t *) (tmp + 1);
  cout << hex << *test << " " << *test2 << dec << endl;
  
  char *p1 = tmp, *p2 = tmp + 1;
  swap(p1, p2);
  cout << "swap "<< (*p1 == 2) << " " << (*p2 == 1) << endl;
  
  struct Ludo_PathEntry {  // waste space in this prototype. can be more efficient in memory
    int status; // >=0: the length of the locatorCC. location updated to othello and key is also empty.
    // -1 or -2: inserted to Cuckoo but othello is under rebuild: -1: first bucket. -2: second bucket.
    // int marks[2] = {-1, -1};  // TODO can further include the markers for alien key detection at the DP.
    
    uint32_t bid: 30;
    uint8_t sid: 2;
    uint8_t newSeed;
    uint8_t s0: 2, s1: 2, s2: 2, s3: 2;
  };
  
  cout << sizeof(Ludo_PathEntry) << endl;
  
  string s("ssss");
  cout << s << endl;
  cout << s.data() << endl;
  cout << s.length() << endl;
  cout << (int) s[0] << (int) s[1] << (int) s[2] << (int) s[3] << (int) s[4] << endl;
  
  struct LowLog {
    uint16_t timer0: 4, masterId0: 12;
    uint16_t timer1: 4, masterId1: 12;
    uint16_t timer2: 4, masterId2: 12;
    uint16_t timer3: 4, masterId3: 12;
  };
  
  cout << sizeof(LowLog) << endl;
  
  NK_MAX = (argc >= 3) ? atol(argv[2]) : NK_MAX;
//  testVacuum();
  
  for (uint64_t Nk = (argc >= 2) ? atol(argv[1]) : 1024ULL; Nk < NK_MAX; Nk <<= 1) {
    for (Distribution distribution:{exponential}) {
//      test<string, ID, ControlPlaneOthello<string, uint16_t, 9>, DataPlaneOthello<string, uint16_t, 9>, OthelloUpdateResult>(Nk, distribution);
//      test<string, ID, ControlPlaneLudo<string, uint16_t, 9>, DataPlaneLudo<string, uint16_t, 9>, ControlPlaneLudo<string, uint16_t, 9>::LudoUpdateResult>(Nk, distribution);
    }
  }
}
