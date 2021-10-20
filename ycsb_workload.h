#include "common.h"
#include "Smash/client.h"
#include "ceph.h"
#include <boost/asio/ip/host_name.hpp>
#include <experimental/filesystem>

class YCSB {
public:
  int recordcount = 1000;
  int operationcount = 1000;
  double readproportion = 0.5;
  double updateproportion = 0.5;
  double readmodifywriteproportion = 0;
  Distribution requestdistribution = zipfian;
  //ZipfianGenerator Z(0,100,0.9);
  int parallel = 1;
  string keyfile;
  int id;
  
  vector<char> buffer;  // for sending value
  vector<K> keys;
   
  void loadKeys(uint32_t size) {
    if (!filesystem::is_regular_file(keyfile)) {
      cerr << "Key file not present. " << endl;
      exit(-1);
    }
    
    cout << "loading keys" << endl;
    string key;
    ifstream f("dist/keys.txt");
    int i = 0;
    while (!f.eof() && keys.size() < size) {
      getline(f, key);
      i++;
      if (i <= keys.size()) continue;
      keys.push_back(key);
    }
    cout << "loaded" << endl;
  }
  
  template<class C>
  inline void load() {
    C client;
    loadKeys(recordcount);
    ofstream fout("dist/loadtime.txt",ios::out);
    struct timeval t1, t2;
    double timeuse;
    
    Clocker c("load");
    for (int i = id % parallel; i < recordcount; i += parallel) {
      sprintf((char *) buffer.data(), "%s#%d...", keys[i].c_str(), i);
      gettimeofday(&t1, NULL);
      client.Insert(keys[i], buffer.data());
      gettimeofday(&t2, NULL);
      
      timeuse = (t2.tv_sec-t1.tv_sec)*1000+(double)(t2.tv_usec-t1.tv_usec)/1000.0;
      fout << timeuse << " ";
    }
    //fout << endl;
    fout.close();
  }
  
  template<class C>
  inline void run() {
    C client;
    loadKeys(recordcount);
    Clocker c("run");
    ofstream fout("dist/run.txt",ios::out);
    struct timeval t1, t2;
    double timeuse;
    
    for (int i = id % parallel; i < operationcount; i += parallel) {
      double r = (double) rand() / RAND_MAX;
        string &k = keys[InputBase::rand()];
      if (r < readproportion + readmodifywriteproportion) { // read
        gettimeofday(&t1, NULL);
        buffer = client.Read(k);
        gettimeofday(&t2, NULL);
      }
      
      if (r > readproportion) {  // update. may read and update
        sprintf((char *) buffer.data(), "%s#%d...", k.c_str(), i);
        gettimeofday(&t1, NULL);
        client.Update(k, buffer.data());
        gettimeofday(&t2, NULL);
      }
      timeuse = (t2.tv_sec-t1.tv_sec)*1000+(double)(t2.tv_usec-t1.tv_usec)/1000.0;
      fout << timeuse << " ";
    }
    //fout << endl;
    fout.close();
  }
  
  template<class C>
  inline void remove() {
    C client;
    loadKeys(recordcount);
    Clocker c("remove");
    ofstream fout("dist/remove.txt",ios::out);
    struct timeval t1, t2;
    double timeuse;
    
    for (int i = id % parallel; i < recordcount; i += parallel) {
      string &k = keys[i];
      
      gettimeofday(&t1, NULL);
      client.Remove(k);
      gettimeofday(&t2, NULL);
      
      timeuse = (t2.tv_sec-t1.tv_sec)*1000+(double)(t2.tv_usec-t1.tv_usec)/1000.0;
      fout << timeuse << " ";
    }
    //fout << endl;
    fout.close();
  }
  
  inline void loadSmash() {
    load<Client>();
  }
  
  inline void runSmash() {
    run<Client>();
  }
  
  inline void removeSmash() {
    remove<Client>();
  }
  
  inline void loadCeph() {
    load<CephClient>();
  }
  
  inline void runCeph() {
    run<CephClient>();
  }
  
  inline void removeCeph() {
    remove<CephClient>();
  }
  
  YCSB(const string workload, int parallel = 1, int override_records = 1000, const string keyfile = "dist/keys.txt", const double zipconst = 0.99) : parallel(parallel), keyfile(keyfile) {
    string host = boost::asio::ip::host_name();
    string ids = host.substr(string("machine").length(), host.find('.'));
    id = atoi(ids.c_str());
    cout << "host name: " << host << ", id: " << id << endl;
    
    buffer.resize(blockSize);
    
    if (workload[0] == 'a') {
      recordcount = override_records;
      operationcount = 1000;
      readproportion = 1;
      updateproportion = 0;
//      scanproportion = 0;
//      insertproportion = 0;
      requestdistribution = uniform;
      
    } else if (workload[0] == 'b') {
      recordcount = override_records;
      operationcount = 1000;
      readproportion = 1;
      updateproportion = 0;
//      scanproportion = 0;
//      insertproportion = 0;
      requestdistribution = zipfian;
      
    } else if (workload[0] == 'c') {
      recordcount = override_records;
      operationcount = 1000;
      readproportion = 0;
      updateproportion = 1;
//      scanproportion = 0;
//      insertproportion = 0;
      requestdistribution = uniform;
    } else if (workload[0] == 'd') {
      recordcount = override_records;
      operationcount = 1000;
      readproportion = 0;
      updateproportion = 1;
      //scanproportion = 0;
      //insertproportion = 0;
      requestdistribution = zipfian;

//    } else if (workload[0] == 'e') {
//      recordcount = 1000;
//      operationcount = 1000;
//      readproportion = 0;
//      updateproportion = 0;
//      scanproportion = 0.95;
//      insertproportion = 0.05;
//      requestdistribution = zipfian;
//      maxscanlength = 100;
//      scanlengthdistribution = uniform;
    } else if (workload[0] == 'f') {
      recordcount = override_records;
      operationcount = 1000;
      readproportion = 0.5;
      updateproportion = 0;
//      scanproportion = 0;
//      insertproportion = 0;
      readmodifywriteproportion = 0.5;
      requestdistribution = zipfian;
    } else {
      ifstream f(workload);
      
      f >> recordcount;
      f >> operationcount;
      f >> readproportion;
      f >> updateproportion;
//      f >> scanproportion;
//      f >> insertproportion;
      f >> readmodifywriteproportion;
      
      string s;
      f >> s;
      if (s == "zipfian") {
        requestdistribution = zipfian;
      } else {
        requestdistribution = uniform;
      }
      
      f.close();
    }
    InputBase::setSeed(1);
    InputBase::distribution = requestdistribution;
    InputBase::bound = recordcount;
  }
  
};
