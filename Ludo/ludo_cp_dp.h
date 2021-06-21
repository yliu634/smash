/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 ==============================================================================*/

#pragma once

#include "../CuckooPresized/cuckoo_ht.h"
#include "utils/hash.h"
#include "../common.h"
#include "../Othello/othello_cp_dp.h"

// Class for efficiently storing key->value mappings when the size is
// known in advance and the keys are pre-hashed into uint64s.
// Keys should have "good enough" randomness (be spread across the
// entire 64 bit space).
//
// Important:  Clients wishing to use deterministic keys must
// ensure that their keys fall in the range 0 .. (uint64max-1);
// the table uses 2^64-1 as the "not occupied" flag.
//
// Inserted k must be unique, and there are no update
// or delete functions (until some subsequent use of this table
// requires them).
//
// Threads must synchronize their access to a PresizedHeadlessCuckoo.
//
// The cuckoo hash table is 4-way associative (each "bucket" has 4
// "slots" for key/value entries).  Uses breadth-first-search to find
// a good cuckoo path with less data movement (see
// http://www.cs.cmu.edu/~dga/papers/cuckoo-eurosys14.pdf )

template<class K, class V, uint VL>
class DataPlaneLudo;

template<class K>
struct Ludo_PathEntry {  // waste space in this prototype. can be more efficient in memory
  int status; // >=0: the length of the locatorCC. location updated to othello and key is also empty. 
  // int marks[2] = {-1, -1};  // can further include the markers for alien key detection at the DP.
  
  uint32_t bid: 30;
  uint8_t sid: 2;
  uint8_t newSeed;
  uint8_t s0: 2, s1: 2, s2: 2, s3: 2;
  vector<uint32_t> locatorCC;  // only useful when locator updated (1/2 probability)
};

// Insertion into a Ludo may:
// success (0). othello need enlarge/rebuild, and is done (1)// and is not permitted (-1).
// cuckoo enlarge/rebuild, and is done (2)// and is not permitted (-2)
template<class K>
struct LudoUpdateResult {
  // 0: success. 1: updated after othello rebuild. -1: othello need rebuild. 2: updated after a enlarge. -2: need enlarge
  int status = 0;
  vector<Ludo_PathEntry<K>> path;
};


template<class K, class V, uint VL = sizeof(V) * 8>
class LudoCommon {
public:
  static const uint8_t LocatorSeedLength = 5;
  static const uint MAX_REHASH = 2;
  
  static_assert(sizeof(V) * 8 >= VL);
  static const uint64_t ValueMask = (1ULL << VL) - 1;
  static const uint64_t VDMask = ValueMask;
  
  // The load factor is chosen slightly conservatively for speed and
  // to avoid the need for a table rebuild on insertion failure.
  // 0.94 is achievable, but 0.85 is faster and keeps the code simple
  // at the cost of a small amount of memory.
  // NOTE:  0 < kLoadFactor <= 1.0
  static constexpr double kLoadFactor = 0.95;
  
  // Cuckoo insert:  The maximum number of entries to scan should be ~400
  // (Source:  Personal communication with Michael Mitzenmacher;  empirical
  // experiments validate.).  After trying 400 candidate locations, declare
  // the table full - it's probably full of unresolvable cycles.  Less than
  // 400 reduces max occupancy;  much more results in very poor performance
  // around the full point.  For (2,4) a max BFS path len of 5 results in ~682
  // nodes to visit, calculated below, and is a good value.
  static constexpr uint8_t kMaxBFSPathLen = 5;
  
  static const uint8_t kSlotsPerBucket = 4;   // modification to this value leads to undefined behavior
  
  // Utility function to compute (x * y) >> 64, or "multiply high".
  // On x86-64, this is a single instruction, but not all platforms
  // support the __uint128_t type, so we provide a generic
  // implementation as well.
  inline uint32_t multiply_high_u32(uint32_t x, uint32_t y) const {
    return (uint32_t) (((uint64_t) x * (uint64_t) y) >> 32);
  }
  
  inline void fast_map_to_buckets(uint64_t x, uint32_t *twoBuckets, uint32_t size) const {
    // Map x (uniform in 2^64) to the range [0, num_buckets_ -1]
    // using Lemire's alternative to modulo reduction:
    // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    // Instead of x % N, use (x * N) >> 64.
    assert((size & (size - 1)) == 0);
    
    twoBuckets[0] = x & (size - 1); //multiply_high_u32(x, size);
    twoBuckets[1] = (x >> 32) & (size - 1); //multiply_high_u32(x >> 32, size);
  }
  
  uint32_t num_buckets_;
  FastHasher64<K> h;
  FastHasher64<K> digestH;
  
  virtual void setSeed(uint32_t s) {
    srand(s);
    h.setSeed(rand() | (uint64_t(rand()) << 32));
    digestH.setSeed(rand() | (uint64_t(rand()) << 32));
  }
};


// Cuckoo never failed to insert. if cuckoo rebuids for larger capacity, sync all data after the whole rebuild.
// Only consider concurrent update for othello locator
template<class K, class V, uint VL = sizeof(V) * 8>
class ControlPlaneLudo : public LudoCommon<K, V, VL> {
public:
  using LudoCommon<K, V, VL>::kSlotsPerBucket;
  using LudoCommon<K, V, VL>::kLoadFactor;
  using LudoCommon<K, V, VL>::kMaxBFSPathLen;
  
  using LudoCommon<K, V, VL>::VDMask;
  using LudoCommon<K, V, VL>::ValueMask;
  
  using LudoCommon<K, V, VL>::fast_map_to_buckets;
  using LudoCommon<K, V, VL>::multiply_high_u32;
  
  using LudoCommon<K, V, VL>::LocatorSeedLength;
  
  using LudoCommon<K, V, VL>::MAX_REHASH;
  using LudoCommon<K, V, VL>::num_buckets_;
  using LudoCommon<K, V, VL>::h;
  using LudoCommon<K, V, VL>::digestH;
  
  typedef LudoUpdateResult<K> UpdateResult;
  typedef Ludo_PathEntry<K> PathEntry;
  
  // Buckets are organized with key_types clustered for access speed
  // and for compactness while remaining aligned.
  struct Bucket {
    uint8_t seed = 0;
    uint8_t occupiedMask = 0;
    K keys[kSlotsPerBucket];
    V values[kSlotsPerBucket];
  } empty_bucket;
  
  uint32_t nKeys = 0;
  
  ControlPlaneOthello<K, uint8_t, 1, 0, true> locator;
  
  // Set upon initialization: num_entries / kLoadFactor / kSlotsPerBucket.
  std::vector<Bucket> buckets_, oldBuckets;
  
  virtual void setSeed(uint32_t s) {
    LudoCommon<K, V, VL>::setSeed(s);
    
    locator.hab.setSeed(rand() | (uint64_t(rand()) << 32));
    locator.hd.setSeed(rand());
  }
  
  // The key type is fixed as a pre-hashed key for this specialized use.
  ControlPlaneLudo(uint32_t capacity_ = 64, bool compact = false, const vector<K> &keys = vector<K>(), const vector<V> &values = vector<V>())
      : locator(1U, true), nKeys(0), capacity(capacity_) {
    assert(!compact);
    
    uint32_t toInsert = min((uint32_t) min(keys.size(), values.size()), capacity);
    
    num_buckets_ = 64U;
    
    for (capacity = num_buckets_ * kLoadFactor * kSlotsPerBucket; capacity < capacity_; capacity = num_buckets_ * kLoadFactor * kSlotsPerBucket)
      num_buckets_ <<= 1U;
    
    buckets_.resize(num_buckets_, empty_bucket);
    locator.resizeCapacity(toInsert);
    // till this line, an empty ludo is ready
    if (toInsert) { // the oldMap should be initialized
      for (int i = 0; i < toInsert; ++i) {
        insert(keys[i], values[i], false);
      }
    }
  }
  
  // The key type is fixed as a pre-hashed key for this specialized use.
  template<class V_, uint vl>
  ControlPlaneLudo(ControlPlaneLudo<K, V_, vl> another, unordered_map<V_, V> m)
      : locator(another.locator), nKeys(another.nKeys), capacity(another.capacity) {
    h = another.h;
    digestH = another.digestH;
    num_buckets_ = another.num_buckets_;
    buckets_.clear();
    buckets_.resize(num_buckets_, empty_bucket);
    
    for (uint i = 0; i < num_buckets_; ++i) {
      typename ControlPlaneLudo<K, V, vl>::Bucket &ob = another.buckets_[i];
      Bucket &b = buckets_[i];
      
      b.keys = ob.keys;
      b.seed = ob.seed;
      b.occupiedMask = ob.occupiedMask;
      
      for (char slot = 0; slot < kSlotsPerBucket; slot++) {
        if (b.occupiedMask & (1 << slot)) {
          b.values[slot] = m[b.values[slot]];
        }
      }
    }
  }
  
  pair<uint32_t, uint32_t> locate(const K &k) const {
    uint32_t buckets[2];
    fast_map_to_buckets(h(k), buckets, buckets_.size());
    
    for (uint32_t &b : buckets) {
      const Bucket &bucket = buckets_[b];
      for (uint32_t slot = 0; slot < kSlotsPerBucket; slot++) {
        if ((bucket.occupiedMask & (1 << slot)) && (bucket.keys[slot] == k)) {
          return make_pair(b, slot);
        }
      }
    }
    
    return make_pair((uint32_t) -1, (uint32_t) -1);
  }
  
  inline uint32_t size() const {
    return nKeys;
  }
  
  uint32_t capacity = 256U;
  
  bool powerSize(uint times) {
    checkIntegrity();
    
    unsigned long oldSize = num_buckets_ / times;
    buckets_.resize(num_buckets_, empty_bucket);
    locator.resizeCapacity(capacity);
    
    integrity = false;
    
    for (uint32_t i = 0; i < oldSize; ++i) {
      auto &bucket = buckets_[i];
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (!(bucket.occupiedMask & (1 << slot))) continue;
        
        K &key = bucket.keys[slot];
        V value = bucket.values[slot];
        
        uint32_t buckets[2];
        fast_map_to_buckets(h(key), buckets, num_buckets_);
        
        uint32_t bi = buckets[locator.lookUp(key)];  // get old bucket
        Bucket &dstBucket = buckets_[bi];
        
        if (bi != i) {
          bucket.occupiedMask ^= 1U << slot;
          
          dstBucket.occupiedMask |= 1U << slot;
          dstBucket.keys[slot] = key;
          dstBucket.values[slot] = value;
          
          updateSeed(bi);
        }
      }
    }
    integrity = true;
    checkIntegrity();
    return true;
  }
  
  /// Resize key and value related memory for the Othello to be able to hold keyCount keys
  /// \param targetCapacity the target capacity
  /// \note Side effect: will change nKeysInOthello, and if hash size is changed, a rebuild is performed
  void resizeCapacity(uint32_t targetCapacity, bool forceBuild = false) {
    targetCapacity = max(nKeys, max(targetCapacity, 256U));
    
    uint64_t nextNbuckets = 64U;
    uint64_t nextCapacity = nextNbuckets * kLoadFactor * kSlotsPerBucket;
    for (; nextCapacity < targetCapacity; nextCapacity = nextNbuckets * kLoadFactor * kSlotsPerBucket)
      nextNbuckets <<= 1U;
    
    bool shrink = nextNbuckets < num_buckets_;
    bool enlarge = nextNbuckets > num_buckets_;
    uint times = enlarge ? nextNbuckets / num_buckets_ : num_buckets_ / nextNbuckets;
    
    num_buckets_ = nextNbuckets;
    capacity = nextCapacity;
    
    // until here, ludo+fallback is integral as a whole, except that capacity and num_buckets_ are changed.
    if (enlarge && integrity) {
      if (powerSize(times)) return;
    }
//    } else if (shrink && integrity) {
//      if (foldSize(times)) return;
//    }
    // at here, ludo+fallback is still integral as a whole
    
    integrity = false;
    oldBuckets = buckets_;
    
    locator.resizeCapacity(capacity);
    buckets_.resize(num_buckets_, empty_bucket);
    
    if (num_buckets_ >= (1U << 30U)) {
      throw runtime_error("Current design only support up to 4 billion key set size! ");
    }
    
    if (forceBuild || enlarge || shrink) {
      build();
    }
  }
  
  // Returns collided key if some key collides with the key being inserted;
  // returns null if the table is full and the k-v is inserted to the fallback table; returns &k if inserted successfully.
  // if online, the updates are to be sent to the dp. so collect the path, and do not allow blocking rebuild in both Othello and Ludo
  // else (offline), we want all the updates to be directly reflected, even when it incur further and recursive retries.
  UpdateResult insert(const K &k, V v, bool online = true) {
    checkIntegrity();

//    v = v & ValueMask;
    UpdateResult result = {};
    if (isMember(k)) {
      result = changeValue(k, v);
    } else if (nKeys + 1 > capacity) {
      if (online) {
        result.status = -2;
      } else {
        resizeCapacity(nKeys + 1, true);
        result = insert(k, v, false);
        if (result.status < 0) throw runtime_error("impossible");
        result.status = 2;
      }
    } else {
      uint32_t target_bucket = -1;
      char target_slot = -1;
      uint32_t buckets[2];
      fast_map_to_buckets(h(k), buckets, buckets_.size());
      
      for (char i = 0; i < 2 && target_slot < 0; ++i) {
        uint32_t bi = buckets[i];
        Bucket &bucket = buckets_[bi];
        
        for (char slot = 0; slot < kSlotsPerBucket; slot++) {
          if (bucket.occupiedMask & (1 << slot)) {
          } else if (target_slot == -1) {
            target_bucket = bi;
            target_slot = slot; // do not break, to go through full duplication test
            
            break;
          }
        }
      }
      
      if (target_slot != -1) {
        if(full_debug) Clocker::count("direct insert");
        bool succ = putItem(k, v, target_bucket, target_slot, target_bucket == buckets[0], online ? &result.path : nullptr);
        if (!succ) result.status = -1;  // must be due to Othello
      } else {
        if(full_debug) Clocker::count("cuckoo insert");
        result = CuckooInsert(k, v, online);  // if fail, may be othello or cuckoo fail
      }
      
      if (result.status >= 0) nKeys++;
    }
    
    checkIntegrity();
    return result;
  }
  
  void checkIntegrity() {
    #ifdef FULL_DEBUG
    unordered_set<K> unvisited = unordered_set<K>(locator.keys.begin(), locator.keys.begin() + locator.nKeysInOthello);
    
    for (uint32_t i = 0; i < buckets_.size(); ++i) {
      Bucket &bucket = buckets_[i];
      
      for (uint32_t slot = 0; slot < kSlotsPerBucket; slot++) {
        if (!(bucket.occupiedMask & (1U << slot))) continue;
        K &k = bucket.keys[slot];
        V v = bucket.values[slot];
        
        uint32_t buckets[2];
        fast_map_to_buckets(h(k), buckets, buckets_.size());
        
        assert(locator.isMember(k));
        assert(buckets[locator.lookUp(k)] == i);
        
        assert(unvisited.erase(k) == 1);
      }
    }
    
    assert(unvisited.empty());
    #endif
  }
  
  inline bool isMember(K k) {
    uint32_t buckets[2];
    fast_map_to_buckets(h(k), buckets, buckets_.size());
    V _;
    for (uint32_t &b : buckets) {
      const Bucket &bucket = buckets_[b];
      if (FindInBucket(k, bucket, _)) return true;
    }
    return false;
  }
  
  inline UpdateResult remove(const K &k) {
    uint32_t buckets[2];
    fast_map_to_buckets(h(k), buckets, buckets_.size());
    
    for (uint32_t &b : buckets) {
      Bucket &bucket = buckets_[b];
      
      if (RemoveInBucket(k, bucket)) {
        nKeys--;
        return {};
      }
    }
    
    checkIntegrity();
    return {};
  }
  
  // slot :2    bucket:30
  inline UpdateResult changeValue(const K &k, V val) {
    uint32_t buckets[2];
    fast_map_to_buckets(h(k), buckets, buckets_.size());
    
    for (uint32_t &b : buckets) {
      Bucket &bucket = buckets_[b];
      
      for (char slot = 0; slot < kSlotsPerBucket; slot++) {
        if (!(bucket.occupiedMask & (1 << slot))) continue;
        
        if (k == bucket.keys[slot]) {
          bucket.values[slot] = val;
          
          return {0, {{0, b, uint8_t(FastHasher64<K>(bucket.seed)(k) >> 62)}}};
        }
      }
    }
    checkIntegrity();
    return {};
  }
  
  // Returns true if found.  Sets *out = value.
  inline bool lookUp(const K &k, V &out) const {
    uint32_t buckets[2];
    fast_map_to_buckets(h(k), buckets, buckets_.size());
    
    for (uint32_t &b : buckets) {
      const Bucket &bucket = buckets_[b];
      if (FindInBucket(k, bucket, out)) return true;
    }
    
    return false;
  }
  
  template<class NV>
  ControlPlaneLudo<K, NV> Compose(unordered_map<V, NV> &trans) {
    ControlPlaneLudo<K, NV> other;
    
    other.nKeys = nKeys;
    other.cpq_.reset();
    buckets_.resize(num_buckets_, empty_bucket);
    other.h = h;
    
    for (uint32_t i = 0; i < num_buckets_; ++i) {
      auto &bsrc = buckets_[i];
      auto &bdst = other.buckets_[i];
      bdst.occupiedMask = bsrc.occupiedMask;
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (!(bsrc.occupiedMask & (1 << slot))) continue;
        
        bdst.keys[slot] = bsrc.keys[slot];
        
        V &value = bsrc.values[slot];
        auto it = trans.find(value);
        if (it != trans.end()) {
          bdst.values[slot] = it->second;
        } else {
          bdst.occupiedMask &= ~(1 << slot);
          other.nKeys--;
        }
      }
    }
    
    return other;
  }
  
  void SelfCompose(unordered_map<V, V> &trans) {
    for (uint32_t i = 0; i < num_buckets_; ++i) {
      auto &bucket = buckets_[i];
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (!(bucket.occupiedMask & (1 << slot))) continue;
        
        V &value = bucket.values[slot];
        auto it = trans.find(value);
        if (it != trans.end()) {
          bucket.values[slot] = it->second;
        } else {
          bucket.occupiedMask &= ~(1 << slot);
          nKeys--;
        }
      }
    }
  }
  
  // only offline. return true if no ludo rebuild is involved
  inline void Merge(const unordered_map<K, V> &another, function<bool(const V &)> isValid) {
    for (auto &p:another) {
      if (isValid(p.second)) {
        UpdateResult res = insert(p.first, p.second, false);
      } else {
        remove(p.first);
      }
    }
  }
  
  unordered_map<K, V, FastHasher64<K>> toMap() const {
    unordered_map<K, V, FastHasher64<K>> map;
    
    for (auto &bucket: buckets_) {  // all buckets
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (bucket.occupiedMask & (1 << slot)) {
          map.insert(make_pair(bucket.keys[slot], bucket.values[slot]));
        }
      }
    }
    
    return map;
  }
  
  // This function will not take memory on the heap into account because we don't know the memory layout of keys on heap
  uint64_t getMemoryCost() const {
    return num_buckets_ * sizeof(buckets_[0]);
  }
  
  // For the associative cuckoo table, check all of the slots in
  // the bucket to see if the key is present.
  inline bool FindInBucket(const K &k, const Bucket &bucket, V &out) const {
    for (char s = 0; s < kSlotsPerBucket; s++) {
      if ((bucket.occupiedMask & (1U << s)) && (bucket.keys[s] == k)) {
        out = bucket.values[s];
        return true;
      }
    }
    return false;
  }
  
  inline char FindFreeSlot(uint32_t bucket) const {
    return FindFreeSlot(buckets_[bucket]);
  }
  
  //  returns either -1 or the index of an
  //  available slot (0 <= slot < kSlotsPerBucket)
  inline char FindFreeSlot(const Bucket &bucket) const {
    for (char i = 0; i < kSlotsPerBucket; i++) {
      if (!(bucket.occupiedMask & (1U << i))) {
        return i;
      }
    }
    return -1;
  }
  
  inline OthelloUpdateResult registerKey(const K &k, uint8_t bi, bool online) {
    if (locator.isMember(k)) {
      if (locator.lookUp(k) == bi) return {};
      return locator.changeValue(k, bi);
    }
    
    return locator.insert(k, bi, online);
  }
  
  inline void unregisterKey(const K &k) {
    locator.remove(k);
  }
  
  inline OthelloUpdateResult toggleKey(const K &k) {
    return locator.toggleInOthello(k);
  }
  
  // For the associative cuckoo table, check all of the slots in
  // the bucket to see if the key is present.
  inline bool RemoveInBucket(const K &k, Bucket &bucket) {
    for (char i = 0; i < kSlotsPerBucket; i++) {
      if ((bucket.occupiedMask & (1U << i)) && bucket.keys[i] == k) {
        bucket.occupiedMask ^= 1U << i;
        
        unregisterKey(k);
        return true;
      }
    }
    return false;
  }
  
  inline bool putItem(const K &k, const V &v, uint32_t b, uint8_t slot, bool toFirstBucket,
                      vector<PathEntry> *const path = 0) {
    OthelloUpdateResult result = registerKey(k, 1 - toFirstBucket, !!path);
    if (result.status < 0) {  // need rebuild and don't allow rebuild
      return false;
    }
    
    Bucket &bucket = buckets_[b];
    
    bucket.keys[slot] = k;
    bucket.values[slot] = v;
    bucket.occupiedMask |= 1U << slot;
    uint8_t toSlot[4];
    uint8_t seed = updateSeed(b, path ? toSlot : nullptr, slot);
    
    if (path) {
      uint8_t oldSeed = buckets_[b].seed;
      const vector<uint32_t> cc = result.xorTemplate ? locator.getHalfTree(k, result.xorTemplate > 0, false) : vector<uint32_t>();
      
      PathEntry entry = {result.status ? -2 + toFirstBucket : (int) cc.size(),
                         b, uint8_t(FastHasher64<K>(seed)(k) >> 62), seed,
                         toSlot[0], toSlot[1], toSlot[2], toSlot[3],
                         cc};
      
      path->push_back(entry);
    }
    return true;
  }
  
  inline void moveItem(uint32_t sBkt, uint8_t sSlot, uint32_t dBkt, uint8_t dSlot, vector<PathEntry> *const path = 0) {
    if(full_debug) Clocker::count("Cuckoo copy item");
    Bucket &dst_bucket = buckets_[dBkt];
    Bucket &src_bucket = buckets_[sBkt];
    
    dst_bucket.keys[dSlot] = src_bucket.keys[sSlot];
    dst_bucket.values[dSlot] = src_bucket.values[sSlot];
    K &k = dst_bucket.keys[dSlot];
    OthelloUpdateResult result = toggleKey(k);
    assert(result.status == 0);
    
    uint8_t toSlot[4];
    uint8_t seed = updateSeed(dBkt, path ? toSlot : nullptr, dSlot);
    
    if (path) {
      uint8_t oldSeed = buckets_[dBkt].seed;
      const vector<uint32_t> vector = locator.getHalfTree(k, result.xorTemplate > 0, false);
      
      PathEntry entry = {(int) vector.size(),
                         dBkt, uint8_t(FastHasher64<K>(seed)(k) >> 62), seed,
                         toSlot[0], toSlot[1], toSlot[2], toSlot[3]};
      
      entry.locatorCC = vector;
      path->push_back(entry);
    }
  }
  
  uint8_t updateSeed(uint32_t bktIdx, uint8_t *dpSlotMove = 0, char slotWithNewKey = -1) {
    Bucket &bucket = buckets_[bktIdx];
    FastHasher64<K> h;
    bool occupied[4];
    
    for (uint8_t seed = 0; seed < 255; ++seed) {
      *(uint32_t *) occupied = 0U;
      h.setSeed(seed);
      
      bool success = true;
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (bucket.occupiedMask & (1 << slot)) {
          uint8_t i = uint8_t(h(bucket.keys[slot]) >> 62);
          if (occupied[i]) {
            success = false;
            break;
          } else { occupied[i] = true; }
        }
      }
      
      if (success) {
        bool withDp = dpSlotMove != nullptr;
        
        if (withDp) {
          FastHasher64<K> oldH(bucket.seed);
          
          memset(dpSlotMove, -1, 4);
          for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
            if ((bucket.occupiedMask & (1 << slot)) && slot != slotWithNewKey) {
              uint8_t oldSlot = uint8_t(oldH(bucket.keys[slot]) >> 62);
              uint8_t toSlot = uint8_t(h(bucket.keys[slot]) >> 62);
              
              dpSlotMove[oldSlot] = toSlot;
            }
          }
          
          *(uint32_t *) occupied = 0U;
          for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
            if (dpSlotMove[slot] != uint8_t(-1)) {
              occupied[dpSlotMove[slot]] = true;
            }
          }
          
          char firstUnusedNewSlot = -1;
          for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
            if (!occupied[slot]) {
              firstUnusedNewSlot = slot;
              break;
            }
          }
          
          for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
            if (dpSlotMove[slot] == uint8_t(-1)) {
              assert(firstUnusedNewSlot >= 0);
              dpSlotMove[slot] = uint8_t(firstUnusedNewSlot);
            }
          }
        }
        
        if(full_debug) Clocker::countMax("Ludo max seed", seed);
        bucket.seed = seed;
        
        return seed;
      }
    }
    
    throw runtime_error("Cannot generate a proper hash seed within 255 tries, which is rare");
  }
  
  Bucket getDpBucket(uint32_t index) const {
    const Bucket cpBucket = buckets_[index];
    
    Bucket result;
    result.occupiedMask = 0;
    result.seed = cpBucket.seed;
    FastHasher64<K> h(result.seed);
    
    for (char cpSlot = 0; cpSlot < 4; ++cpSlot) {
      if (!(cpBucket.occupiedMask & (1ULL << cpSlot))) continue;
      
      char dpSlot = h(cpBucket.keys[cpSlot]) >> 62;
      result.occupiedMask |= 1 << dpSlot;
      result.values[dpSlot] = cpBucket.values[cpSlot];
    }
    
    return result;
  }
  
  // Insert uses the BFS optimization (search before moving) to reduce
  // the number of cache lines dirtied during search.
  /// @return cuckoo path if rememberPath is true. or {1} to indicate success and {} to indicate fail.
  inline UpdateResult CuckooInsert(const K &k, const V &v, bool online) {
    UpdateResult result;
    
    int visited_end = -1;
    cpq_.reset();
    
    {
      uint32_t buckets[2];
      fast_map_to_buckets(h(k), buckets, buckets_.size());
      
      for (uint32_t b : buckets) {
        cpq_.push_back({b, 1, -1, -1}); // Note depth starts at 1.
        if(full_debug) Clocker::count("Cuckoo see bucket");
      }
    }
    
    while (!cpq_.empty()) {
      CuckooPathEntry entry = cpq_.pop_front();
      if(full_debug) Clocker::count("Cuckoo visit bucket");
      char free_slot = FindFreeSlot(entry.bucket);
      if (free_slot != -1) {
        if(full_debug) Clocker::count("Cuckoo total depth", entry.depth);
        if(full_debug) Clocker::countMax("Cuckoo max depth", entry.depth);
        
        // found a free slot in this path. just insert and follow this path
        buckets_[entry.bucket].occupiedMask |= 1U << free_slot;
        
        bool toFirstBucket = false;
        while (entry.depth > 1) {
          // "copy" instead of "swap" because one entry is always zero.
          // After, write target key/value over top of last copied entry.
          CuckooPathEntry parent = visited_[entry.parent];
          if (entry.depth == 2) toFirstBucket = entry.parent == 0;
          
          moveItem(parent.bucket, entry.parent_slot, entry.bucket, free_slot, online ? &result.path : nullptr);
          
          free_slot = entry.parent_slot;
          entry = parent;
        }
        
        bool succ = putItem(k, v, entry.bucket, free_slot, toFirstBucket, online ? &result.path : nullptr);
        if (!succ) result.status = -1;
        return result;
      } else if (entry.depth < kMaxBFSPathLen) {
        visited_[++visited_end] = entry;
        auto parent_index = visited_end;
        
        // Don't always start with the same slot, to even out the path depth.
        char start_slot = (entry.depth + entry.bucket) % kSlotsPerBucket;
        const Bucket &bucket = buckets_[entry.bucket];
        
        for (char i = 0; i < kSlotsPerBucket; i++) {
          char slot = (start_slot + i) % kSlotsPerBucket;
          
          uint32_t buckets[2];
          fast_map_to_buckets(h(bucket.keys[slot]), buckets, buckets_.size());
          
          for (char j = 0; j < 2; ++j) {  // maybe two buckets are both tested, because the key is in a wrong place!
            if (buckets[j] == entry.bucket) continue;
            
            cpq_.push_back({buckets[j], entry.depth + 1, parent_index, slot});
            if(full_debug) Clocker::count("Cuckoo reach bucket");
          }
        }
      }
    }
    
    if (online) {
      return {-2};
    } else {
      resizeCapacity(nKeys + 1, true);
      result = insert(k, v, false);
      result.status = 2;
      return result;
    }
  }
  
  bool build() {
    while (true) {
      int tryCount = 0;
      
      bool built;
      do {
        h.setSeed((uint64_t(rand()) << 32U) | rand());
        tryCount++;
        if (tryCount > 2 && !(tryCount & (tryCount - 1))) {
          cout << "Try #" << tryCount << endl;
        }
        built = tryBuild();
      } while ((!built) && (tryCount < MAX_REHASH));
      
      if (built) {
        break;
      } else {
        resizeCapacity(capacity + 1, true);
      }
    }
    
    checkIntegrity();
    
    return true;
  }
  
  /// Begin a new build
  /// Side effect: 1) discard all memory except keys and values. 2) build fail, or
  /// all the values and disjoint set are properly set
  bool tryBuild() {
    #ifdef PROFILE
    if(full_debug) Clocker rebuild("Ludo try rebuild");
    #else
    cout << "Ludo try rebuild" << endl;
    #endif
    
    locator.clear();
    
    nKeys = 0;
    for (auto &bucket: buckets_) {  // all buckets
      bucket.occupiedMask = 0;
    }
    
    bool succ = true;
    
    for (auto &bucket: oldBuckets) {  // all buckets
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (bucket.occupiedMask & (1 << slot)) {
          UpdateResult result = insert(bucket.keys[slot], bucket.values[slot], false);   // rebuilding is always offline
          if (result.status < 0)
            succ = false;
        }
      }
      if (!succ) break;
    }
    
    return (integrity = succ);
  }
  
  
  // Constants for BFS cuckoo path search:
  // The visited list must be maintained for all but the last level of search
  // in order to trace back the path. The BFS search has two roots
  // and each can go to a total depth (including the root) of 5.
  // The queue must be sized for 4 * \sum_{k=0...4}{(3*kSlotsPerBucket)^k}.
  // The visited queue, however, does not need to hold the deepest level,
  // and so it is sized 4 * \sum{k=0...3}{(3*kSlotsPerBucket)^k}
  static constexpr int calMaxQueueSize() {
    int result = 0;
    int term = 4;
    for (int i = 0; i < kMaxBFSPathLen; ++i) {
      result += term;
      term *= ((2 - 1) * kSlotsPerBucket);
    }
    return result;
  }
  
  static constexpr int calVisitedListSize() {
    int result = 0;
    int term = 4;
    for (int i = 0; i < kMaxBFSPathLen - 1; ++i) {
      result += term;
      term *= ((2 - 1) * kSlotsPerBucket);
    }
    return result;
  }
  
  static constexpr int kMaxQueueSize = calMaxQueueSize();
  static constexpr int kVisitedListSize = calVisitedListSize();
  
  struct CuckooPathEntry {
    uint32_t bucket;
    int depth;
    int parent;      // To index in the visited array.
    int parent_slot; // Which slot in our parent did we come from?  -1 == root.
  };
  
  // CuckooPathQueue is a trivial circular queue for path entries.
  // The caller is responsible for not inserting more than kMaxQueueSize
  // entries.  Each PresizedHeadlessCuckoo has one (heap-allocated) CuckooPathQueue
  // that it reuses across inserts.
  class CuckooPathQueue {
  public:
    CuckooPathQueue()
        : head_(0), tail_(0) {
    }
    
    void push_back(CuckooPathEntry e) {
      queue_[tail_] = e;
      tail_ = (tail_ + 1) % kMaxQueueSize;
    }
    
    CuckooPathEntry pop_front() {
      CuckooPathEntry &e = queue_[head_];
      head_ = (head_ + 1) % kMaxQueueSize;
      return e;
    }
    
    bool empty() const {
      return head_ == tail_;
    }
    
    bool full() const {
      return ((tail_ + 1) % kMaxQueueSize) == head_;
    }
    
    void reset() {
      head_ = tail_ = 0;
    }
  
  private:
    CuckooPathEntry queue_[kMaxQueueSize];
    int head_;
    int tail_;
  };
  
  CuckooPathQueue cpq_;
  CuckooPathEntry visited_[kVisitedListSize];
  bool integrity = true;
};

template<class K, class V, uint VL = sizeof(V) * 8>
class DataPlaneLudo : public LudoCommon<K, V, VL> {
public:
  using LudoCommon<K, V, VL>::kSlotsPerBucket;
  using LudoCommon<K, V, VL>::kLoadFactor;
  using LudoCommon<K, V, VL>::kMaxBFSPathLen;
  
  using LudoCommon<K, V, VL>::VDMask;
  using LudoCommon<K, V, VL>::ValueMask;
  
  using LudoCommon<K, V, VL>::fast_map_to_buckets;
  using LudoCommon<K, V, VL>::multiply_high_u32;
  
  using LudoCommon<K, V, VL>::LocatorSeedLength;
  using LudoCommon<K, V, VL>::num_buckets_;
  using LudoCommon<K, V, VL>::h;
  using LudoCommon<K, V, VL>::digestH;
  
  typedef Ludo_PathEntry<K> PathEntry;
  
  struct Bucket {  // only as parameters and return values for easy access. the storage is compact.
    uint8_t seed;
    V values[kSlotsPerBucket];
    
    bool operator==(const Bucket &other) const {
      if (seed != other.seed) return false;
      for (char s = 0; s < kSlotsPerBucket; s++) {
        if (values[s] != other.values[s]) return false;
      }
      return true;
    }
    
    bool operator!=(const Bucket &other) const {
      return !(*this == other);
    }
  };
//  static const uint8_t bucketLength = sizeof(Bucket);
  
  vector<Bucket> buckets;
  DataPlaneOthello<K, uint8_t, 1> locator;
  vector<uint8_t> lock = vector<uint8_t>(8192, 0);
  
  virtual void setSeed(uint32_t s) {
    LudoCommon<K, V, VL>::setSeed(s);
    
    locator.hab.setSeed(rand() | (uint64_t(rand()) << 32));
    locator.hd.setSeed(rand());
  }
  
  DataPlaneLudo() {}
  
  explicit DataPlaneLudo(const ControlPlaneLudo<K, V, VL> &cp) : locator(cp.locator) {
    h = cp.h;
    digestH = cp.digestH;
    num_buckets_ = cp.num_buckets_;
    resetMemory();
    
    for (uint32_t bktIdx = 0; bktIdx < num_buckets_; ++bktIdx) {
      const typename ControlPlaneLudo<K, V, VL>::Bucket &cpBucket = cp.buckets_[bktIdx];
      Bucket &dpBucket = buckets[bktIdx];
      dpBucket.seed = cpBucket.seed;
      memset(dpBucket.values, 0, kSlotsPerBucket * sizeof(V));
      
      const FastHasher64<K> locateHash(cpBucket.seed);
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (cpBucket.occupiedMask & (1U << slot)) {
          const K &k = cpBucket.keys[slot];
          dpBucket.values[locateHash(k) >> 62] = cpBucket.values[slot];
        }
      }
    }
  }
  
  template<class V2>
  DataPlaneLudo(const ControlPlaneLudo<K, V2, VL> &cp, unordered_map<V2, V> m): locator(cp.locator) {
    h = cp.h;
    digestH = cp.digestH;
    num_buckets_ = cp.num_buckets_;
    
    resetMemory();
    
    for (uint32_t bktIdx = 0; bktIdx < num_buckets_; ++bktIdx) {
      const typename ControlPlaneLudo<K, V2, VL>::Bucket &cpBucket = cp.buckets_[bktIdx];
      Bucket &dpBucket = buckets[bktIdx];
      dpBucket.seed = cpBucket.seed;
      memset(dpBucket.values, 0, kSlotsPerBucket * sizeof(V));
      
      const FastHasher64<K> locateHash(cpBucket.seed);
      
      for (char slot = 0; slot < kSlotsPerBucket; ++slot) {
        if (cpBucket.occupiedMask & (1U << slot)) {
          const K &k = cpBucket.keys[slot];
          dpBucket.values[locateHash(k) >> 62] = m[cpBucket.values[slot]];
        }
      }
    }
  }
  
  // will clear all entries. because it is only used at initialization
  void resizeCapacity(uint32_t targetCapacity) {
    targetCapacity = max(targetCapacity, 256U);
    
    uint64_t nextNbuckets = 64U;
    uint64_t nextCapacity = nextNbuckets * kLoadFactor * kSlotsPerBucket;
    for (; nextCapacity < targetCapacity; nextCapacity = nextNbuckets * kLoadFactor * kSlotsPerBucket)
      nextNbuckets <<= 1U;
    
    num_buckets_ = nextNbuckets;
    resetMemory();
    
    locator.resizeCapacity(nextCapacity);
  }
  
  inline void resetMemory() {
    buckets.resize(num_buckets_);
  }
  
  inline V lookUp(const K &k) const {
    V out;
    if (!lookUp(k, out)) throw runtime_error("key does not exist");
    return out;
  }
  
  // Returns true if found.  Sets *out = value.
  inline bool lookUp(const K &k, V &out) const {
    uint32_t bktId[2];
    fast_map_to_buckets(h(k), bktId, num_buckets_);
    
    while (true) {
      uint8_t va1 = lock[bktId[0] & 8191], vb1 = lock[bktId[1] & 8191];
      COMPILER_BARRIER();
      if (va1 % 2 == 1 || vb1 % 2 == 1) continue;
      
      const Bucket &bucket = buckets[bktId[locator.lookUp(k)]];
      
      COMPILER_BARRIER();
      uint8_t va2 = lock[bktId[0] & 8191], vb2 = lock[bktId[1] & 8191];
      
      if (va1 != va2 || vb1 != vb2) continue;
      
      uint64_t i = FastHasher64<K>(bucket.seed)(k) >> 62;
      out = bucket.values[i];
      
      return true;
    }
  }
  
  inline void applyInsert(const vector<PathEntry> &path, V value) {
    for (int i = 0; i < path.size(); ++i) {
      PathEntry entry = path[i];
      Bucket bucket = buckets[entry.bid];
      bucket.seed = entry.newSeed;
      
      uint8_t toSlots[] = {entry.s0, entry.s1, entry.s2, entry.s3};
      
      V buffer[4];       // solve the permutation is slow. just copy the 4 elements
      for (char s = 0; s < 4; ++s) {
        buffer[s] = bucket.values[s];
      }
      
      for (char s = 0; s < 4; ++s) {
        bucket.values[toSlots[s]] = buffer[s];
      }
      
      if (i + 1 == path.size()) {  // put the new value
        bucket.values[entry.sid] = value;
      } else {  // move key from another bucket and slot to this bucket and slot
        PathEntry from = path[i + 1];
        uint8_t tmp[4] = {from.s0, from.s1, from.s2, from.s3};
        uint8_t sid;
        for (uint8_t ii = 0; ii < 4; ++ii) {
          if (tmp[ii] == from.sid) {
            sid = ii;
            break;
          }
        }
        bucket.values[entry.sid] = buckets[from.bid].values[sid];
      }
      
      lock[entry.bid & 8191]++;
      COMPILER_BARRIER();
      
      if (entry.locatorCC.size()) {
        locator.fixHalfTreeByConnectedComponent(entry.locatorCC, 1);
      }
      
      buckets[entry.bid] = bucket;
      
      COMPILER_BARRIER();
      lock[entry.bid & 8191]++;
    }
  }
  
  inline void applyUpdate(uint32_t bid, uint8_t sid, V val) {
    lock[bid & 8191]++;
    COMPILER_BARRIER();
  
    
    buckets[bid].values[sid] = val;
    
    COMPILER_BARRIER();
    lock[bid & 8191]++;
  }
  
  inline void applyUpdate(uint32_t bs, V val) {
    uint32_t bid = bs >> 2;
    uint8_t sid = bs & 3;
    
    applyUpdate(bid, sid, val);
  }
  
  inline uint64_t getMemoryCost() const {
    return buckets.size() * sizeof(Bucket);
  }
};
