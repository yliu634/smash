#pragma once

#include "../common.h"

using namespace std;

template<class K, class V, uint8_t L, uint8_t DL>
class DataPlaneOthello;

template<class K, class V, uint8_t L, uint8_t DL,
    bool maintainDP, bool maintainDisjointSet, bool randomized>
class ControlPlaneOthello;

struct OthelloCPCell {
  uint32_t keyId;
  uint32_t nodeId;
};

// Insertion into an Othello may:
// success (0). othello need rebuild, and is done (1)// and is not permitted (-1)
// othello need enlarge, and is done (2)// and is not permitted (-2)
struct OthelloUpdateResult {
  char status = 0; // 0: succ. 1: updated after a rebuild. -1: need rebuild. 2: updated after enlarge. -2: need enlarge
  int64_t xorTemplate = 0; // > 0 starting from the A node. < 0 from the B node. == 0 doesn't matter because no update
  int marks[2] = {-1, -1};
  vector<uint32_t> cc;
};

template<class K, class V, uint8_t L = sizeof(V) * 8, uint8_t DL = 0>
class OthelloCommon {
public:
  //*******builtin values
  const static uint8_t VDL = L + DL;
  static_assert(
      VDL <= 60, "Value is too long. You should consider another solution to avoid space waste. ");
  const static uint64_t VDEMASK = ~(uint64_t(-1) << VDL);   // lower VDL bits are 1, others are 0
  const static uint64_t DEMASK = ~(uint64_t(-1) << DL);   // lower DL bits are 1, others are 0
  const static uint64_t VMASK = ~(uint64_t(-1) << L);   // lower L bits are 1, others are 0
  const static uint64_t VDMASK = (VDEMASK << 1U) & VDEMASK; // [1, VDL) bits are 1
  
  vector<uint64_t> mem;        // memory space for array A and array B. All elements are stored compactly into consecutive uint64_t
  uint32_t ma = 0;               // number of elements of array A
  uint32_t mb = 0;               // number of elements of array B
  Hasher64<K> hab;          // hash function Ha
  Hasher32<K> hd;
  
  inline uint32_t multiply_high_u32(uint32_t x, uint32_t y) const {
    return (uint32_t) (((uint64_t) x * (uint64_t) y) >> 32U);
  }
  
  inline uint64_t fast_map_to_A(uint32_t x) const {
    // Map x (uniform in 2^64) to the range [0, num_buckets_ -1]
    // using Lemire's alternative to modulo reduction:
    // http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    // Instead of x % N, use (x * N) >> 64.
    return multiply_high_u32(x, ma);
  }
  
  inline uint64_t fast_map_to_B(uint32_t x) const {
    return multiply_high_u32(x, mb);
  }

/// \param k
/// \return ma + the index of k into array B
  inline void getIndices(const K &k, uint32_t &aInd, uint32_t &bInd) const {
    uint64_t hash = hab(k);
    bInd = fast_map_to_B(hash >> 32U) + ma;
    aInd = fast_map_to_A(hash);
  }

/// Set the index-th element to be value. if the index > ma, it is the (index - ma)-th element in array B
/// \param index in array A or array B
/// \param value
  inline void memSet(uint32_t index, uint64_t value) {
    if (VDL == 0) return;
    
    uint64_t v = uint64_t(value) & VDEMASK;
    
    uint64_t i = (uint64_t) index * VDL;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    char left = char(offset + VDL - 64);
    
    uint64_t mask = ~(VDEMASK << offset); // [offset, offset + VDL) should be 0, and others are 1
    
    mem[start] &= mask;
    mem[start] |= v << offset;
    
    if (left > 0) {
      mask = uint64_t(-1) << left;     // lower left bits should be 0, and others are 1
      mem[start + 1] &= mask;
      mem[start + 1] |= v >> (VDL - left);
    }
  }

/// \param index in array A or array B
/// \return the index-th element. if the index > ma, it is the (index - ma)-th element in array B
  inline uint64_t memGet(uint32_t index) const {
    if (VDL == 0) return 0;
    
    uint64_t i = (uint64_t) index * VDL;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    
    char left = char(offset + VDL - 64);
    left = char(left < 0 ? 0 : left);
    
    uint64_t mask = ~(uint64_t(-1) << (VDL - left));   // lower VDL-left bits should be 1, and others are 0
    uint64_t result = (mem[start] >> offset) & mask;
    
    if (left > 0) {
      mask = ~(uint64_t(-1) << left);     // lower left bits should be 1, and others are 0
      result |= (mem[start + 1] & mask) << (VDL - left);
    }
    
    return result;
  }
  
  inline void memValueSet(uint32_t index, uint64_t value) {
    if (L == 0) return;
    
    uint64_t v = uint64_t(value) & VMASK;
    
    uint64_t i = (uint64_t) index * VDL + DL;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    char left = char(offset + L - 64);
    
    uint64_t mask = ~(VMASK << offset); // [offset, offset + L) should be 0, and others are 1
    
    mem[start] &= mask;
    mem[start] |= v << offset;
    
    if (left > 0) {
      mask = uint64_t(-1) << left;     // lower left bits should be 0, and others are 1
      mem[start + 1] &= mask;
      mem[start + 1] |= v >> (L - left);
    }
  }
  
  inline uint64_t memValueGet(uint32_t index) const {
    if (L == 0) return 0;
    
    uint64_t i = (uint64_t) index * VDL + DL;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    char left = char(offset + L - 64);
    left = char(left < 0 ? 0 : left);
    
    uint64_t mask = ~(uint64_t(-1)
        << (L - left));     // lower L-left bits should be 1, and others are 0
    uint64_t result = (mem[start] >> offset) & mask;
    
    if (left > 0) {
      mask = ~(uint64_t(-1) << left);     // lower left bits should be 1, and others are 0
      result |= (mem[start + 1] & mask) << (L - left);
    }
    
    return result;
  }
  
  inline bool isEmpty(int index) {
    return !(memGet(index) & 1);
  }
  
  virtual V lookUp(const K &k) const = 0;
};

/**
 * Describes the data structure *l-Othello*. It classifies keys of *keyType* into *2^L* classes.
 * The array are all stored in an array of uint64_t. There are actually m_a+m_b cells in this array, each of length L.
 * \note Be VERY careful!!!! valueType must be some kind of int with no more than 8 bytes' length
 */
template<class K, class V, uint8_t L = sizeof(V) * 8, uint8_t DL = 0>
class DataPlaneOthello : public OthelloCommon<K, V, L, DL> {
public:
  using OthelloCommon<K, V, L, DL>::VDL;
  using OthelloCommon<K, V, L, DL>::VMASK;
  using OthelloCommon<K, V, L, DL>::VDMASK;
  using OthelloCommon<K, V, L, DL>::VDEMASK;
  using OthelloCommon<K, V, L, DL>::DEMASK;
  using OthelloCommon<K, V, L, DL>::memValueSet;
  using OthelloCommon<K, V, L, DL>::memValueGet;
  using OthelloCommon<K, V, L, DL>::memSet;
  using OthelloCommon<K, V, L, DL>::memGet;
  using OthelloCommon<K, V, L, DL>::ma;
  using OthelloCommon<K, V, L, DL>::mb;
  using OthelloCommon<K, V, L, DL>::mem;
  using OthelloCommon<K, V, L, DL>::hab;
  using OthelloCommon<K, V, L, DL>::hd;
  using OthelloCommon<K, V, L, DL>::getIndices;
  using OthelloCommon<K, V, L, DL>::fast_map_to_A;
  using OthelloCommon<K, V, L, DL>::fast_map_to_B;
  
  vector<uint8_t> lock = vector<uint8_t>(8192, 0);
  vector<uint8_t> versions;
  
  DataPlaneOthello() {}
  
  // will clear all entries. because it is only used at initialization
  void resizeCapacity(uint32_t targetCapacity, bool compact = true) {
    targetCapacity = max(targetCapacity, 256U);
    
    uint64_t nextMb;
    
    if (compact) {
      nextMb = targetCapacity;
    } else {
      nextMb = 256U;
      while (nextMb < targetCapacity)
        nextMb <<= 1U;
    }
    uint64_t nextMa = uint64_t(1.33334 * nextMb);
    
    if (nextMa > ma || nextMa < 0.8 * ma) {
      ma = nextMa;// this breaks othello integrity
      mb = nextMb;
      
      mem.resize((((uint64_t) ma + mb) * VDL + 63) / 64);
    }
  }
  
  inline void memSet(uint32_t index, uint64_t value) {
    lock[index & 8191U]++;
    OthelloCommon<K, V, L, DL>::memSet(index, value);
    lock[index & 8191U]++;
  }
  
  inline void memValueSet(uint32_t index, uint64_t value) {
    lock[index & 8191U]++;
    COMPILER_BARRIER();
    OthelloCommon<K, V, L, DL>::memValueSet(index, value);
    COMPILER_BARRIER();
    lock[index & 8191U]++;
  }
  
  template<bool keepDigest = false>
  inline void fillSingle(uint32_t valueToFill, uint32_t nodeToFill) {
    if (keepDigest) {
      memValueSet(nodeToFill, valueToFill);
    } else {
      memSet(nodeToFill, valueToFill);
    }
  }
  
  inline void setTaken(uint32_t nodeIndex) {
    if (DL) {
      memSet(nodeIndex, memGet(nodeIndex) | 1);
    }
  }
  
  inline void setEmpty(uint32_t nodeIndex) {
    if (DL) {
      memSet(nodeIndex, memGet(nodeIndex) & ~uint64_t(1));
    }
  }
  
  template<bool keepDigest = false>
  /// fix the value and index at single node by xoring x
  /// \param x the xor'ed number
  inline void fixSingle(uint32_t nodeToFix, uint64_t x) {
    if (keepDigest) {
      uint64_t valueToFill = x ^memValueGet(nodeToFix);
      memValueSet(nodeToFix, valueToFill);
    } else {
      uint64_t valueToFill = x ^memGet(nodeToFix);
      memSet(nodeToFix, valueToFill);
    }
  }
  
  /// Fix the values of a connected tree starting at the root node and avoid searching keyId
  /// Assume:
  /// 1. the value of root is not properly set before the function call
  /// 2. the values are in the value array
  /// 3. the root is always from array A
  /// Side effect: all node in this tree is set and if updateToFilled
  inline void fixHalfTreeByConnectedComponent(vector<uint32_t> indices, uint32_t xorTemplate) {
    for (uint32_t index: indices) {
      fixSingle<true>(index, xorTemplate);
    }
  }
  
  /// \param k
  /// \param v the lookup value for k
  /// \return the lookup is successfully passed the digest match, but it does not mean the key is really a member
  inline bool lookUp(const K &k, V &v) const {
    uint32_t ha, hb;
    getIndices(k, ha, hb);
    
    while (true) {
      uint8_t va1 = lock[ha & 8191], vb1 = lock[hb & 8191];
      COMPILER_BARRIER();
      
      if (va1 % 2 == 1 || vb1 % 2 == 1) continue;
      
      uint64_t aa = memGet(ha);
      uint64_t bb = memGet(hb);
      
      COMPILER_BARRIER();
      uint8_t va2 = lock[ha & 8191], vb2 = lock[hb & 8191];
      
      if (va1 != va2 || vb1 != vb2) continue;
      
      ////printf("%llx   [%x] %x ^ [%x] %x = %x\n", k,ha,aa&LMASK,hb,bb&LMASK,(aa^bb)&LMASK);
      uint64_t vd = aa ^bb;
      
      v = vd >> DL;  // extract correct v
      
      if (DL == 0) return true;      // no filter features
      
      if ((aa & 1) == 0 || (bb & 1) == 0) {
        return false;
      }     // with filter features, then the last bit must be 1
      
      if (DL == 1) return true;  // shortcut for one bit digest
      
      uint32_t digest = uint32_t(vd & DEMASK);
      return (digest | 1) == ((hd(k) & DEMASK) | 1);        // ignore the last bit
    }
  }
  
  inline V lookUp(const K &k) const {
    V result;
    bool success = lookUp(k, result);
    if (success) return result;
    
    throw runtime_error("No matched key! ");
  }
  
  template<bool maintainDP, bool maintainDisjointSet, bool randomized>
  explicit DataPlaneOthello(const ControlPlaneOthello<K, V, L, DL, maintainDP, maintainDisjointSet, randomized> &cp) {
    fullSync(cp);
    
    #ifndef NDEBUG
    for (int i = 0; i < cp.nKeysInOthello; ++i) {
      auto &k = cp.keys[i];
      V out;
      assert(cp.lookUp(k, out) && (out == lookUp(k)));
    }
    #endif
    
    versions.resize(ma + mb);
  }
  
  template<bool maintainDisjointSet, bool maintainDP, bool randomized>
  void fullSync(const ControlPlaneOthello<K, V, L, DL, maintainDP, maintainDisjointSet, randomized> &cp) {
    (const_cast<ControlPlaneOthello<K, V, L, DL, maintainDP, maintainDisjointSet, randomized> &>(cp)).prepareDP();
    ma = cp.ma;
    mb = cp.mb;
    hab = cp.hab;
    hd = cp.hd;
    mem = cp.mem;
  }
  
  void fullSync(const DataPlaneOthello<K, V, L, DL> &dp) {
    ma = dp.ma;
    mb = dp.mb;
    hab = dp.hab;
    hd = dp.hd;
    mem = dp.mem;
  }
  
  virtual uint64_t getMemoryCost() const {
    return mem.size() * sizeof(mem[0]);
  }
};

/**
 * Control plane Othello can track connections (Add [amortized], Delete, Membership Judgment) in O(1) time,
 * and can iterate on the keys in exactly n elements.
 *
 * Implementation: just add an array indMem to be maintained. always ensure that registered keys can
 * be queried to get the index of it in the keys array
 *
 * How to ensure:
 * add to tail when add, and store the value as well as the index to othello
 * when delete, move key-value and update corresponding index
 *
 * @note
 *  The valueType must be compatible with all int operations
 *
 *  If you wish to export the control plane to a data plane lookUp structure at a fast speed and at any time, then
 *  set willExport to true. Additional computation and memory overheads will apply on insert, while lookups will be faster.
 *
 *  If you wish to maintain the disjoint set, the insertion will become faster but the deletion is slower, in the sense that
 *  memory accesses are more expensive than computation
 */
template<class K, class V, uint8_t L = sizeof(V) * 8, uint8_t DL = 0,
    bool maintainDP = false, bool maintainDisjointSet = true, bool randomized = false>
class ControlPlaneOthello : public OthelloCommon<K, V, L, DL> {
public:
  const static uint8_t MAX_REHASH = 5; //!< Maximum number of rehash tries before report an error. If this limit is reached, Othello build fails.
  
  using OthelloCommon<K, V, L, DL>::VDL;
  using OthelloCommon<K, V, L, DL>::VMASK;
  using OthelloCommon<K, V, L, DL>::VDMASK;
  using OthelloCommon<K, V, L, DL>::VDEMASK;
  using OthelloCommon<K, V, L, DL>::DEMASK;
  using OthelloCommon<K, V, L, DL>::memValueSet;
  using OthelloCommon<K, V, L, DL>::memValueGet;
  using OthelloCommon<K, V, L, DL>::memSet;
  using OthelloCommon<K, V, L, DL>::memGet;
  using OthelloCommon<K, V, L, DL>::ma;
  using OthelloCommon<K, V, L, DL>::mb;
  using OthelloCommon<K, V, L, DL>::mem;
  using OthelloCommon<K, V, L, DL>::hab;
  using OthelloCommon<K, V, L, DL>::hd;
  using OthelloCommon<K, V, L, DL>::getIndices;
  using OthelloCommon<K, V, L, DL>::isEmpty;
  
  bool maintainingDP = maintainDP;  // if maintainDP, then always maintaining DP. if not maintainDP, then only maintain DP for export
  bool compact;
  
  bool integrity = true;
  
  /// \param k
  /// \param v the lookup value for k
  /// \return the lookup action is successful, but it does not mean the key is really a member
  /// \note No membership is checked. Use isMember to check the membership
  inline bool lookUp(const K &k, V &out) const {
    if (maintainingDP) {
      uint32_t ha, hb;
      getIndices(k, ha, hb);
      V aa = memGet(ha);
      V bb = memGet(hb);
      uint64_t vd = aa ^bb;
      out = vd >> DL;
    } else {
      uint32_t index = lookUpIndex(k);
      if (index >= values.size()) return false;
      out = values[index];
    }
    
    return true;
  }
  
  inline V lookUp(const K &k) const {  // for debugging
    V out;
    if (!lookUp(k, out)) throw runtime_error("Key absent");
    return out;
  }
  
  explicit ControlPlaneOthello(uint32_t keyCapacity = 1, bool compact = true, const vector<K> &keys = vector<K>(),
                               const vector<V> &values = vector<V>())
      : nKeysInOthello(min((uint32_t) min(keys.size(), values.size()), keyCapacity)),
        keys(keys.begin(), keys.begin() + nKeysInOthello), values(values.begin(), values.begin() + nKeysInOthello),
        compact(compact) {
    
    nextAtA.resize(nKeysInOthello);
    nextAtB.resize(nKeysInOthello);
    
    resizeCapacity(max(keyCapacity, nKeysInOthello), true);
  }
  
  /// Resize key and value related memory for the Othello to be able to hold keyCount keys
  /// \param targetCapacity the target capacity
  /// \note Side effect: will change nKeysInOthello, and if hash size is changed, a rebuild is performed
  void resizeCapacity(uint32_t targetCapacity, bool forceBuild = false, bool skipBuild = false) {
    targetCapacity = max(nKeysInOthello, max(targetCapacity, 256U));
    
    uint64_t nextMb;
    
    if (compact) {
      nextMb = targetCapacity;
    } else {
      nextMb = 256U;
      while (nextMb < targetCapacity)
        nextMb <<= 1U;
    }
    uint64_t nextMa = uint64_t(1.33334 * nextMb);
    
    if (nextMa + nextMb >= (1ULL << 32U)) {
      throw runtime_error("Too many elements. Othello index overflow! ");
    }
    
    if (targetCapacity > keys.size()) {
      uint32_t keyCntReserve = max(256U, targetCapacity * (compact ? 1 : 2));
      keys.resize(keyCntReserve);
      values.resize(keyCntReserve);
      nextAtA.resize(keyCntReserve);
      nextAtB.resize(keyCntReserve);
    }
    
    if (nextMa > ma || nextMa < 0.8 * ma) {
      // until here, the Othello is untouched
      integrity = false;
      
      ma = nextMa;// this breaks othello integrity
      mb = nextMb;
      
      mem.resize((((uint64_t) ma + mb) * VDL + 63) / 64);
      
      indMem.resize((uint64_t) ma + mb);
      head.resize((uint64_t) ma + mb);
      connectivityForest.resize((uint64_t) ma + mb);
      
      if (!skipBuild) {
        build();
      }
    } else if (forceBuild) {
      build();
    }
  }
  
  //****************************************
  //*************CONTROL plane
  //****************************************
  uint32_t nKeysInOthello = 0;
  
  void clear() {
    nKeysInOthello = 0;
  }
  
  void compose(const unordered_map<V, V> &migration) {  // TODO trigger update
    for (int i = 0; i < nKeysInOthello; ++i) {
      V &val = values[i];
      
      auto it = migration.find(val);
      if (it == migration.end()) {
        remove(keys[i]);
        --i;
      } else {
        V dst = it->second;
        val = dst;
      }
    }
    
    if (maintainingDP) {
      fillValue<true>();
    }
  }
  
  void prepareDP() {
    if (maintainDP) return;
    
    maintainingDP = true;
    fillValue();
    checkIntegrity();
    maintainingDP = false;
  }
  
  // ******input of control plane
  vector<K> keys;
  vector<V> values;
  vector<uint32_t> indMem;       // memory space for indices
  
  /*! multiple keys may share a same end (hash value)
   first and next1, next2 maintain linked lists,
   each containing all keys with the same hash in either of their ends
   */
  vector<OthelloCPCell> head;         //!< subscript: hashValue, value: keyIndex
  vector<OthelloCPCell> nextAtA;         //!< subscript: keyIndex, value: keyIndex
  vector<OthelloCPCell> nextAtB;         //! h2(keys[i]) = h2(keys[next2[i]]);
  mutable DisjointSet connectivityForest;                     //!< store the hash values that are connected by key edges
  
  inline uint32_t size() const {
    return nKeysInOthello;
  }
  
  inline bool isMember(const K &k) const {
    uint32_t index = lookUpIndex(k);
    return (index < nKeysInOthello && keys[index] == k);
  }
  
  /// Insert a key-value pair
  /// \return
  ///         cyclic add: 1 << VDL
  ///         acyclic add: the xor template:
  ///                                > 0 starting from the A node
  ///                                < 0 from the B node
  ///                                == 0 doesn't matter
  ///         if rebuilt is set to true, then the return value means nothing.
  inline OthelloUpdateResult insert(const K &k, V v, bool DoNotRebuild = false) {
    OthelloUpdateResult result;
    
    if (isMember(k)) {
      result = changeValue(k, v);
    } else if (nKeysInOthello >= keys.size() || nKeysInOthello >= mb) {
      if (DoNotRebuild) {
        result.status = -2;
      } else {
        resizeCapacity(nKeysInOthello * 2, true);
        result = insert(k, v, false);
        if (result.status < 0) throw runtime_error("impossible");
        result.status = 2;
      }
    } else {
      uint32_t ha, hb;
      getIndices(k, ha, hb);
      
      if (isConnectedDFS(ha, hb)) {
        if (DoNotRebuild) {
          result.status = -1;
        } else {
          keys[nKeysInOthello] = k;
          values[nKeysInOthello++] = v;
          build();
          result.status = 1;
        }
      } else {  // acyclic, just add
        keys[nKeysInOthello] = k;
        values[nKeysInOthello] = v;
        
        addEdge(nKeysInOthello, ha, hb);
        result.xorTemplate = fixHalfTreeDFS<maintainDP, true>(nKeysInOthello++, ha, hb);
        result.cc = getHalfTree(k, result.xorTemplate > 0, false);
      }
    }
    
    return result;
  }
  
  inline OthelloUpdateResult remove(const K &k, uint32_t keyId = -1) {
    if (keyId == uint32_t(-1)) {
      keyId = lookUpIndex(k);
    }
    
    if (keyId >= nKeysInOthello || !(keys[keyId] == k)) return {};
    
    uint32_t ha, hb;
    getIndices(k, ha, hb);
    nKeysInOthello--;
    
    // Delete the edge of keyId. By maintaining the linked lists on nodes ha and hb.
    OthelloCPCell headA = head[ha];
    if (headA.keyId == keyId) {
      head[ha] = nextAtA[keyId];
    } else {
      int t = headA.keyId;
      while (nextAtA[t].keyId != keyId)
        t = nextAtA[t].keyId;
      nextAtA[t] = nextAtA[keyId];
    }
    OthelloCPCell headB = head[hb];
    if (headB.keyId == keyId) {
      head[hb] = nextAtB[keyId];
    } else {
      int t = headB.keyId;
      while (nextAtB[t].keyId != keyId)
        t = nextAtB[t].keyId;
      nextAtB[t] = nextAtB[keyId];
    }
    
    // move the last to override current key-value
    if (keyId != nKeysInOthello) {
      const K &lastKey = keys[nKeysInOthello];
      keys[keyId] = lastKey;
      values[keyId] = values[nKeysInOthello];
      
      uint32_t hal, hbl;
      getIndices(lastKey, hal, hbl);
      
      // repair the broken linked list because of key movement
      nextAtA[keyId] = nextAtA[nKeysInOthello];
      if (head[hal].keyId == nKeysInOthello) {
        head[hal] = {keyId, hbl};
      } else {
        int t = head[hal].keyId;
        while (nextAtA[t].keyId != nKeysInOthello)
          t = nextAtA[t].keyId;
        nextAtA[t] = {keyId, hbl};
      }
      nextAtB[keyId] = nextAtB[nKeysInOthello];
      if (head[hbl].keyId == nKeysInOthello) {
        head[hbl] = {keyId, hal};
      } else {
        int t = head[hbl].keyId;
        while (nextAtB[t].keyId != nKeysInOthello)
          t = nextAtB[t].keyId;
        nextAtB[t] = {keyId, hal};
      }
      // update the mapped index
      fixHalfTreeDFS<false, true, true>(keyId, hal, hbl);
    }
    
    if (maintainDisjointSet) {
      // repair the disjoint set
      connectDFS(ha);
      connectDFS(hb);
    }
    
    checkIntegrity();
    return {0, 0, {isEmpty(ha) ? int(ha) : -1, isEmpty(hb) ? int(hb) : -1}};
  }
  
  inline OthelloUpdateResult toggleInOthello(const K &k) {
    static_assert(L == 1);
    assert(isMember(k));
    
    uint32_t keyId = lookUpIndex(k);
    
    values[keyId] = !values[keyId];
    
    if (maintainDP) {
      uint32_t ha, hb;
      getIndices(k, ha, hb);
      int64_t xorTemplate = fixHalfTreeDFS<true, false, true>(keyId, ha, hb);
      return {0, 1, {-1, -1}, getHalfTree(k, xorTemplate > 0, false)};
    }
    
    return {};
  }
  
  inline OthelloUpdateResult changeValue(const K &k, V val) {
    assert(isMember(k));
    
    OthelloUpdateResult result;
    result.xorTemplate = changeValueAt(lookUpIndex(k), val);
    if (result.xorTemplate) {  // else skip this step because it means nothing
      result.cc = getHalfTree(k, result.xorTemplate > 0, false);
    }
    return result;
  }
  
  /// try really hard to build, until success or tryCount >= MAX_REHASH
  ///
  /// Side effect: 1) discard all memory except keys and values. 2) build fail, or
  /// all the values and disjoint set are properly set
  bool build() {
    while (true) {
      int tryCount = 0;
      bool built;
      do {
        hab.setSeed((uint64_t(rand()) << 32U) | rand());
        tryCount++;
        if (tryCount > 2 && !(tryCount & (tryCount - 1))) {
          cout << "Try #" << tryCount << " " << human(nKeysInOthello) << " Keys, ma/mb = "
               << human(ma) << "/" << human(mb) << " key length: " << sizeof(K) * 8 << "b  value length: "
               << sizeof(V) * 8 << "b VDL=" << (int) VDL << endl;
        }
        built = tryBuild();
      } while ((!built) && (tryCount < MAX_REHASH));
      
      if (built) {
        break;
      } else {
        resizeCapacity(keys.size() * 1.33, true);
      }
    }
    checkIntegrity();
    integrity = true;
    
    return true;
  }
  
  /// Begin a new build
  /// Side effect: 1) discard all memory except keys and values. 2) build fail, or
  /// all the values and disjoint set are properly set
  bool tryBuild() {
    #ifdef PROFILE
    if(full_debug) Clocker rebuild("Othello try rebuild");
    #else
    cout << "rebuild" << endl;
    #endif
    resetBuildState();
    
    if (nKeysInOthello == 0) {
      return true;
    }
    
    bool succ = testHash();  // time consuming
    if (succ) {
      fillValue<false>();  // time consuming
      integrity = true;
    }
    
    return succ;
  }
  
  inline vector<uint32_t> getHalfTree(const K &k, bool startFromA, bool prependTheOtherEnd) const {
    uint32_t na;
    uint32_t nb;
    getIndices(k, na, nb);
    
    vector<uint32_t> result;
    
    stack<pair<uint32_t, uint32_t>> stack;  // previous key id, this node
    
    if (startFromA) {
      if (prependTheOtherEnd) result.push_back(nb);
      result.push_back(na);
      stack.push(make_pair(lookUpIndex(k), na));
    } else {
      if (prependTheOtherEnd) result.push_back(na);
      result.push_back(nb);
      stack.push(make_pair(lookUpIndex(k), nb));
    }
    
    do {
      uint32_t prev = stack.top().first;
      uint32_t nid = stack.top().second;
      stack.pop();
      
      bool isAtoB = nid < ma;
      
      // // find all the opposite side node to be filled
      // search all the edges of this node, to fill and enqueue the opposite side, and record the fill
      const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
      
      for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
        // now the opposite side node needs to be filled
        // fill and enqueue all next element of it
        if (cell.keyId == prev) continue;
        
        uint32_t nextNode = cell.nodeId;
        result.push_back(nextNode);
        
        stack.push(make_pair(uint32_t(cell.keyId), nextNode));
      }
    } while (!stack.empty());
    
    return result;
  }
  
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  ///// No need to acquire locks below this line because the callers already have the correct locks
  /////////////////////////////////////////////////////////////////////////////////////////////////////////
  
  /// Forget all previous build states and get prepared for a new build
  void resetBuildState() {
    for (uint32_t i = 0; i < ma + mb; ++i) {
      if (maintainingDP) memSet(i, randomized ? (randVal(i) & VDMASK) : 0);
    }
    
    memset(&head[0], -1, head.size() * sizeof(head[0]));
    memset(&nextAtA[0], -1, nextAtA.size() * sizeof(nextAtA[0]));
    memset(&nextAtB[0], -1, nextAtB.size() * sizeof(nextAtB[0]));
    
    connectivityForest.reset();
  }
  
  /// test the two nodes are connected or not
  /// Assume the Othello is properly built
  /// \note cannot use disjoint set if because disjoint set cannot maintain valid after key deletion. So a traverse is performed
  /// \param ha0
  /// \param hb0
  /// \return true if connected
  bool isConnectedDFS(uint32_t ha0, uint32_t hb0) const {
    if (maintainDisjointSet) return connectivityForest.representative(ha0) == connectivityForest.representative(hb0);
    
    if (ha0 == hb0) return true;
    
    stack<pair<uint32_t, uint32_t>> stack;  // previous key id, this node
    stack.push(make_pair(uint32_t(-1), ha0));
    
    do {
      uint32_t prev = stack.top().first;
      uint32_t nid = stack.top().second;
      stack.pop();
      
      bool isAtoB = nid < ma;
      
      const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
      
      for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
        // now the opposite side node needs to be filled
        // fill and enqueue all next element of it
        if (cell.keyId == prev) continue;
        
        uint32_t nextNode = cell.nodeId;
        
        if (nextNode == hb0) {
          return true;
        }
        
        stack.push(make_pair(uint32_t(cell.keyId), nextNode));
      }
    } while (!stack.empty());
    
    return false;
  }
  
  /// Ensure the disjoint set is properly maintained according to the connectivity of this tree.
  /// the workflow is: set the representatives of all connected nodes as root
  /// \param node
  void connectDFS(uint32_t root) {
    stack<pair<uint32_t, uint32_t>> stack;  // previous key id, this node
    
    if (head[root].keyId == uint32_t(-1)) {
      if (maintainingDP && DL) {
        memSet(root, memGet(root) & (uint64_t(-1) << 1U));   // mark as empty
      }
      connectivityForest.__set(root, root);   // singleton tree
      return;
    } else if (root > ma) { // all representatives are from array A
      root = head[root].nodeId;
    }
    connectivityForest.__set(root, root);
    stack.push(make_pair(uint32_t(-1), root));
    
    do {
      uint32_t prev = stack.top().first;
      uint32_t nid = stack.top().second;
      stack.pop();
      
      bool isAtoB = nid < ma;
      
      const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
      
      for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
        // now the opposite side node needs to be filled
        // fill and enqueue all next element of it
        if (cell.keyId == prev) continue;
        
        uint32_t nextNode = cell.nodeId;
        
        connectivityForest.__set(nextNode, root);
        
        stack.push(make_pair(uint32_t(cell.keyId), nextNode));
      }
    } while (!stack.empty());
  }
  
  template<bool fillValue, bool fillIndex, bool keepDigest = false>
  inline void fillSingle(uint32_t keyId, uint32_t nodeToFill, uint32_t oppositeNode) {
    if (fillValue && maintainingDP) {
      uint64_t valueToFill;
      if (keepDigest) {
        uint64_t v = values[keyId];
        valueToFill = v ^ memValueGet(oppositeNode);
        memValueSet(nodeToFill, valueToFill);
      } else {
        if (DL) {
          uint64_t digest = hd(keys[keyId]) & DEMASK;
          uint64_t vd = (values[keyId] << DL) | digest;
          valueToFill = (vd ^ memGet(oppositeNode)) | 1ULL;
        } else {
          uint64_t v = values[keyId];
          valueToFill = v ^ memGet(oppositeNode);
        }
        
        memSet(nodeToFill, valueToFill);
      }
    }
    
    if (fillIndex) {
      uint32_t indexToFill = keyId ^indMem[oppositeNode];
      indMem[nodeToFill] = indexToFill;
    }
  }
  
  template<bool fillValue, bool fillIndex, bool keepDigest = false>
  /// fix the value and index at single node by xoring x
  /// \param x the xor'ed number
  inline void fixSingle(uint32_t nodeToFix, uint64_t x, uint32_t ix) {
    if (fillValue && maintainDP) {
      if (keepDigest) {
        uint64_t valueToFill = x ^memValueGet(nodeToFix);
        memValueSet(nodeToFix, valueToFill);
      } else {
        uint64_t valueToFill = x ^memGet(nodeToFix);
        memSet(nodeToFix, valueToFill);
      }
    }
    
    if (fillIndex) {
      uint32_t indexToFill = ix ^indMem[nodeToFix];
      indMem[nodeToFix] = indexToFill;
    }
  }
  
  /// Fix the values of a connected tree starting at the root node and avoid searching keyId
  /// Assume:
  /// 1. the value of root is not properly set before the function call
  /// 2. the values are in the value array
  /// 3. the root is always from array A
  /// Side effect: all node in this tree is set and if updateToFilled
  /// @return  the xor template:
  ///           > 0 starting from the A node
  ///           < 0 from the B node
  ///           == 0 doesn't matter
  template<bool fillValue, bool fillIndex, bool keepDigest = false>
  int64_t fixHalfTreeDFS(uint32_t keyId, uint32_t startNode, uint32_t skippedNode) {
    assert(startNode < ma && keyId != uint32_t(-1));
    
    bool swapped = false;
    
    uint64_t startNodeVal = maintainingDP ? memGet(startNode) : 0;
    uint64_t skippedNodeVal = maintainingDP ? memGet(skippedNode) : 0;
    
    if (maintainingDP && DL && (skippedNodeVal & 1U) == 0) {
      skippedNodeVal |= 1U;
      memSet(skippedNode, skippedNodeVal);
      
      if ((startNodeVal & 1U) == 1) {
        swap(startNode, skippedNode);
        swap(startNodeVal, skippedNodeVal);
        swapped = true;
      }
    }
    
    uint64_t x = fillValue ? (keepDigest ? memValueGet(startNode) : startNodeVal) : 0;
    uint32_t ix = fillIndex ? indMem[startNode] : 0;
    
    fillSingle<fillValue, fillIndex, keepDigest>(keyId, startNode, skippedNode);
    
    x = fillValue ? (x ^ (keepDigest ? memValueGet(startNode) : memGet(startNode)))
                  : 0;  // the xor'ed value field, including digests. E must be 1, because both ends are 1
    ix = fillIndex ? ix ^ indMem[startNode] : 0;  // the xor'ed index field
    
    stack<pair<uint32_t, uint32_t>> stack;  // previous key id, this node
    stack.push(make_pair(keyId, startNode));
    
    do {
      if(full_debug) Clocker::count("Othello fixHalfTreeDFS step");
      uint32_t prev = stack.top().first;
      uint32_t nid = stack.top().second;
      stack.pop();
      
      bool isAtoB = nid < ma;
      
      // // find all the opposite side node to be filled
      // search all the edges of this node, to fill and enqueue the opposite side, and record the fill
      const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
      
      for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
        // now the opposite side node needs to be filled
        // fill and enqueue all next element of it
        if (cell.keyId == prev) continue;
        
        uint32_t nextNode = cell.nodeId;
        
        fixSingle<fillValue, fillIndex, keepDigest>(nextNode, x, ix);
        
        stack.push(make_pair(uint32_t(cell.keyId), nextNode));
      }
    } while (!stack.empty());
    
    checkIntegrity();
    
    return swapped ? -x : x;
  }
  
  inline int64_t changeValueAt(uint32_t keyId, V val) {
    assert (keyId < nKeysInOthello);
    
    values[keyId] = val;
    
    if (maintainDP) {
      const K &k = keys[keyId];
      uint32_t ha, hb;
      getIndices(k, ha, hb);
      return fixHalfTreeDFS<true, false, true>(keyId, ha, hb);
    }
    
    return 0;
  }
  
  inline V randVal(int i = 0) const {
    V v = rand();
    
    if (sizeof(V) > 4) {
      *(((int *) &v) + 1) = rand();
    }
    return v;
  }
  
  /// \param k
  /// \return the index of k in the array of keys
  inline uint32_t lookUpIndex(const K &k) const {
    uint32_t ha, hb;
    getIndices(k, ha, hb);
    uint32_t aa = indMem[ha];
    uint32_t bb = indMem[hb];
    return aa ^ bb;
  }
  
  /// Fill *Othello* so that the lookUp returns values as defined
  ///
  /// Assume: edges and disjoint set are properly set up.
  /// Side effect: all values are properly set
  template<bool keepDigest = false>
  void fillValue() {
    for (uint32_t i = 0; i < ma + mb; i++) {
      if (connectivityForest.isRoot(i)) {  // we can only fix one end's value in a cc of keys, then fix the roots'
        if ((DL || randomized) && maintainingDP) {
          memSet(i, randomized ? randVal() | 1 : 1);
        }
        
        fillTreeDFS<true, true, keepDigest>(i);
      }
    }
  }
  
  /// Fill the values of a connected tree starting at the root node and avoid searching keyId
  /// Assume:
  /// 1. the value of root is properly set before the function call
  /// 2. the values are in the value array
  /// 3. the root is always from array A
  /// Side effect: all node in this tree is set and if updateToFilled
  template<bool fillValue, bool fillIndex, bool keepDigest = false>
  void fillTreeDFS(uint32_t root) {
    stack<pair<uint32_t, uint32_t>> stack;  // previous key id, this node
    stack.push(make_pair(uint32_t(-1), root));
    
    do {
      if(full_debug) Clocker::count("Othello fillTreeDFS step");
      uint32_t prev = stack.top().first;
      uint32_t nid = stack.top().second;
      stack.pop();
      
      bool isAtoB = nid < ma;
      
      // // find all the opposite side node to be filled
      // search all the edges of this node, to fill and enqueue the opposite side, and record the fill
      const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
      
      for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
        // now the opposite side node needs to be filled
        // fill and enqueue all next element of it
        if (cell.keyId == prev) continue;
        
        uint32_t nextNode = cell.nodeId;
        
        fillSingle<fillValue, fillIndex, keepDigest>(cell.keyId, nextNode, nid);
        
        stack.push(make_pair(uint32_t(cell.keyId), nextNode));
      }
    } while (!stack.empty());
  }
  
  /// test if this hash pair is acyclic, and build:
  /// the connected forest and the disjoint set of connected relation
  /// the disjoint set will be only useful to determine the root of a connected component
  ///
  /// Assume: all build related memory are cleared before
  /// Side effect: the disjoint set and the connected forest are changed
  bool testHash() {
    uint32_t ha, hb;
    
    for (int i = 0; i < nKeysInOthello; i++) {
      const K &k = keys[i];
      uint32_t ha, hb;
      getIndices(k, ha, hb);
      
      // two indices are in the same disjoint set, which means the current key will incur a circle.
      if (connectivityForest.sameSet(ha, hb)) {
        #ifdef FULL_DEBUG
        //        checkLoop(ha, hb, i);
        #endif
        
        return false;
      }
      addEdge(i, ha, hb);
    }
    return true;
  }
  
  /// update the disjoint set and the connected forest so that
  /// include all the old keys and the newly inserted key
  /// \note this method won't change the node value
  inline void addEdge(uint32_t key, uint32_t ha, uint32_t hb) {
    nextAtA[key] = head[ha];
    head[ha] = {key, hb};
    nextAtB[key] = head[hb];
    head[hb] = {key, ha};
    
    connectivityForest.merge(ha, hb);
  }
  
  inline void checkIntegrity() const {
    #ifdef FULL_DEBUG
    for (int i = 0; i < nKeysInOthello; ++i) {
      V q;
      assert(lookUp(keys[i], q));
      q &= VMASK;
      V e = values[i] & VMASK;
      assert(q == e);
      assert(lookUpIndex(keys[i]) == i);
    }
    #endif
  }
  
  //****************************************
  //*********As a randomizer
  //****************************************
public:
  // return the mapped count of all possible values
  vector<uint32_t> getCnt() const {
    vector<uint32_t> cnt(1ULL << L);
    
    for (int i = 0; i < ma; i++) {
      for (int j = ma; j < ma + mb; j++) {
        cnt[memGet(i) ^ memGet(j)]++;
      }
    }
    return cnt;
  }
  
  void outputMappedValues(ofstream &fout) const {
    bool partial = (uint64_t) ma * (uint64_t) mb > (1UL << 22);
    
    if (partial) {
      for (int i = 0; i < (1 << 22); i++) {
        fout << uint32_t(memGet(rand() % (ma - 1)) ^ memGet(ma + rand() % (mb - 1))) << endl;
      }
    } else {
      for (int i = 0; i < ma; i++) {
        for (int j = ma; j < ma + mb; ++j) {
          fout << uint32_t(memGet(ma) ^ memGet(j)) << endl;
        }
      }
    }
  }
  
  int getStaticCnt() {
    return ma * mb;
  }
  
  uint64_t getMemoryCost() const {
    return mem.size() * sizeof(mem[0]) + keys.size() * sizeof(keys[0]) + values.size() * sizeof(values[0]) +
           indMem.size() * sizeof(indMem[0]);
  }
  
  #ifdef FULL_DEBUG
  
  void checkLoop(int ha, int hb, int keyId) {
    const K &k = keys[keyId];
    vector<pair<uint32_t, uint32_t>> stack;
    stack.emplace_back(uint32_t(keyId), ha);
    checkLoop(hb, keyId, stack);
  }
  
  void checkLoop(int loopEnd, int excludeId, vector<pair<uint32_t, uint32_t>> &stack) {
    uint32_t prev = stack.back().first;
    uint32_t nid = stack.back().second;
    
    bool isAtoB = nid < ma;
    const vector<OthelloCPCell> &nextKeyOfThisKey = isAtoB ? nextAtA : nextAtB;
    
    for (OthelloCPCell cell = head[nid]; cell.keyId != uint32_t(-1); cell = nextKeyOfThisKey[cell.keyId]) {
      if (cell.keyId == prev || cell.keyId == excludeId) continue;
      
      uint32_t nextNode = cell.nodeId;
      
      stack.emplace_back(uint32_t(cell.keyId), nextNode);
      if (nextNode == loopEnd) {
        cout << "Loop found! " << endl;
        
        for (pair<uint32_t, uint32_t> &pair: stack) {
          uint32_t keyId = pair.first;
          const K &k = keys[keyId];
          
          uint32_t ha, hb;
          getIndices(k, ha, hb);
          
          cout << k << ": (" << ha << ", " << hb << ")" << endl;
        }
        
        return;
      }
      
      checkLoop(loopEnd, excludeId, stack);
      stack.pop_back();
    }
  }
  
  #endif
};


template<class K, class V, uint8_t L = sizeof(V) * 8>
class OthelloMap : public ControlPlaneOthello<K, V, L, false, true> {
public:
  explicit OthelloMap(uint32_t keyCapacity = 256) : ControlPlaneOthello<K, V, L, false, true>(keyCapacity) {}
};

template<class K>
class OthelloSet : public ControlPlaneOthello<K, bool, 0, false, true> {
public:
  explicit OthelloSet(uint32_t keyCapacity = 256) : ControlPlaneOthello<K, bool, 0, false, true>(keyCapacity) {}
  
  inline bool insert(const K &k) {
    return ControlPlaneOthello<K, bool, 0, false, true>::insert(k, true);
  }
};
