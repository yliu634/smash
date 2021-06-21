#pragma once

#include "common.h"

template<uint L>
class CompactArray {
  const static uint64_t MASK = ~(uint64_t(-1) << L);   // lower L bits are 1, others are 0

public:
  vector<uint64_t> _m;  // either give me a pre-allocated mem, or I will maintain one for you.
  uint64_t *mem;
  uint64_t capacity;
  
  explicit CompactArray(uint64_t capacity = 0, void *_mem = nullptr) :
      capacity(capacity), _m(_mem ? 0 : (capacity * L + 63) / 64), mem((uint64_t *) _mem) {}
  
  void resize(uint64_t capacity, uint64_t pad = 0) {
    _m.resize((capacity * L + 63) / 64, pad);
  }
  
  void clear() {
    _m.clear();
  }
  
  inline const uint64_t *getMem() const {
    return mem ? mem : _m.data();
  }
  
  inline uint64_t *getMem() {
    return mem ? mem : _m.data();
  }
  
  /// Set the index-th element to be value. if the index > ma, it is the (index - ma)-th element in array B
  /// \param index in array A or array B
  /// \param value
  inline void memSet(uint32_t index, uint64_t value) {
    if (L == 0) return;
    
    uint64_t v = uint64_t(value) & MASK;
    
    uint64_t i = (uint64_t) index * L;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    char left = char(offset + L - 64);
    
    uint64_t mask = ~(MASK << offset); // [offset, offset + L) should be 0, and others are 1
    
    getMem()[start] &= mask;
    getMem()[start] |= v << offset;
    
    if (left > 0 && (L != 1 && L != 2 && L != 4 && L != 8 && L != 16 && L != 32 && L != 64)) {
      mask = uint64_t(-1) << left;     // lower left bits should be 0, and others are 1
      getMem()[start + 1] &= mask;
      getMem()[start + 1] |= v >> (L - left);
    }
  }

/// \param index in array A or array B
/// \return the index-th element. if the index > ma, it is the (index - ma)-th element in array B
  inline uint64_t memGet(uint32_t index) const {
    if (L == 0) return 0;
    
    uint64_t i = (uint64_t) index * L;
    uint32_t start = i / 64;
    uint8_t offset = uint8_t(i % 64);
    
    char left = char(offset + L - 64);
    left = char(left < 0 ? 0 : left);
    
    uint64_t mask = ~(uint64_t(-1) << (L - left));   // lower L-left bits should be 1, and others are 0
    uint64_t result = (getMem()[start] >> offset) & mask;
    
    if (left > 0 && (L != 1 && L != 2 && L != 4 && L != 8 && L != 16 && L != 32 && L != 64)) {
      mask = ~(uint64_t(-1) << left);     // lower left bits should be 1, and others are 0
      result |= (getMem()[start + 1] & mask) << (L - left);
    }
    
    return result;
  }
};