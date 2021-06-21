#pragma once

#include <cassert>
#include <sstream>

#include "bitsutil.h"
#include "debug.h"
#include "printutil.h"
#include "../utils/CompactArray.h"

namespace cuckoofilter {

// the most naive table implementation: one huge bit array
  template<size_t bits_per_tag>
  class BetterTable {
    static const size_t kTagsPerBucket = 4;
    static const uint32_t kTagMask = (1ULL << bits_per_tag) - 1;
    
    // using a pointer adds one more indirection
    size_t num_buckets_;
    CompactArray<bits_per_tag> buckets_;
  
  public:
    explicit BetterTable(const size_t num) : num_buckets_(num), buckets_(num_buckets_ * kTagsPerBucket) {}
    
    size_t NumBuckets() const {
      return num_buckets_;
    }
    
    size_t SizeInBytes() const {
      return num_buckets_ * kTagsPerBucket * bits_per_tag / 8;
    }
    
    size_t SizeInTags() const {
      return kTagsPerBucket * num_buckets_;
    }
    
    std::string Info() const {
      std::stringstream ss;
      ss << "BetterTable with tag size: " << bits_per_tag << " bits \n";
      ss << "\t\tAssociativity: " << kTagsPerBucket << "\n";
      ss << "\t\tTotal # of rows: " << num_buckets_ << "\n";
      ss << "\t\tTotal # slots: " << SizeInTags() << "\n";
      return ss.str();
    }
    
    // read tag from pos(i,j)
    inline uint64_t ReadTag(const size_t i, const size_t j) const {
      return buckets_.memGet(i * kTagsPerBucket + j);
    }
    
    // write tag to pos(i,j)
    inline void WriteTag(const size_t i, const size_t j, const uint32_t t) {
      buckets_.memSet(i * kTagsPerBucket + j, t);
    }
    
    inline void ReadBucket(const size_t i, uint32_t *tag) {
      tag[0] = ReadTag(i, 0);
      tag[1] = ReadTag(i, 1);
      tag[2] = ReadTag(i, 2);
      tag[3] = ReadTag(i, 3);
    }
    
    inline bool FindTagInBucket(const size_t i, const uint32_t tag) const {
      for (size_t j = 0; j < kTagsPerBucket; j++) {
          if (ReadTag(i, j) == tag) {
            return true;
          }
        }
        return false;
    }
    
    inline bool DeleteTagFromBucket(const size_t i, const uint32_t tag) {
      for (size_t j = 0; j < kTagsPerBucket; j++) {
        if (ReadTag(i, j) == tag) {
          WriteTag(i, j, 0);
          return true;
        }
      }
      return false;
    }
    
    inline bool
    InsertTagToBucket(const size_t i, const uint32_t tag, const bool kickout, uint32_t &oldtag, uint32_t *tags) {
      for (size_t j = 0; j < kTagsPerBucket; j++) {
        tags[j] = ReadTag(i, j);
        if (tags[j] == 0) {
          WriteTag(i, j, tag);
          return true;
        }
      }
      if (kickout) {
        size_t r = rand() % kTagsPerBucket;
        oldtag = tags[r];
        WriteTag(i, r, tag);
      }
      return false;
    }
  
    inline void WriteBucket(const size_t i, uint32_t tags[4], bool sort = true, int pos = 4) {
      // stupid
    }
  
    inline void WriteBucket(const size_t i, uint32_t tags[4]) {
      for (int pos = 0; pos < kTagsPerBucket; ++pos)
        WriteTag(i, pos, tags[pos]);
    }
    
    inline size_t NumTagsInBucket(const size_t i) const {
      size_t num = 0;
      for (size_t j = 0; j < kTagsPerBucket; j++) {
        if (ReadTag(i, j) != 0) {
          num++;
        }
      }
      return num;
    }
  };
}  // namespace cuckoofilter
