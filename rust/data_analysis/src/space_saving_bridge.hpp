#ifndef __SPACE_SAVING_BRIDGE_HPP
#define __SPACE_SAVING_BRIDGE_HPP

#include "cpp_include/util/cityhash_tc.hpp"
#include "rust/cxx.h"

struct frequent_item;
struct frequent_item_with_range;

namespace turi {

// This needs to be defined
static inline uint64_t hash64(const rust::String& s) {
  return hash64(s.data(), s.size());
}

}  // namespace turi

#include "cpp_include/sketches/space_saving.hpp"

typedef turi::sketches::space_saving<rust::String> space_saving_sketch_t;

static inline std::shared_ptr<space_saving_sketch_t> new_space_saving_sketch(
    double epsilon) {
  return std::make_shared<space_saving_sketch_t>(epsilon);
}

static inline void sss_add_element(std::shared_ptr<space_saving_sketch_t> sss,
                                   rust::String value) {
  sss->add(value);
}

/// The number of elements inserted into the sketch.
static inline size_t sss_size(std::shared_ptr<space_saving_sketch_t> sss) {
  return sss->size();
}

/// Clears everything out.
static inline void sss_clear(std::shared_ptr<space_saving_sketch_t> sss) {
  sss->clear();
}

/**
 * Merges a second space saving sketch into the current sketch.
 */
static inline void sss_combine(
    std::shared_ptr<space_saving_sketch_t> sss,
    std::shared_ptr<space_saving_sketch_t> sss_other) {
  sss->combine(*sss_other);
}

/**
 * Returns all the elements tracked by the sketch as well as an
 * estimated count. The estimated can be a large overestimate.
 */
std::unique_ptr<std::vector<::frequent_item>> sss_frequent_items(
    std::shared_ptr<space_saving_sketch_t> sss);

/**
 * Returns all the elements tracked by the sketch as well as an
 * estimated count. All elements returned are guaranteed to have
 * occurance >= epsilon * m_size
 */
std::unique_ptr<std::vector<::frequent_item>> sss_guaranteed_frequent_items(
    std::shared_ptr<space_saving_sketch_t> sss);

/**
 *  Returns all the elements tracked by the sketch and a range of possible count
 *  values.
 */
std::unique_ptr<std::vector<::frequent_item_with_range>>
sss_frequent_items_with_range(std::shared_ptr<space_saving_sketch_t> sss);

#endif