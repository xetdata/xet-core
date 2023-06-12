#include "space_saving_bridge.hpp"

#include "data_analysis/src/sketches_bridge.rs.h"

static inline std::unique_ptr<std::vector<::frequent_item>> convert_item_format(
    std::vector<std::pair<rust::String, size_t>>&& src) {
  std::vector<frequent_item> ret_v;
  ret_v.reserve(src.size());

  for (const auto& p : src) {
    frequent_item fi_ret;
    fi_ret.value = std::move(p.first);
    fi_ret.count = p.second;
    ret_v.push_back(fi_ret);
  }

  return std::make_unique<std::vector<frequent_item>>(std::move(ret_v));
}

std::unique_ptr<std::vector<::frequent_item>> sss_guaranteed_frequent_items(
    std::shared_ptr<space_saving_sketch_t> sss) {
  auto fr_it = sss->guaranteed_frequent_items();
  return convert_item_format(std::move(fr_it));
}

std::unique_ptr<std::vector<::frequent_item>> sss_frequent_items(
    std::shared_ptr<space_saving_sketch_t> sss) {
  auto fr_it = sss->frequent_items();
  return convert_item_format(std::move(fr_it));
}

std::unique_ptr<std::vector<::frequent_item_with_range>>
sss_frequent_items_with_range(std::shared_ptr<space_saving_sketch_t> sss) {
  auto src = sss->frequent_items_with_count_ranges();

  std::vector<::frequent_item_with_range> ret_v;
  ret_v.reserve(src.size());

  for (const auto& p : src) {
    ::frequent_item_with_range fi_ret;
    fi_ret.value = std::move(p.first);
    fi_ret.count_min = p.second.first;
    fi_ret.count_max = p.second.second;
    ret_v.push_back(fi_ret);
  }

  return std::make_unique<std::vector<frequent_item_with_range>>(
      std::move(ret_v));
}
