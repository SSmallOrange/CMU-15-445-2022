//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key,  ValueType &value,
                                                                   KeyComparator &cmp) -> bool {
  // 先找到当前key所在的索引
  int index = FindkeyIndex(key, cmp);
  // 如果索引存在且和预期值相等，则将值返回并return true
  if (index < GetSize() && cmp(KeyAt(index), key) == 0) {
    value = array_[index].second;
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::FindkeyIndex(const KeyType &key, KeyComparator &cmp) -> int {
  int l = 0;
  int r = GetSize() - 1;
  assert(GetSize() >= 0);
  while (l < r) {
    int mid = (r - l) / 2 + l;
    if (cmp(array_[mid].first,key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &cmp) -> int {
  // 找到合适的插入位置
  int index = FindkeyIndex(key, cmp);
  int cur_size = GetSize();
  SetSize(++cur_size);
  for (int idx = cur_size - 1; idx > index; --idx) {
    array_[idx].first = array_[idx - 1].first;
    array_[idx].second = array_[idx - 1].second;
  }
  array_[index].first = key;
  array_[index].second = value;
  return cur_size;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitDataTo(B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page) {
  assert(new_leaf_page != nullptr);
  for (int idx = GetSize() / 2, new_idx = 0; idx < GetMaxSize(); idx++, new_idx++) {
    new_leaf_page->array_[new_idx].first = array_[idx].first;
    new_leaf_page->array_[new_idx].second = array_[idx].second;
  }
  SetSize(GetSize() / 2);
  new_leaf_page->SetSize(max_size_ - size_);
  new_leaf_page->next_page_id_ = next_page_id_;
  next_page_id_ = new_leaf_page->page_id_;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
