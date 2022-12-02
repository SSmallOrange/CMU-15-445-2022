//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index > GetSize() - 1) {
    return;
  }
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindLowerBound(const KeyType &key, const KeyComparator &cmp) const -> ValueType{
  // 二分查找大于key的index,通过return l - 1 来返回大于key的page_id
  int l = 1;
  int r = GetSize() - 1;
  assert(GetSize() > 1);
  while (l <= r) {
    int mid = (r - l) / 2 + l;
    if (cmp(array_[mid].first, key) <= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return array_[l - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirstInit(const ValueType &old_page_id, const ValueType &new_page_id,
                                                   const KeyType &key) {
  array_[0].second = old_page_id;
  array_[1].first = key;
  array_[1].second = new_page_id;
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAfterIt(const ValueType &left_page_id, const ValueType &right_page_id,
                                                      KeyComparator &cmp, const KeyType &key) -> int {
  int index = FindLowerBound(key, cmp);
  int cur_size = GetSize();
  SetSize(++cur_size);
  for (int idx = cur_size - 1; idx > index; --idx) {
    array_[idx].first = array_[idx - 1].first;
    array_[idx].second = array_[idx - 1].second;
  }
  array_[index].first = key;
  array_[index].second = right_page_id;
  return cur_size;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitDataTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *right_page) {
  int cur_size = GetSize();
  int index = cur_size / 2;
  for (int idx = index; idx < cur_size; idx++) {
    right_page->array_[idx - index].first = array_[idx].first;
    right_page->array_[idx - index].second = array_[idx].second;
  }
  SetSize(index);
  right_page->SetSize(cur_size - index);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
