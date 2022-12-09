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
#include <string.h>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_internal_page.h"
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // replace with your own code
  array_[index].second = value;
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKeyIndex(const KeyType &key, KeyComparator &cmp) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (cmp(array_[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::FindkeyIndex(const KeyType &key, KeyComparator &cmp) -> int {
  assert(GetSize() >= 0);
  int st = 0, ed = GetSize() - 1;
  while (st <= ed) { //find the last key in array <= input
    int mid = (ed - st) / 2 + st;
    if (cmp(array_[mid].first,key) >= 0) ed = mid - 1;
    else st = mid + 1;
  }
  return ed + 1;
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
void B_PLUS_TREE_LEAF_PAGE_TYPE::SplitDataTo(B_PLUS_TREE_LEAF_PAGE_TYPE *new_leaf_page,
                                             __attribute__((unused)) BufferPoolManager *buf) {
  assert(new_leaf_page != nullptr);
  for (int idx = GetSize() / 2, new_idx = 0; idx < GetSize(); idx++, new_idx++) {
    new_leaf_page->array_[new_idx].first = array_[idx].first;
    new_leaf_page->array_[new_idx].second = array_[idx].second;
  }
  // TODO 用GetSize来修改长度
  SetSize(GetSize() / 2);
  new_leaf_page->SetSize(max_size_ - size_);
  new_leaf_page->next_page_id_ = next_page_id_;
  next_page_id_ = new_leaf_page->page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &cmp) -> int {
  int index = FindkeyIndex(key, cmp);
  int cur_size = GetSize();
  if (index < cur_size && cmp(array_[index].first, key) == 0) {
    for (int i = index; i < GetSize() - 1; i++) {
      array_[i].first = array_[i + 1].first;
      array_[i].second = array_[i + 1].second;
    }
    IncreaseSize(-1);
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MergeWith(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *other_page,
                                           int index_of_parent,
                                           BufferPoolManager *buf) {
  int start = GetSize();
  for (int i = 0; i < other_page->GetSize(); i++) {
    array_[start + i].first = other_page->KeyAt(i);
    array_[start + i].second = other_page->array_[i].second;
  }
  SetNextPageId(other_page->GetNextPageId());
  SetSize(start + other_page->GetSize());
  assert(start + other_page->GetSize() <= GetMaxSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *other_page, BufferPoolManager *buf) {
  // memmove(other_page->array_ + 1, other_page->array_, GetSize()*sizeof(MappingType));
  for (int i = GetSize(); i > 0; i--) {
    other_page->array_[i].first = other_page->array_[i - 1].first;
    other_page->array_[i].second = other_page->array_[i - 1].second;
  }
  other_page->SetKeyAt(0, array_[GetSize() - 1].first);
  other_page->SetValueAt(0, array_[GetSize() - 1].second);

  auto page = buf->FetchPage(other_page->GetParentPageId());
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> (page->GetData());

  int index = parent_page->FindValueIndex(other_page->GetPageId());
  parent_page->SetKeyAt(index, other_page->array_[0].first);
  buf->UnpinPage(parent_page->GetPageId(), true);
  IncreaseSize(-1);
  other_page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFrontToLastOf(BPlusTreeLeafPage *other_page, BufferPoolManager *buf) {
  other_page->SetKeyAt(other_page->GetSize(), array_[0].first);
  other_page->SetValueAt(other_page->GetSize(), array_[0].second);

  auto page = buf->FetchPage(GetParentPageId());
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *> (page->GetData());

  int index = parent_page->FindValueIndex(GetPageId());
  parent_page->SetKeyAt(index, array_[1].first);
  buf->UnpinPage(parent_page->GetPageId(), true);
  IncreaseSize(-1);
  other_page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPair(int index) -> MappingType & {
  return array_[index];
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
