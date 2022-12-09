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

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0 || index > GetSize() - 1) {
    return;
  }
  array_[index].second = value;
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

// TODO 可能有问题
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetEndValue() -> ValueType {
  return ValueAt(GetSize() - 1);
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

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ChangeRoot() -> ValueType {
  assert(size_ == 1);
  IncreaseSize(-1);
  return array_[0].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeWith(BPlusTreeInternalPage *other_page, int index_of_parent, BufferPoolManager *buf) {
  Page *page = buf->FetchPage(other_page->GetParentPageId());
  assert(page != nullptr);
  int start = GetSize();
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *> (page->GetData());
  array_[start].first = parent_page->KeyAt(index_of_parent); // 关键！指向当前节点的父节点的下标的key是当前节点0下标的key!
  array_[start].second = other_page->ValueAt(0);
  buf->UnpinPage(parent_page->GetPageId(), false);

  for (int i = 1; i < other_page->GetSize(); i++) {
    array_[start + i].first = other_page->KeyAt(i);
    array_[start + i].second = other_page->ValueAt(i);

    Page *sufpage = buf->FetchPage(other_page->ValueAt(i));
    BPlusTreeInternalPage *child_page = reinterpret_cast<BPlusTreeInternalPage *> (sufpage->GetData());
    child_page->SetParentPageId(parent_page_id_);
    buf->UnpinPage(array_[i].second, true);
  }

  SetSize(start + other_page->GetSize());
  assert(GetSize() <= GetMaxSize());
  other_page->SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteInternal(int index) -> int {
  int cur_size = GetSize();
  assert(index >= 0 && index <= GetSize());
  memmove(array_ + index, array_ + index + 1, static_cast<size_t>((cur_size - index - 1) * sizeof(MappingType)));
  IncreaseSize(-1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *other_page, BufferPoolManager *buf) {
  assert(parent_page_id_ == other_page->GetParentPageId());
  memmove(other_page->array_ + 1, other_page->array_, other_page->GetSize()*sizeof(MappingType));

  auto page = buf->FetchPage(parent_page_id_);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *> (page->GetData());
  int index = parent_page->FindValueIndex(other_page->page_id_);

  other_page->SetKeyAt(1, parent_page->KeyAt(index));
  other_page->SetValueAt(0, ValueAt(GetSize() - 1));

  parent_page->SetKeyAt(index, KeyAt(GetSize() - 1));

  auto child = buf->FetchPage(ValueAt(GetSize() - 1));
  auto child_page = reinterpret_cast<BPlusTreePage *> (child->GetData());
  child_page->SetParentPageId(other_page->page_id_);
  buf->UnpinPage(child_page->GetPageId(), true);
  buf->UnpinPage(parent_page->GetPageId(), true);
  IncreaseSize(-1);
  other_page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrontToLastOf(BPlusTreeInternalPage *other_page, BufferPoolManager *buf) {
  assert(parent_page_id_ == other_page->GetParentPageId());

  auto page = buf->FetchPage(parent_page_id_);
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage *> (page->GetData());
  int index = parent_page->FindValueIndex(page_id_);

  other_page->SetKeyAt(other_page->GetSize(), parent_page->KeyAt(index));
  other_page->SetValueAt(other_page->GetSize(), ValueAt(0));

  parent_page->SetKeyAt(index, KeyAt(1));

  assert(GetSize() - 1 > 0);
  memmove(array_, array_ + 1, (GetSize() - 1)*sizeof(MappingType));

  auto child = buf->FetchPage(other_page->ValueAt(GetSize()));
  auto child_page = reinterpret_cast<BPlusTreePage *> (child->GetData());
  child_page->SetParentPageId(other_page->page_id_);
  buf->UnpinPage(child_page->GetPageId(), true);
  buf->UnpinPage(parent_page->GetPageId(), true);
  IncreaseSize(-1);
  other_page->IncreaseSize(1);
}

  // valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
