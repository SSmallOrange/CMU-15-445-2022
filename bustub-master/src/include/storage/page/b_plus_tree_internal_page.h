//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);
  auto ValueAt(int index) const -> ValueType;
  auto FindValueIndex(const ValueType &value) -> int;
  auto FindLowerBound(const KeyType &key, const KeyComparator &cmp) const -> ValueType;
  auto GetEndValue() -> ValueType;
  void InsertFirstInit(const ValueType &old_page_id, const ValueType &new_page_id, const KeyType &key);
  auto InsertKeyAfterIt(const ValueType &left_page_id, const ValueType &right_page_id, KeyComparator &cmp,
                        const KeyType &key) -> int;
  void SplitDataTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *right_page, BufferPoolManager *buf);
  auto ChangeRoot() -> ValueType;
  void MergeWith(BPlusTreePage *other_page_, int index_of_parent, BufferPoolManager *buf);
  auto DeleteInternal(int index) -> int;
  void MoveLastToFrontOf(BPlusTreeInternalPage *other_page, BufferPoolManager *buf);
  void MoveFrontToLastOf(BPlusTreeInternalPage *other_page, BufferPoolManager *buf);

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
