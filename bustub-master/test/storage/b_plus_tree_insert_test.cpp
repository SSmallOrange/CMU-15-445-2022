//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

#include <numeric>
#include <random>
#include <string>

#include "concurrency/transaction.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeTests, InsertTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  (void)header_page;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid, transaction);

  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  bpm->UnpinPage(root_page_id, false);
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, InsertTest3) {
/*
  17  2  12  29  30  24  20  22  25  7  4  13  1  15  23  14  16  6  27  21  18  11  3  9  5  28  8  26  10  19
  18  30  14  27  4  9  25  15  12  24  20  10  28  17  5  1  11  29  16  22  8  23  6  26  3  21  13  2  19  7
  Remove!!!
  4  15  20  3  9  16  27  12  21  19  18  26  2  13  14  17  22  30  1  25  8  6  28  23  10  11  29  24  7  5
         * */
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 6, 6);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int size = 30;

  std::vector<int64_t> keys(size);

  std::iota(keys.begin(), keys.end(), 1);

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  //8  26  25  22  9  4  2  21  17  27  29  6  28  16  23  30  13  19  24  15  3  11  7  1  14  5  12  10  20  18
/*
  //TODO debug
  std::vector<int> tt = {8,26,25,22,9,4,2,21,17,27,29,6,28,16,23,30,13,19,24,15,3,11,7,1,14,5,12,10,20,18};
  for (int index = 0; index < size; index++) {
    keys[index] = tt[index];
  }*/

  for (auto key : keys) {
    std::cout << key << "  ";
  }
  std::cout << std::endl;
  int i = 0;
  (void)i;
  for (auto key : keys) {
    i++;
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
     //tree.Debug();
     //std::cout << "-----------------------------------------------" << std::endl;
  }
  std::vector<RID> rids;

  // 打乱数组的排序
  std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    std::cout << key << "  ";
  }
  std::cout << std::endl;
  //25  6  19  1  17  21  2  12  3  7  26  29  11  15  13  5  8  9  30  16  14  28  4  22  10  23  27  24  20  18
/*
  // TODO debug
  std::vector<int> t = {25,6,19,1,17,21,2,12,3,7,26,29,11,15,13,5,8,9,30,16,14,28,4,22,10,23,27,24,20,18};
  for (int index = 0; index < size; index++) {
    keys[index] = t[index];
  }*/


  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.End(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  std::cout << "Remove!!!" << std::endl;
  EXPECT_EQ(current_key, keys.size() + 1);
  // 25  24  9  8  21  12  3  11  28  15  26  29  23  10  1  13  27  18  4  16  22  30  19  20  5  17  14  6  2  7
  std::shuffle(keys.begin(), keys.end(), g);
/*
  std::vector<int> ttt = {25,24,9,8,21,12,3,11,28,15,26,29,23,10,1,13,27,18,4,16,22,30,19,20,5,17,14,6,2,7};
  for (int index = 0; index < size; index++) {
    keys[index] = ttt[index];
  }*/

  for (auto key : keys) {
    std::cout << key << "  ";
  }
  std::cout << std::endl;
  for (auto key : keys) {
    i++;
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
     //tree.Debug();
     //std::cout << "-----------------------------------------------" << std::endl;
    // tree.Draw(bpm, "/homexstub-privateild-vscode/bin/InsertTest1_step" + std::to_string(i) + " delele" +
    //                    std::to_string(key) + ".dot");
  }

  EXPECT_EQ(true, tree.IsEmpty());

  bpm->UnpinPage(HEADER_PAGE_ID, true);

  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

}

}  // namespace bustub
