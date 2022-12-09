#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // 首先，找到key对应的叶子节点
  auto left_page = GetLeafNode(key);
  result->resize(1);  // 如果参数类型有重载‘=’运算符就不能使用resize?
  bool ans = left_page->FindKey(key, (*result)[0], comparator_);
  // 叶子节点使用完后进行unpin
  buffer_pool_manager_->UnpinPage(left_page->GetPageId(), false);
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // 如果发现根还是空的
  if (IsEmpty()) {
    ApplyNewRootPage(key, value);
    return true;
  }
  return InsertLeafInternal(key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::ApplyNewRootPage(const KeyType &key, const ValueType &value) {
  // 为root取一个新页
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(&new_page_id);
  assert(page != nullptr);
  // 更新root_page_id
  root_page_id_ = new_page_id;
  //每次申请新page时调用init
  auto leaf_page = reinterpret_cast<LeafPage*> (page->GetData());
  leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // rootpage 发生变化时调用UpdateRootPageId
  UpdateRootPageId(true);
  leaf_page->Insert(key, value, comparator_);
  // 使用完unpin
  buffer_pool_manager_->UnpinPage(new_page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeafInternal(const KeyType &key, const ValueType &value) -> bool{
  auto leaf_page = GetLeafNode(key);
  // 查看当前叶子节点有没有即将插入的key
  ValueType val;
  if (leaf_page->FindKey(key, val, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  // 如果插入后发现叶子节点的kv数达到了最大值
  if (leaf_page->Insert(key, value, comparator_) == leaf_max_size_) {
    // 新建一个新的page，将原page的一半转移到新page并更新next_page_id
    LeafPage *new_leaf_page = Split(leaf_page);
    // 将用于指向新节点的key插入到parent_page中
    InsertKeyToParentPage(leaf_page, new_leaf_page, new_leaf_page->KeyAt(0));
    buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template<class PageType> auto BPLUSTREE_TYPE::Split(PageType *left_page) -> PageType * {
  assert(left_page != nullptr);
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_page_id);
  PageType *right_page = reinterpret_cast<PageType*> (new_page->GetData());
  right_page->Init(new_page_id, left_page->GetParentPageId(), left_page->GetMaxSize());
  // 调用函数分离数据
  left_page->SplitDataTo(right_page);
  return right_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertKeyToParentPage(BPlusTreePage *left_page, BPlusTreePage *right_page, const KeyType &key) {
  // 分两种情况，一种是split前的节点是root节点，此时需要新new一个page出来作为根节点
  if (left_page->IsRootPage()) {
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_page != nullptr);
    assert(new_page->GetPinCount() == 1);
    InternalPage *new_root_page = reinterpret_cast<InternalPage*> (new_page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    // 给根节点插入新分裂出的page
    new_root_page->InsertFirstInit(left_page->GetPageId(), right_page->GetPageId(), key);
    UpdateRootPageId();
    left_page->SetParentPageId(root_page_id_);
    right_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    return;
  }
  // 第二种情况：普通的叶子节点split
  Page *page = buffer_pool_manager_->FetchPage(left_page->GetParentPageId());
  InternalPage *parent_page = reinterpret_cast<InternalPage*> (page->GetData());
  right_page->SetParentPageId(left_page->GetParentPageId());
  // 把分割点插入parent_page
  // 如果Insert发现需要split
  if (parent_page->InsertKeyAfterIt(left_page->GetPageId(), right_page->GetPageId(), comparator_, key) == internal_max_size_) {
    InternalPage *new_split_page = Split(parent_page);
    InsertKeyToParentPage(parent_page, new_split_page, new_split_page->KeyAt(0));
    buffer_pool_manager_->UnpinPage(new_split_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  LeafPage *leaf_page = GetLeafNode(key);
  if (leaf_page->Delete(key, comparator_) < leaf_page->GetMinSize()) {
    // 删除后发现节点个数小于最小值,需要merge或redistribute
    MergeOrRedistribute(leaf_page);
  }
}

//从兄弟节点调用数据填补自身或与兄弟直接进行合并
INDEX_TEMPLATE_ARGUMENTS
template <class PageType>
auto BPLUSTREE_TYPE::MergeOrRedistribute(PageType *old_page) -> bool {
  // 如果需要进行判断的是root
  if (old_page->IsRootPage()) {
    AdjustRoot(old_page);
    return true;
  }
  // 先判断当前节点能否和兄弟节点进行合并
  BPlusTreePage *left_page;
  BPlusTreePage *right_page;
  int index = FindBrothers(old_page, &left_page, &right_page);
  if (left_page && left_page->GetSize() + old_page->GetSize() <= left_page->GetMaxSize()) {
    Merge(old_page, left_page, index);
    return true;
  }
  if (right_page && right_page->GetSize() + old_page->GetSize() <= right_page->GetMaxSize()) {
    Merge(old_page, right_page, index);
    return true;
  }
  // 再判断是否能从兄弟那儿拿点来
  if (left_page) {
    (dynamic_cast<PageType *> (left_page))->MoveLastToFrontOf(old_page, buffer_pool_manager_);
  } else if (right_page) {
    (dynamic_cast<PageType *> (right_page))->MoveFrontToLastOf(old_page, buffer_pool_manager_);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <class PageType>
void BPLUSTREE_TYPE::Merge(PageType *original_page, BPlusTreePage *target, int index) {
  auto target_page = dynamic_cast<PageType *> (target);
  assert(original_page->GetSize() + target_page->GetSize() <= target_page->GetMaxSize());
  // 将original数据移到target中
  target_page->MergeWith(original_page, index, buffer_pool_manager_);
  auto old_page_id = original_page->GetPageId();
  auto old_parent_page_id = original_page->GetParentPageId();
  buffer_pool_manager_->UnpinPage(old_page_id, true);
  buffer_pool_manager_->DeletePage(old_page_id);
  buffer_pool_manager_->UnpinPage(target_page->GetPageId(), true);
  auto parent_page = dynamic_cast<InternalPage *> (FetchPage(old_parent_page_id));
  assert(parent_page != nullptr);
  // 有说法的， 能到这里的都是内部节点，内部节点在判断是否需要重置的时候需要用小于等于，也就是提前一步就进行重置
  // 因为内部节点的header是空的，也就是小于等于 minsize - 1
  if (parent_page->DeleteInternal(index) <= parent_page->GetMinSize()) {
    MergeOrRedistribute(parent_page);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindBrothers(BPlusTreePage *cur_page, BPlusTreePage **left_page, BPlusTreePage **right_page) -> int {
    BPlusTreePage *parent = FetchPage(cur_page->GetParentPageId());
    auto page_id = cur_page->GetPageId();
    auto parent_page = dynamic_cast<InternalPage *> (parent);
    assert(parent_page);
    int index = parent_page->FindValueIndex(page_id);
    *left_page = index == 0 ? nullptr : FetchPage(parent_page->ValueAt(index - 1));
    *right_page = index == parent_page->GetMaxSize() - 1 ? nullptr : FetchPage(parent_page->ValueAt(index + 1));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    return index;
}

/*
 * root分两种情况：
 * 删除后root只剩一个节点（即0位置的key为空的节点），此时需要将该page换上来当根节点
 * 当前root为叶子节点且删除了最后一个节点，此时树中再无数据，删除树的最后一个节点
 */
INDEX_TEMPLATE_ARGUMENTS
template <class PageType>
void BPLUSTREE_TYPE::AdjustRoot(PageType *old_root_page) {
  assert(old_root_page != nullptr);
  if (old_root_page->IsLeafPage()) {
    assert(old_root_page->GetSize() == 0);
    assert (old_root_page->GetParentPageId() == INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return ;
  }
  if (old_root_page->GetSize() == 1) {
    auto old_internal_root_page = dynamic_cast<InternalPage*> (old_root_page);
    auto new_root_page_id = old_internal_root_page->ChangeRoot();
    auto old_root_page_id = old_internal_root_page->GetPageId();
    // 初始化新的root
    auto new_page = buffer_pool_manager_->FetchPage(new_root_page_id);
    assert(new_page != nullptr);
    auto new_root_page = reinterpret_cast<LeafPage*> (new_page->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    buffer_pool_manager_->UnpinPage(old_root_page_id, false);
    buffer_pool_manager_->DeletePage(old_root_page_id);
    return ;
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <class PageType>
void BPLUSTREE_TYPE::Redistribute(PageType *original_page, BPlusTreePage *target_page, int index, bool IsLeft) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  KeyType unless;
  auto leaf_page = GetLeafNode(unless, -1);
  return IndexIterator(leaf_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf_page = GetLeafNode(key);
  int index = leaf_page->FindKeyIndex(key);
  assert(index != -1);
  return IndexIterator(leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
    KeyType unless;
  auto leaf_page = GetLeafNode(unless, 1);
  assert(leaf_page != nullptr);
  return IndexIterator(leaf_page, leaf_page->GetSize() - 1, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafNode(const KeyType &key, int iter) -> LeafPage* {
  assert(!IsEmpty());
  // 获得根节点对应的页，从根节点开始遍历
  auto cur_page = FetchPage(root_page_id_);
  // 直到叶子节点才停止
  while (!cur_page->IsLeafPage()) {
    if(iter == -1) {
      page_id_t  page_id = dynamic_cast<InternalPage *> (cur_page)->ValueAt(0);
      cur_page = FetchPage(page_id);
      continue;
    }
    if (iter == 1) {
      page_id_t  page_id = dynamic_cast<InternalPage *> (cur_page)->GetEndValue();
      cur_page = FetchPage(page_id);
      continue;
    }
    // 提升指针后调用相应节点接口获得下一次应该寻找的子结点page_id
    page_id_t page_id = dynamic_cast<InternalPage*> (cur_page)->FindLowerBound(key, comparator_);
    // 每一个page最后一次使用后需要unpin以防缓存池以为还有用户在使用该页而无法进行驱逐等操作
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    // 跳转到下一层的页
    cur_page = FetchPage(page_id);
  }
  // 返回叶子节点
  return dynamic_cast<LeafPage*> (cur_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FetchPage(page_id_t page_id) -> BPlusTreePage* {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  return reinterpret_cast<BPlusTreePage*> (page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
