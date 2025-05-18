#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {
auto Trie::FindPath(std::string_view key) const -> std::vector<const TrieNode *> {
  // key is not end with '\0'
  std::vector<const TrieNode *> path;
  const TrieNode *temp_node = root_.get();
  if (temp_node != nullptr) {
    path.emplace_back(temp_node);
    for (auto c : key) {
      if (auto it = temp_node->children_.find(c); it != temp_node->children_.end()) {
        temp_node = it->second.get();
        path.emplace_back(temp_node);
      } else {
        break;
      }
    }
  }
  return path;
}
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // maybe root
  if (!key.empty() && key.back() == '\0') {
    key = key.substr(0, key.size() - 1);
  }
  auto path = FindPath(key);
  if (path.size() != key.size() + 1) {
    return nullptr;
  }
  auto ptr = dynamic_cast<const TrieNodeWithValue<T> *>(path.back());
  if (ptr == nullptr) {
    return nullptr;
  }
  return ptr->value_.get();

  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  if (!key.empty() && key.back() == '\0') {
    key = key.substr(0, key.size() - 1);
  }
  std::vector<std::shared_ptr<TrieNode>> new_nodes;
  auto path = FindPath(key);
  // path.size()<=key.size()+1
  for (size_t index = 0; index < key.size(); ++index) {
    if (path.size() > index) {
      new_nodes.emplace_back(std::shared_ptr<TrieNode>(path[index]->Clone()));
    } else {
      new_nodes.emplace_back(std::make_shared<TrieNode>());
    }
  }
  if (path.size() == key.size() + 1) {
    new_nodes.emplace_back(
        std::make_shared<TrieNodeWithValue<T>>(path.back()->children_, std::make_shared<T>(std::move(value))));
  } else {
    new_nodes.emplace_back(std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
  }
  for (size_t index = new_nodes.size() - 1; index > 0; --index) {
    new_nodes[index - 1]->children_.insert_or_assign(key[index - 1], std::move(new_nodes[index]));
  }
  // new_nodes must have one
  return Trie(new_nodes[0]);

  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (!key.empty() && key.back() == '\0') {
    key = key.substr(0, key.size() - 1);
  }
  auto path = FindPath(key);
  if (path.size() != key.size() + 1 || !path.back()->is_value_node_) {
    return Trie(root_);
  }
  // newNode number needed
  size_t sum = path.size();
  if (path[sum - 1]->children_.empty()) {
    --sum;
    for (; sum > 0; --sum) {
      if (!path[sum - 1]->is_value_node_ && path[sum - 1]->children_.size() == 1) {
      } else {
        break;
      }
    }
  }

  if (sum <= 0) {
    return {};
  }

  std::vector<std::shared_ptr<TrieNode>> new_nodes;
  for (size_t index = 0; index < sum - 1; ++index) {
    new_nodes.emplace_back(std::shared_ptr<TrieNode>(path[index]->Clone()));
  }
  // need to save the last node
  if (sum == key.size() + 1) {
    new_nodes.emplace_back(std::make_shared<TrieNode>(path[sum - 1]->children_));
  } else {
    // don't need to save
    new_nodes.emplace_back(std::shared_ptr<TrieNode>(path[sum - 1]->Clone()));
    new_nodes.back()->children_.erase(key[sum - 1]);
  }
  for (size_t index = sum - 1; index > 0; --index) {
    new_nodes[index - 1]->children_.insert_or_assign(key[index - 1], std::move(new_nodes[index]));
  }
  return Trie(std::move(new_nodes[0]));

  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
