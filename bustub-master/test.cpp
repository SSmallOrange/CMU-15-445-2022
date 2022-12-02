#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <set>
#include <list>
#include <unordered_map>
#include <queue>
#include <time.h>
#include <pthread.h>
#include <unordered_set>
#include <algorithm>
#include <assert.h>

using namespace std;


class person{
 public:
      person(){cout << "person\n" << '\n';}
      virtual ~person(){cout << "~person\n" << '\n';}
      virtual void print(){cout << "hello person\n" << '\n';}
      int aaa;
   private:
    int size_;
};
template <typename T>
class child : public person{
public:
    child(){cout << "child\n" << '\n';}
    ~child(){cout << "~child\n" << '\n';}
    void print(){cout << "hello child\n" << '\n';}
    void ttt() {}
    template <class test>
    void TemplateFunc(test a);
};

template<typename T> template<class test>
void child<T>::TemplateFunc(test a) {

}

//转移unique_ptr指针的对象：移动语义
//模板类无法使用多态，应该使用dynamic_cast先进行类型转换后再调用子类的函数
unique_ptr<person> global;
void foo(unique_ptr<person> &&temp){
    global = std::move(temp);
    temp->print();
}
void test(int* &&t){

}

list<pair<int,int>>::iterator foo(list<pair<int,int>>  & a){
    return a.begin();
}
auto OtherBucket(size_t x) -> size_t {
  return (x & (1 << (3 - 1))) == 0 ? (x | (1 << (3 - 1)))
                                                : (x & (~(1 << (3 - 1))));
}
struct myhash{
public:
    size_t operator()(pair<int,int> a) const {
        return a.first ^ a.second;
    }
};
class compare {
public:
    bool operator()(int a, int b) {
        return a < b;
    }
};
union obj {
    union obj* free_list_link;
    char clinet_data[1]; 
};

  void GetIndexInternal(size_t local_depth, size_t global_depth, size_t cnt, size_t x, std::vector<size_t> &temp) {
    if (local_depth + cnt > global_depth - 1) {
      temp.push_back(x);
      return;
    }
    GetIndexInternal(local_depth, global_depth, cnt + 1, x & (~(1 << (local_depth + cnt))), temp);
    GetIndexInternal(local_depth, global_depth, cnt + 1, (x | (1 << (local_depth + cnt))), temp);
  }
  auto GetIndex(size_t idx, size_t local_depth) -> std::vector<size_t> {
    std::vector<size_t> temp;
    size_t x = (idx & ((1 << local_depth) - 1));
    cout << x << endl;;
    GetIndexInternal(local_depth, 3, 0, x, temp);
    return temp;
  }



int main(){
  int a[9] = {1,4,9,11,16,25,31,79,88};
  int l = 0,r = 9;
  while (l < r) {
    int mid = (r - l) / 2 + l;
    if (a[mid] < 26) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  cout << l << endl;
  assert(5 < 6);
  return 0;
}