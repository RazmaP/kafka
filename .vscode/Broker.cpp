#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <alghorithm>
#include <cstring>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>


using namespace std;

struct Messege{
    string key;
    string value
};
using Partition = std::vector<Messege>;
struct Topic{
    vector::<partition> partitions;
};
struct Broker{
    public:
     bool createTopic(const string& name, size_t partitions){
        lock_guard::mutex> lk(mu_);
        if (topics_.count(name)) return false;
        Topic t;
        t.partitions.resize(partitions);
        topics_[name] = std::move(t);
        return true;
     }
     
}
