#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

struct Message {
  std::string key;
  std::string value;
};
using Partition = std::vector<Message>;

struct Topic {
  std::vector<Partition> partitions;
};

class Broker {
 public:
  bool createTopic(const std::string& name, size_t partitions) {
    std::lock_guard<std::mutex> lk(mu_);
    if (topics_.count(name)) return false;
    Topic t;
    t.partitions.resize(partitions);
    topics_[name] = std::move(t);
    return true;
  }

  // returns offset
  std::optional<size_t> produce(const std::string& topic, size_t p,
                                std::string key, std::string value) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return std::nullopt;
    if (p >= it->second.partitions.size()) return std::nullopt;
    auto& part = it->second.partitions[p];
    part.push_back({std::move(key), std::move(value)});
    return part.size() - 1;
  }

  // read up to max_n from offset
  std::vector<std::pair<size_t, Message>> consume(const std::string& topic,
                                                  size_t p, size_t from,
                                                  size_t max_n) {
    std::lock_guard<std::mutex> lk(mu_);
    std::vector<std::pair<size_t, Message>> out;
    auto it = topics_.find(topic);
    if (it == topics_.end() || p >= it->second.partitions.size()) return out;
    auto& part = it->second.partitions[p];
    if (from >= part.size()) return out;
    size_t to = std::min(part.size(), from + max_n);
    out.reserve(to - from);
    for (size_t i = from; i < to; ++i) out.push_back({i, part[i]});
    return out;
  }

  size_t size(const std::string& topic, size_t p) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = topics_.find(topic);
    if (it == topics_.end() || p >= it->second.partitions.size()) return 0;
    return it->second.partitions[p].size();
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::string, Topic> topics_;
};

static void sendAll(int sock, const std::string& data) {
  const char* p = data.data();
  size_t left = data.size();
  while (left > 0) {
    ssize_t n = ::send(sock, p, left, 0);
    if (n <= 0) return;
    p += n;
    left -= n;
  }
}

static bool recvLine(int sock, std::string& line) {
  line.clear();
  char c;
  while (true) {
    ssize_t n = ::recv(sock, &c, 1, 0);
    if (n <= 0) return false;
    if (c == '\n') return true;
    line.push_back(c);
  }
}

int main(int argc, char** argv) {
  int port = (argc > 1) ? std::stoi(argv[1]) : 9099;
  Broker broker;

  // socket setup
  int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  int opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);
  if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("bind");
    return 1;
  }
  listen(server_fd, 64);
  std::cout << "mini-broker listening on " << port << "\n";

  while (true) {
    sockaddr_in cli{};
    socklen_t len = sizeof(cli);
    int sock = accept(server_fd, (sockaddr*)&cli, &len);
    if (sock < 0) continue;

    std::thread([sock, &broker]() {
      std::string line;
      while (recvLine(sock, line)) {
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;
        if (cmd == "CREATE") {
          std::string topic; size_t parts;
          if (iss >> topic >> parts) {
            bool ok = broker.createTopic(topic, parts);
            sendAll(sock, ok ? "OK\n" : "ERR exists\n");
          } else {
            sendAll(sock, "ERR bad_create\n");
          }
        } else if (cmd == "PRODUCE") {
          std::string topic; size_t p; size_t klen, vlen;
          if (!(iss >> topic >> p >> klen >> vlen)) {
            sendAll(sock, "ERR bad_produce\n"); continue;
          }
          std::string key(klen, '\0');
          std::string value(vlen, '\0');
          size_t got = 0;
          while (got < klen) {
            ssize_t n = recv(sock, &key[got], klen - got, 0);
            if (n <= 0) break; got += n;
          }
          got = 0;
          while (got < vlen) {
            ssize_t n = recv(sock, &value[got], vlen - got, 0);
            if (n <= 0) break; got += n;
          }
          auto off = broker.produce(topic, p, std::move(key), std::move(value));
          if (off) {
            sendAll(sock, "OK " + std::to_string(*off) + "\n");
          } else {
            sendAll(sock, "ERR no_topic_or_partition\n");
          }
        } else if (cmd == "CONSUME") {
          std::string topic; size_t p, from, maxn;
          if (!(iss >> topic >> p >> from >> maxn)) {
            sendAll(sock, "ERR bad_consume\n"); continue;
          }
          auto msgs = broker.consume(topic, p, from, maxn);
          sendAll(sock, "N " + std::to_string(msgs.size()) + "\n");
          for (auto& [off, m] : msgs) {
            std::ostringstream hdr;
            hdr << off << " " << m.key.size() << " " << m.value.size() << "\n";
            sendAll(sock, hdr.str());
            sendAll(sock, m.key);
            sendAll(sock, m.value);
          }
        } else if (cmd == "SIZE") {
          std::string topic; size_t p;
          if (!(iss >> topic >> p)) { sendAll(sock, "ERR bad_size\n"); continue; }
          size_t n = broker.size(topic, p);
          sendAll(sock, "OK " + std::to_string(n) + "\n");
        } else {
          sendAll(sock, "ERR unknown_cmd\n");
        }
      }
      close(sock);
    }).detach();
  }
}
