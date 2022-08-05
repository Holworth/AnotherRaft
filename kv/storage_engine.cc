#include "storage_engine.h"

#include <iostream>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "rocksdb/db.h"

namespace kv {

// Use leveldb as a storage engine
class LevelDBEngine final : public StorageEngine {
 public:
  explicit LevelDBEngine(const std::string& dbname) {
    leveldb::Options options;
    options.create_if_missing = true;

    auto stat = leveldb::DB::Open(options, dbname, &dbptr_);
    if (!stat.ok()) {
      std::cout << stat.ToString() << std::endl;
    }
    assert(stat.ok());
  }

  ~LevelDBEngine() { delete dbptr_; }

  std::string EngineName() const override {
    return std::string("LevelDBEngine");
  }

  bool Put(const std::string& key, const std::string& value) override {
    // NOTE: Should we use wo.sync=true or wo.sync=false, there is a huge performance
    // difference between these two choices
    auto wo = leveldb::WriteOptions();
    wo.sync = false;
    auto stat = dbptr_->Put(wo, key, value);
    return stat.ok();
  }

  bool Delete(const std::string& key) override {
    auto stat = dbptr_->Delete(leveldb::WriteOptions(), key);
    return stat.ok();
  }

  bool Get(const std::string& key, std::string* value) override {
    auto stat = dbptr_->Get(leveldb::ReadOptions(), key, value);
    return stat.ok();
  }

  void Close() override {
    // delete dbptr_;
  }

 private:
  leveldb::DB* dbptr_;
};

class RocksDBEngine final : public StorageEngine {
 public:
  explicit RocksDBEngine(const std::string& dbname) {
    rocksdb::Options options;
    options.create_if_missing = true;

    auto stat = rocksdb::DB::Open(options, dbname, &dbptr_);
    if (!stat.ok()) {
      std::cout << stat.ToString() << std::endl;
    }
    assert(stat.ok());
  }

  ~RocksDBEngine() { delete dbptr_; }

  std::string EngineName() const override {
    return std::string("RocksDBEngine");
  }

  bool Put(const std::string& key, const std::string& value) override {
    // NOTE: Should we use wo.sync=true or wo.sync=false, there is a huge performance
    // difference between these two choices
    auto wo = rocksdb::WriteOptions();
    wo.sync = false;
    auto stat = dbptr_->Put(wo, key, value);
    return stat.ok();
  }

  bool Delete(const std::string& key) override {
    auto stat = dbptr_->Delete(rocksdb::WriteOptions(), key);
    return stat.ok();
  }

  bool Get(const std::string& key, std::string* value) override {
    auto stat = dbptr_->Get(rocksdb::ReadOptions(), key, value);
    return stat.ok();
  }

  void Close() override { dbptr_->Close(); }

 private:
  rocksdb::DB* dbptr_;
};

StorageEngine* StorageEngine::Default(const std::string& dbname) {
  return NewRocksDBEngine(dbname);
}

StorageEngine* StorageEngine::NewLevelDBEngine(const std::string& name) {
  return new LevelDBEngine(name);
}

StorageEngine* StorageEngine::NewRocksDBEngine(const std::string& name) {
  return new RocksDBEngine(name);
}
}  // namespace kv
