#include "storage_engine.h"
#include "leveldb/db.h"

namespace kv {

// Use leveldb as a storage engine
class LevelDBStorageEngine final : public StorageEngine {
 public:
  explicit LevelDBStorageEngine(const std::string& dbname) {
    leveldb::Options options;
    options.create_if_missing = true;

    auto stat = leveldb::DB::Open(options, dbname, &dbptr_);
    assert(stat.ok());
  }

  ~LevelDBStorageEngine() {
    delete dbptr_;
  }

  bool Put(const std::string& key, const std::string& value) override {
    auto stat = dbptr_->Put(leveldb::WriteOptions(), key, value);
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
 private:
  leveldb::DB* dbptr_;
};

StorageEngine* StorageEngine::Default(const std::string& dbname) {
  return new LevelDBStorageEngine(dbname);
}
}
