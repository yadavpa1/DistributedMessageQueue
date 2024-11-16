#ifndef ROCKSDB_WRAPPER_H
#define ROCKSDB_WRAPPER_H

#include <rocksdb/db.h>
#include <string>
#include <memory>

class RocksDBWrapper {
    public:
    RocksDBWrapper(const std::string& db_path);
    ~RocksDBWrapper();
    void put(const std::string& key, const std::string& value);
    std::string get(const std::string& key);
    std::unique_ptr<rocksdb::Iterator> createIterator();

    private:
    rocksdb::DB* db_;
};

#endif // ROCKSDB_WRAPPER_H