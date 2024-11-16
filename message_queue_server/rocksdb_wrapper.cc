#include "rocksdb_wrapper.h"
#include <stdexcept>

RocksDBWrapper::RocksDBWrapper(const std::string& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
    if (!status.ok()) {
        throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
    }
}

RocksDBWrapper::~RocksDBWrapper() {
    delete db_;
}

void RocksDBWrapper::put(const std::string& key, const std::string& value) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to write to RocksDB: " + status.ToString());
    }
}

std::string RocksDBWrapper::get(const std::string& key) {
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to read from RocksDB: " + status.ToString());
    }
    return value;
}

std::unique_ptr<rocksdb::Iterator> RocksDBWrapper::createIterator() {
    return std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(rocksdb::ReadOptions()));
}
