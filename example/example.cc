#include <cassert>
#include <leveldb/db.h>
#include <iostream>
using namespace std;

int main() {
    leveldb::DB* db;

    //open
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "test", &db);
    assert(status.ok());

    //write
    status = db->Put(leveldb::WriteOptions(), "k1", "v1");
    assert(status.ok());

    //read
    string value;
    status = db->Get(leveldb::ReadOptions(), "k1", &value);
    assert(status.ok());
    std::cout << "k1: " << value << std::endl;

    //close
    delete db;
    return 0;
}