void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value)
{
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len =
    VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);

  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;

  EncodeFixed64(p, (s << 8) | type);
  p += 8;

  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);

  table_.Insert(buf);
}