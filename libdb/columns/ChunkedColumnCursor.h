

#ifndef CHUNKEDCOLUMNCURSOR_H
#define CHUNKEDCOLUMNCURSOR_H


#include <arrow/table.h>

template <typename T>
class ChunkedColumnCursor {
public:
    explicit ChunkedColumnCursor(std::shared_ptr<arrow::Column> column);
    bool hasMore();
    bool next();
    bool isNull();
    typename T::value_type get();
    void reset();
    bool seek(uint64_t to);
protected:
    bool advance_chunk();
private:
    std::shared_ptr<arrow::Column> _column;
    std::shared_ptr<T> _current_chunk;
    int32_t _chunk = 0;
    int64_t _pos_in_chunk = 0;
    int64_t _pos = 0;
};


#endif // CHUNKEDCOLUMNCURSOR_H
