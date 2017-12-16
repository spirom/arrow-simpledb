

#include <iostream>
#include "ChunkedColumnCursor.h"
#include "DBSchema.h"


namespace db {

template<typename T>
ChunkedColumnCursor<T>::ChunkedColumnCursor(std::shared_ptr<arrow::Column> column)
        : _column(std::move(column)) {
    // std::cout << "Cursor: [" << _column->data()->num_chunks() << "]" << std::endl;
    reset();
}

template<typename T>
bool
ChunkedColumnCursor<T>::hasMore() {
    return (_pos + 1) < _column->length();
}

template<typename T>
bool
ChunkedColumnCursor<T>::next() {
    if ((_pos + 1) < _column->length()) {
        _pos++;
        _pos_in_chunk++;
        // may have hit the end of the current chunk
        if (_pos_in_chunk >= _current_chunk->length()) {
            // invariant: if this could fail (we are ignoring the return) it would have been caught above
            // TODO: still check the invariant as it's cheap
            advance_chunk();
        }
        return true;
    } else {
        return false;
    }
}

template<typename T>
bool
ChunkedColumnCursor<T>::isNull() {
    return _current_chunk->IsNull(_pos_in_chunk);
}

template<typename T>
typename T::ElementType
ChunkedColumnCursor<T>::get() {
    return _current_chunk->Value(_pos_in_chunk);
}

template<>
typename db::StringType::ElementType
ChunkedColumnCursor<db::StringType>::get() {
    return _current_chunk->GetString(_pos_in_chunk);
}

template<typename T>
void
ChunkedColumnCursor<T>::reset() {

    _pos = 0;
    _chunk = 0;
    _pos_in_chunk = 0;
    _current_chunk =
            std::static_pointer_cast<typename T::ArrayType>(_column->data()->chunk(_chunk));
    // TODO: this may fail if the column is empty
}

template<typename T>
bool
ChunkedColumnCursor<T>::seek(uint64_t to) {
    // the key idea here is to avoid touching the memory of the intervening chunks completely
    int64_t distance = to - _pos;
    while (_pos_in_chunk + distance >= _current_chunk->length()) {
        int64_t advancing = _current_chunk->length() - _pos_in_chunk;
        distance -= advancing;
        if (!advance_chunk()) return false;
        _pos += advancing;
    }
    // invariant: there's enough data since the loop exited and advance_chunk() returned true
    _pos += distance;
    _pos_in_chunk += distance;
    //std::cout << to << " << " << _pos << " , " << _pos_in_chunk << " >>" << std::endl;
    return true;
}

template<typename T>
bool
ChunkedColumnCursor<T>::advance_chunk() {
    if ((_chunk + 1) < _column->data()->num_chunks()) {
        _chunk++;
        _pos_in_chunk = 0;
        _current_chunk =
                std::static_pointer_cast<typename T::ArrayType>(_column->data()->chunk(_chunk));
        return true;
    } else {
        return false;
    }
}

};

template class db::ChunkedColumnCursor<db::LongType>;
template class db::ChunkedColumnCursor<db::DoubleType>;
template class db::ChunkedColumnCursor<db::StringType>;