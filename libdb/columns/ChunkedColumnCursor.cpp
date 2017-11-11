

#include <iostream>
#include "ChunkedColumnCursor.h"

template <typename T>
ChunkedColumnCursor<T>::ChunkedColumnCursor(std::shared_ptr<arrow::Column> column)
        : _column(std::move(column))
{
    reset();

}

template <typename T>
bool
ChunkedColumnCursor<T>::hasMore()
{
    return (_pos + 1) < _column->length();
}

template <typename T>
bool
ChunkedColumnCursor<T>::next()
{
    if ((_pos + 1) < _column->length()) {
        _pos++;
        _pos_in_chunk++;
        if (_pos_in_chunk >= _current_chunk->length()) {
            advance_chunk();
        }
        return true;
    } else {
        return false;
    }
}

template <typename T>
bool ChunkedColumnCursor<T>::isNull()
{
    return false; // TODO: handle nulls
}

template <typename T>
typename T::value_type
ChunkedColumnCursor<T>::get()
{
    return _current_chunk->Value(_pos_in_chunk);
}

template <typename T>
void
ChunkedColumnCursor<T>::reset()
{
    _pos = 0;
    _chunk = 0;
    _pos_in_chunk = 0;
    _current_chunk =
            std::static_pointer_cast<T>(_column->data()->chunk(_chunk));
}

template <typename T>
bool
ChunkedColumnCursor<T>::seek(uint64_t to)
{
    int64_t distance = to - _pos;
    while (_pos_in_chunk + distance >= _current_chunk->length()) {
        int64_t advancing =_current_chunk->length() - _pos_in_chunk;
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

template <typename T>
bool
ChunkedColumnCursor<T>::advance_chunk() {
    if ((_chunk + 1) < _column->data()->num_chunks()) {
        _chunk++;
        _pos_in_chunk = 0;
        _current_chunk =
                std::static_pointer_cast<T>(_column->data()->chunk(_chunk));
        return true;
    } else {
        return false;
    }
}

template class ChunkedColumnCursor<arrow::Int64Array>;
template class ChunkedColumnCursor<arrow::DoubleArray>;