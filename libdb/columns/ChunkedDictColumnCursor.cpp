

#include <iostream>
#include "ChunkedDictColumnCursor.h"

using arrow::Int64Type;
using arrow::Int32Type;

template <typename T>
ChunkedDictColumnCursor<T>::ChunkedDictColumnCursor(std::shared_ptr<arrow::Column> column)
        : _column(std::move(column))
{
    reset();
}

template <typename T>
bool
ChunkedDictColumnCursor<T>::hasMore()
{
    return (_pos + 1) < _column->length();
}

template <typename T>
bool
ChunkedDictColumnCursor<T>::next()
{
    if ((_pos + 1) < _column->length()) {
        _pos++;
        _pos_in_chunk++;
        // may have hit the end of the current chunk
        if (_pos_in_chunk >= _current_indices->length()) {
            // invariant: if this could fail (we are ignoring the return) it would have been caught above
            // TODO: still check the invariant as it's cheap
            advance_chunk();
        }
        return true;
    } else {
        return false;
    }
}

template <typename T>
bool ChunkedDictColumnCursor<T>::isNull()
{
    return false; // TODO: handle nulls
}

template <typename T>
typename T::c_type
ChunkedDictColumnCursor<T>::get()
{
    int index = _current_indices->Value(_pos_in_chunk);
    return _current_dict->Value(index);
}

template <typename T>
void
ChunkedDictColumnCursor<T>::reset()
{
    _pos = 0;
    _chunk = 0;
    _pos_in_chunk = 0;
    std::shared_ptr<arrow::DictionaryArray> dict_array =
            std::dynamic_pointer_cast<arrow::DictionaryArray>(_column->data()->chunk(_chunk));
    std::shared_ptr<arrow::Array> indices = dict_array->indices();
    _current_indices =
            std::dynamic_pointer_cast<arrow::NumericArray<Int32Type>>(dict_array->indices());
    _current_dict =
            std::dynamic_pointer_cast<arrow::NumericArray<T>>(dict_array->dictionary());
    // TODO: this may fail if the column is empty
}

template <typename T>
bool
ChunkedDictColumnCursor<T>::seek(uint64_t to)
{
    // the key idea here is to avoid touching the memory of the intervening chunks completely
    int64_t distance = to - _pos;
    while (_pos_in_chunk + distance >= _current_indices->length()) {
        int64_t advancing =_current_dict->length() - _pos_in_chunk;
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
ChunkedDictColumnCursor<T>::advance_chunk() {
    if ((_chunk + 1) < _column->data()->num_chunks()) {
        _chunk++;
        _pos_in_chunk = 0;
        std::shared_ptr<arrow::DictionaryArray> dict_array =
                std::dynamic_pointer_cast<arrow::DictionaryArray>(_column->data()->chunk(_chunk));
        _current_indices =
                std::dynamic_pointer_cast<arrow::NumericArray<Int32Type>>(dict_array->indices());
        _current_dict =
                std::dynamic_pointer_cast<arrow::NumericArray<T>>(dict_array->dictionary());
        return true;
    } else {
        return false;
    }
}

template class ChunkedDictColumnCursor<arrow::Int64Type>;
//template class ChunkedDictColumnCursor<arrow::DoubleArray>;