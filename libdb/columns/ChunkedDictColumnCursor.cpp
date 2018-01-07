

#include <iostream>
#include "ChunkedDictColumnCursor.h"
#include "core/DBSchema.h"

using arrow::Int64Type;
using arrow::Int8Type;


namespace db {

template<typename T>
ChunkedDictColumnCursor<T>::ChunkedDictColumnCursor(std::shared_ptr<arrow::Column> column, TableCursor &table_cursor)
        : BaseColumnCursor<T>(table_cursor), _column(column) {
    reset();
}

template<typename T>
bool
ChunkedDictColumnCursor<T>::hasMore() {
    return (_pos + 1) < _column->length();
}

template<typename T>
bool
ChunkedDictColumnCursor<T>::next() {
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

template<typename T>
bool ChunkedDictColumnCursor<T>::isNull() {
    seek(this->get_table_cursor_position());
    return _current_indices->IsNull(_pos_in_chunk);
}

template<typename T>
typename T::ElementType
ChunkedDictColumnCursor<T>::get() {
    seek(this->get_table_cursor_position());
    int index = _current_indices->Value(_pos_in_chunk);
    return _current_dict->Value(index);
}

template<>
typename db::StringType::ElementType
ChunkedDictColumnCursor<db::StringType>::get() {
    seek(this->get_table_cursor_position());
    int index = _current_indices->Value(_pos_in_chunk);
    return _current_dict->GetString(index);
}

template<typename T>
void
ChunkedDictColumnCursor<T>::populate_pointers() {
    std::shared_ptr<arrow::DictionaryArray> dict_array =
            std::dynamic_pointer_cast<arrow::DictionaryArray>(_column->data()->chunk(_chunk));
    _current_indices =
            std::dynamic_pointer_cast<arrow::NumericArray<Int8Type>>(dict_array->indices());
    _current_dict =
            std::dynamic_pointer_cast<typename T::ArrayType>(dict_array->dictionary());
}

template<typename T>
void
ChunkedDictColumnCursor<T>::reset() {
    _pos = 0;
    _chunk = 0;
    _pos_in_chunk = 0;
    populate_pointers();
    // TODO: this may fail if the column is empty
}

template<typename T>
bool
ChunkedDictColumnCursor<T>::seek(uint64_t to) {
    // the key idea here is to avoid touching the memory of the intervening chunks completely
    int64_t distance = to - _pos;
    while (_pos_in_chunk + distance >= _current_indices->length()) {
        int64_t advancing = _current_indices->length() - _pos_in_chunk;
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
ChunkedDictColumnCursor<T>::advance_chunk() {
    if ((_chunk + 1) < _column->data()->num_chunks()) {
        _chunk++;
        _pos_in_chunk = 0;
        populate_pointers();
        return true;
    } else {
        return false;
    }
}

};

template class db::ChunkedDictColumnCursor<db::LongType>;
template class db::ChunkedDictColumnCursor<db::DoubleType>;
template class db::ChunkedDictColumnCursor<db::StringType>;