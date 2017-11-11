//
// Created by spiro on 11/7/17.
//

#include "ColumnCursorWrapper.h"

template <typename T>
ColumnCursorWrapper<T>::ColumnCursorWrapper(
        std::shared_ptr<ChunkedColumnCursor<T>> base_cursor,
        ScanTableCursor &table_cursor)
        : GenericColumnCursor(table_cursor), _base_cursor(base_cursor)
{

}

template <typename T>
bool
ColumnCursorWrapper<T>::isNull()
{
    _base_cursor->seek(get_table_cursor().getPosition());
    return _base_cursor->isNull();
}

template <typename T>
typename T::value_type
ColumnCursorWrapper<T>::get()
{
    _base_cursor->seek(get_table_cursor().getPosition());
    return _base_cursor->get();
}

template <typename T>
void ColumnCursorWrapper<T>::reset()
{
    _base_cursor->reset();
}

template class ColumnCursorWrapper<arrow::Int64Array>;
template class ColumnCursorWrapper<arrow::DoubleArray>;