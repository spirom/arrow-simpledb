//
// Created by spiro on 11/7/17.
//

#include "ColumnCursorWrapper.h"
#include "columns/ChunkedColumnCursor.h"
#include "columns/ChunkedDictColumnCursor.h"



template <typename T>
ColumnCursorWrapper<T>::ColumnCursorWrapper(
        std::shared_ptr<arrow::Column> column,
        Encoding encoding,
        TableCursor &table_cursor) : GenericColumnCursor(table_cursor)
{

    switch (encoding) {
        case ColumnCursorWrapper::Encoding::PLAIN: {
            _base_cursor =
                    std::make_shared<ChunkedColumnCursor<T>>(column);
            break;
        }
        case ColumnCursorWrapper::Encoding::DICT:
        {
            _base_cursor =
                    std::make_shared<ChunkedDictColumnCursor<T>>(column);
            break;
        }
        default:
            // do something
            break;

    }
}



template <typename T>
ColumnCursorWrapper<T>::ColumnCursorWrapper(
        std::shared_ptr<BaseColumnCursor<T>> base_cursor,
        TableCursor &table_cursor)
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
typename T::c_type
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

template class ColumnCursorWrapper<arrow::Int64Type>;
template class ColumnCursorWrapper<arrow::DoubleType>;