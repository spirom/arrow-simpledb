

#include "ColumnCursorWrapper.h"
#include "columns/ChunkedColumnCursor.h"
#include "columns/ChunkedDictColumnCursor.h"

using namespace db;

template <typename T>
ColumnCursorWrapper<T>::ColumnCursorWrapper(
        std::shared_ptr<arrow::Column> column,
        db::ColumnEncoding encoding,
        TableCursor &table_cursor) : GenericColumnCursor(table_cursor)
{

    switch (encoding) {
        case db::ColumnEncoding::PLAIN: {
            _base_cursor =
                    std::make_shared<ChunkedColumnCursor<T>>(column);
            break;
        }
        case db::ColumnEncoding::DICT:
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
    _base_cursor->seek(get_table_cursor_position());
    return _base_cursor->isNull();
}

template <typename T>
typename T::ElementType
ColumnCursorWrapper<T>::get()
{
    _base_cursor->seek(get_table_cursor_position());
    return _base_cursor->get();
}

template <typename T>
void ColumnCursorWrapper<T>::reset()
{
    _base_cursor->reset();
}

template class ColumnCursorWrapper<db::LongType>;
template class ColumnCursorWrapper<db::DoubleType>;
template class ColumnCursorWrapper<db::StringType>;