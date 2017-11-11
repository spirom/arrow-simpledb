
#include "arrow/api.h"

#include "GreaterThanFilter.h"

template <typename T>
GreaterThanFilter<T>::GreaterThanFilter(std::string column_name, typename T::value_type value)
        : _column_name(column_name), _value(value)
{

}

template <typename T>
void
GreaterThanFilter<T>::initialize(TableCursor& table_cursor) {
    _cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<T>>(
            table_cursor.getColumn(_column_name));
}

template <typename T>
bool
GreaterThanFilter<T>::evaluate()
{
    return _cursor->get() > _value;
}

template class GreaterThanFilter<arrow::Int64Array>;
template class GreaterThanFilter<arrow::DoubleArray>;