

#include "LessThanFilter.h"

#include "arrow/api.h"

template <typename T>
LessThanFilter<T>::LessThanFilter(std::string column_name, typename T::c_type value)
        : _column_name(column_name), _value(value)
{

}

template <typename T>
void
LessThanFilter<T>::initialize(TableCursor& table_cursor) {
    _cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<T>>(
            table_cursor.getColumn(_column_name));
}

template <typename T>
bool
LessThanFilter<T>::evaluate()
{
    return _cursor->get() < _value;
}

template class LessThanFilter<arrow::Int64Type>;
template class LessThanFilter<arrow::DoubleType>;
