
#include "arrow/api.h"

#include "GreaterThanFilter.h"

template <typename T>
GreaterThanFilter<T>::GreaterThanFilter(std::string column_name, typename T::c_type value)
        : _column_name(column_name), _value(value)
{

}

template <>
void
GreaterThanFilter<arrow::Int64Type>::initialize(TableCursor& table_cursor) {
    _cursor = table_cursor.getLongColumn(_column_name);
}

template <>
void
GreaterThanFilter<arrow::DoubleType>::initialize(TableCursor& table_cursor) {
    _cursor = table_cursor.getDoubleColumn(_column_name);
}

template <typename T>
bool
GreaterThanFilter<T>::evaluate()
{
    return _cursor->get() > _value;
}

template class GreaterThanFilter<arrow::Int64Type>;
template class GreaterThanFilter<arrow::DoubleType>;