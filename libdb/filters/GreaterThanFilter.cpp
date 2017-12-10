
#include "arrow/api.h"

#include "GreaterThanFilter.h"

template <typename T>
GreaterThanFilter<T>::GreaterThanFilter(std::string column_name, typename T::ElementType value)
        : _column_name(column_name), _value(value)
{

}

template <>
void
GreaterThanFilter<db::LongType>::initialize(TableCursor& table_cursor) {
    _cursor = table_cursor.getLongColumn(_column_name);
}

template <>
void
GreaterThanFilter<db::DoubleType>::initialize(TableCursor& table_cursor) {
    _cursor = table_cursor.getDoubleColumn(_column_name);
}

template <typename T>
bool
GreaterThanFilter<T>::evaluate()
{
    return _cursor->get() > _value;
}

template class GreaterThanFilter<db::LongType>;
template class GreaterThanFilter<db::DoubleType>;