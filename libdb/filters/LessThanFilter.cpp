

#include "LessThanFilter.h"

#include "arrow/api.h"

namespace db {

template<typename T>
LessThanFilter<T>::LessThanFilter(std::string column_name, typename T::ElementType value)
        : _column_name(column_name), _value(value) {

}

template<>
void
LessThanFilter<db::LongType>::initialize(TableCursor &table_cursor) {
    _cursor = table_cursor.getLongColumn(_column_name);
}

template<>
void
LessThanFilter<db::DoubleType>::initialize(TableCursor &table_cursor) {
    _cursor = table_cursor.getDoubleColumn(_column_name);
}

template<typename T>
bool
LessThanFilter<T>::evaluate() {
    return _cursor->get() < _value;
}

};

template class db::LessThanFilter<db::LongType>;
template class db::LessThanFilter<db::DoubleType>;
