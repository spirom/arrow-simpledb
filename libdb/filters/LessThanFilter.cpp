

#include "LessThanFilter.h"

#include "arrow/api.h"

LessThanFilter::LessThanFilter(std::string column_name, double value)
        : _column_name(column_name), _value(value)
{

}

void
LessThanFilter::initialize(TableCursor& table_cursor) {
    _cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            table_cursor.getColumn(_column_name));
}

bool
LessThanFilter::evaluate()
{
    return _cursor->get() < _value;
}
