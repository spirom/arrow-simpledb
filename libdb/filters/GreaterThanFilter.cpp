
#include "arrow/api.h"

#include "GreaterThanFilter.h"

GreaterThanFilter::GreaterThanFilter(std::string column_name, double value)
        : _column_name(column_name), _value(value)
{

}

void
GreaterThanFilter::initialize(TableCursor& table_cursor) {
    _cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            table_cursor.getColumn(_column_name));
}

bool
GreaterThanFilter::evaluate()
{
    return _cursor->get() > _value;
}
