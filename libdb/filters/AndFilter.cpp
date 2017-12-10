
#include "arrow/api.h"

#include "AndFilter.h"

using namespace db;

AndFilter::AndFilter(std::string column_name, std::shared_ptr<Filter> left, std::shared_ptr<Filter> right)
        : _column_name(column_name), _left(left), _right(right)
{

}

void
AndFilter::initialize(TableCursor& table_cursor) {
    _left->initialize(table_cursor);
    _right->initialize(table_cursor);
}

bool
AndFilter::evaluate()
{
    // short circuit explicitly
    if (_left->evaluate()) {
        return _right->evaluate();
    } else {
        return false;
    }
}
