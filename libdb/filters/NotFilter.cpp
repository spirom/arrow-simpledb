

#include "NotFilter.h"

using namespace db;

NotFilter::NotFilter(std::shared_ptr<Filter> underlying)
        : _underlying(underlying)
{

}

void
NotFilter::initialize(TableCursor& table_cursor) {
    _underlying->initialize(table_cursor);
}

bool
NotFilter::evaluate()
{
    return !_underlying->evaluate();
}