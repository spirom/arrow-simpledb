

#include <columns/DBSchema.h>
#include "FilterProjectTableCursor.h"
#include "filters/Filter.h"
#include "columns/BaseColumnCursor.h"

using namespace db;

FilterProjectTableCursor::FilterProjectTableCursor(TableCursor &source_cursor, std::shared_ptr<Filter> &filter)
        : _source_cursor(source_cursor), _filter(filter)
{
    reset();
    filter->initialize(*this);
}

std::shared_ptr<BaseColumnCursor<db::StringType>>
FilterProjectTableCursor::getStringColumn(std::string colName)
{
    return std::dynamic_pointer_cast<BaseColumnCursor<db::StringType>>(_source_cursor.getStringColumn(colName));
}

std::shared_ptr<BaseColumnCursor<db::LongType>>
FilterProjectTableCursor::getLongColumn(std::string colName)
{
    return std::dynamic_pointer_cast<BaseColumnCursor<db::LongType>>(_source_cursor.getLongColumn(colName));
}

std::shared_ptr<BaseColumnCursor<db::DoubleType>>
FilterProjectTableCursor::getDoubleColumn(std::string colName)
{
    return std::dynamic_pointer_cast<BaseColumnCursor<db::DoubleType>>(_source_cursor.getDoubleColumn(colName));
}

std::shared_ptr<GenericColumnCursor>
FilterProjectTableCursor::getColumn(std::string colName)
{
    return _source_cursor.getColumn(colName);
}

bool
FilterProjectTableCursor::hasMore()
{
    while (_source_cursor.hasMore()) {
        if (satisfiesFilter()) return true;
    }
    return false;
}

void FilterProjectTableCursor::reset()
{
    _source_cursor.reset();
}

uint64_t
FilterProjectTableCursor::getPosition()
{
    return _source_cursor.getPosition();
}

bool
FilterProjectTableCursor::satisfiesFilter()
{
    return _filter->evaluate();
}