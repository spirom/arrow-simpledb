

#ifndef FILTER_PROJECT_TABLECURSOR_H
#define FILTER_PROJECT_TABLECURSOR_H

#include "TableCursor.h"

class Filter;

/**
 * Cursor for filtering and projecting a table. Compose with some other cursor.
 * Compose filters from classes like GreaterTHanFilter, LessThanFilter and AndFilter.
 */
class FilterProjectTableCursor : public TableCursor {
public:
    explicit FilterProjectTableCursor(TableCursor& source_cursor, std::shared_ptr<Filter> &filter);

    std::shared_ptr<GenericColumnCursor> getColumn(std::string colName) override;

    bool hasMore() override;

    void reset() override;

protected:
    uint64_t getPosition() override;

    bool satisfiesFilter();

private:
    TableCursor& _source_cursor;
    std::shared_ptr<Filter> _filter;

};


#endif // FILTERPROJECTTABLECURSOR_H
