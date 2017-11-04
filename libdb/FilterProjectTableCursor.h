

#ifndef FILTERPROJECTTABLECURSOR_H
#define FILTERPROJECTTABLECURSOR_H

#include <arrow/table.h>
#include "ColumnCursorWrapper.h"
#include "TableCursor.h"
#include "Filter.h"

class FilterProjectTableCursor : public TableCursor {
public:
    explicit FilterProjectTableCursor(TableCursor& source_cursor, std::shared_ptr<Filter> &filter);

    std::shared_ptr<GenericColumnCursor> getColumn(std::string colName) override;

    bool hasMore() override;

    void reset() override;

    uint64_t getPosition() override;

protected:

    bool satisfiesFilter();

private:
    TableCursor& _source_cursor;
    std::shared_ptr<Filter> _filter;

};


#endif // FILTERPROJECTTABLECURSOR_H
