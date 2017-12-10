//
// Created by spiro on 11/7/17.
//

#ifndef GENERICCOLUMNCURSOR_H
#define GENERICCOLUMNCURSOR_H

#include <cstdint>

class TableCursor;

/**
 * Access to columns, controlled by a TableCursor. Obtain one of these by
 * calling getColumn on your outermost TableCursor, and use that TableCursor's hasMore()
 * method to iterate. TO get data out of one of these, cast it to the right kind of
 * ColumnCursorWrapper and call get().
 */
class GenericColumnCursor {
public:
    virtual ~GenericColumnCursor() = default;
    virtual void reset() = 0;
protected:
    explicit GenericColumnCursor(TableCursor &table_cursor);
    uint64_t get_table_cursor_position();

private:
    TableCursor &_table_cursor;
};


#endif // GENERICCOLUMNCURSOR_H
