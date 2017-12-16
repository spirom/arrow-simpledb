

#ifndef GENERIC_COLUMN_CURSOR_H
#define GENERIC_COLUMN_CURSOR_H

#include <cstdint>

namespace db {

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

        /**
         * Is the element at the current position null?
         * @return
         */
        virtual bool isNull() = 0;

        /**
         * Reset to the first element, if any.
         */
        virtual void reset() = 0;

    protected:
        explicit GenericColumnCursor(TableCursor &table_cursor);

        uint64_t get_table_cursor_position();

    private:
        TableCursor &_table_cursor;
    };

};


#endif // GENERIC_COLUMN_CURSOR_H
