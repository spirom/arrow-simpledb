

#ifndef BASE_COLUMN_CURSOR_H
#define BASE_COLUMN_CURSOR_H

#include "GenericColumnCursor.h"
#include <memory>
#include "arrow/api.h"
#include "DBSchema.h"

namespace db {

    class TableCursor;


    template<typename T>
    class BaseColumnCursor : public GenericColumnCursor {
    public:
        explicit BaseColumnCursor(TableCursor &table_cursor);

        static std::shared_ptr<BaseColumnCursor<T>> makeCursor(
                std::shared_ptr<arrow::Column> column, ColumnEncoding encoding, TableCursor &table_cursor);

        /**
         * Get value at current position.
         * @return
         */
        virtual typename T::ElementType get() = 0;

    protected:

        /**
         * Seek to the given position.
         * @param to zero-based ordinal position of element in column
         * @return True if successful.
         */
        virtual bool seek(uint64_t to) = 0;

        /**
         * Will next() produce another element?
         * @return
         */
        virtual bool hasMore() = 0;

        /**
         * Move to the next element.
         * @return True if an element is available, false otherwise (end of column.)
         */
        virtual bool next() = 0;

    };
};

#endif // BASE_COLUMN_CURSOR_H
