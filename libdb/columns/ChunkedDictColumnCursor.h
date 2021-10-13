

#ifndef CHUNKEDDICTCOLUMNCURSOR_H
#define CHUNKEDDICTCOLUMNCURSOR_H


#include "BaseColumnCursor.h"

#include <arrow/table.h>


namespace db {


/**
 * A simple column cursor implemented on top of a possibly chunked Arrow column, the hides the
 * chunking to present a simpel column structure. This is not directly used for executing queries.
 *
 * @tparam T The underlying Arrow array type:: for example, arrow::Int64Array.
 */
    template<typename T>
    class ChunkedDictColumnCursor : public BaseColumnCursor<T> {
    public:
        /**
         * Create from a column -- initially positioned at first element, if any.
         * @param column
         */
        explicit ChunkedDictColumnCursor(std::shared_ptr<arrow::ChunkedArray> column, TableCursor &table_cursor);

        /**
         * Will next() produce another element?
         * @return
         */
        bool hasMore();

        /**
         * Move to the next element.
         * @return True if an element is available, false otherwise (end of column.)
         */
        bool next();

        /**
         * Is the element at the current position null?
         * @return
         */
        bool isNull();

        /**
         * Get value at current position.
         * @return
         */
        typename T::ElementType get();

        /**
         * Reset to the first element, if any.
         */
        void reset();

        /**
         * Seek to the given position.
         * @param to zero-based ordinal position of element in column
         * @return True if successful.
         */
        bool seek(uint64_t to);

    protected:

        /**
         * Advance to the next chunk in the column's chunk sequence, when the values
         * in the current chunk have been exhausted.
         * @return True if successful, false if the current chunk was the last.
         */
        bool advance_chunk();

    private:

        /**
         * The underlying column
         */
        std::shared_ptr<arrow::ChunkedArray> _column;

        /**
         * The current chunk of the underlying column
         */

        std::shared_ptr<typename T::ArrayType> _current_dict;
        std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> _current_indices;

        /**
         * Offset of current chunk inthe sequence of chunks
         */
        int32_t _chunk = 0;

        /**
         * Offset within the current chunk
         */
        int64_t _pos_in_chunk = 0;

        /**
         * Position within the (logical) column.
         */
        int64_t _pos = 0;

    private:
        void populate_pointers();
    };

};


#endif // CHUNKEDDICTCOLUMNCURSOR_H
