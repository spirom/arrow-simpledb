

#ifndef CHUNKEDCOLUMNCURSOR_H
#define CHUNKEDCOLUMNCURSOR_H

#include <arrow/table.h>
#include "BaseColumnCursor.h"



/**
 * A simple column cursor implemented on top of a possibly chunked Arrow column, the hides the
 * chunking to present a simpel column structure. This is not directly used for executing queries.
 *
 * @tparam T The underlying Arrow array type:: for example, arrow::Int64Array.
 */
template <typename T>
class ChunkedColumnCursor : public BaseColumnCursor<T> {
public:
    /**
     * Create from a column -- initially positioned at first element, if any.
     * @param column
     */
    explicit ChunkedColumnCursor(std::shared_ptr<arrow::Column> column);

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
    typename T::ReturnType get();

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
    std::shared_ptr<arrow::Column> _column;

    /**
     * The current chunk of the underlying column
     */
    std::shared_ptr<typename T::ArrayType> _current_chunk;

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
};


#endif // CHUNKEDCOLUMNCURSOR_H
