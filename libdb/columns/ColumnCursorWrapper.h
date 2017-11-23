

#ifndef COLUMNCURSORWRAPPER_H
#define COLUMNCURSORWRAPPER_H

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "GenericColumnCursor.h"

class TableCursor;

/**
 * This cursor is actually used for executing queries. Notice it only has the
 * most minimal methods for cursor positioning, since it gets its position from the
 * table cursor it belongs to.
 *
 * @tparam T The underlying Arrow element type for the array, :: for example, arrow::Int64Type.
 */
template <typename T>
class ColumnCursorWrapper : public GenericColumnCursor {
public:
    explicit ColumnCursorWrapper(
            std::shared_ptr<BaseColumnCursor<T>> base_cursor,
            TableCursor &table_cursor);

    bool isNull();

    typename T::c_type get();

    void reset();

private:
    std::shared_ptr<BaseColumnCursor<T>> _base_cursor;

};


#endif // COLUMNCURSORWRAPPER_H
