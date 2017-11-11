

#ifndef COLUMNCURSORWRAPPER_H
#define COLUMNCURSORWRAPPER_H

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "GenericColumnCursor.h"

class ScanTableCursor;

template <typename T>
class ColumnCursorWrapper : public GenericColumnCursor {
public:
    explicit ColumnCursorWrapper(
            std::shared_ptr<ChunkedColumnCursor<T>> base_cursor,
            ScanTableCursor &table_cursor);

    bool isNull();

    typename T::value_type get();

    void reset();

private:
    std::shared_ptr<ChunkedColumnCursor<T>> _base_cursor;

};


#endif // COLUMNCURSORWRAPPER_H
