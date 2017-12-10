

#ifndef TABLE_CURSOR_H
#define TABLE_CURSOR_H


#include <cstdint>
#include <memory>

#include <arrow/api.h>

class GenericColumnCursor;
class FilterProjectTableCursor;

template <typename T>
class ColumnCursorWrapper;

/**
 * Common base of all table cursors, exposing row iteration and access to columns.
 */
class TableCursor {

    friend class GenericColumnCursor;
    friend class FilterProjectTableCursor;
public:

    /**
     * Get access to the specified String type column, always synchronized with this cursor's position.
     * @param colName
     * @return
     */
    virtual std::shared_ptr<ColumnCursorWrapper<arrow::StringType>> getStringColumn(std::string colName) = 0;

    /**
     * Get access to the specified Long type column, always synchronized with this cursor's position.
     * @param colName
     * @return
     */
    virtual std::shared_ptr<ColumnCursorWrapper<arrow::Int64Type>> getLongColumn(std::string colName) = 0;

    /**
     * Get access to the specified Double type column, always synchronized with this cursor's position.
     * @param colName
     * @return
     */
    virtual std::shared_ptr<ColumnCursorWrapper<arrow::DoubleType>> getDoubleColumn(std::string colName) = 0;

    /**
     * True if there are more rows.
     * @return
     */
    virtual bool hasMore() = 0;

    /**
     * Reaset to start and ready to iterate again.
     */
    virtual void reset() = 0;

    virtual ~TableCursor() = default;

protected:
    virtual uint64_t getPosition() = 0;
};


#endif // TABLE_CURSOR_H
