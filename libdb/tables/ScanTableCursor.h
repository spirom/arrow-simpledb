

#ifndef SCAN_TABLE_CURSOR_H
#define SCAN_TABLE_CURSOR_H

#include <unordered_map>
#include <arrow/table.h>
#include "columns/ColumnCursorWrapper.h"
#include "columns/GenericColumnCursor.h"
#include "TableCursor.h"
#include <columns/DBSchema.h>


class GenericColumnCursor;
class TableTest;

/**
 * Table cursor implementation for scanning a table. Get this by calling getScanCursor() on a DBTable.
 */
class ScanTableCursor : public TableCursor {
    friend class TableTest;
public:
    /**
     * Overload for specifying non-default column encodings.
     * @param table
     * @param encodings
     */
    explicit ScanTableCursor(
            std::shared_ptr<arrow::Table> table,
            std::vector<GenericColumnCursor::Encoding> encodings
    );

    /**
     * Overload using default (PLAIN) column encodings
     * @param table
     */
    explicit ScanTableCursor(
            std::shared_ptr<arrow::Table> table
    );

    std::shared_ptr<ColumnCursorWrapper<db::StringType>> getStringColumn(std::string colName) override;

    std::shared_ptr<ColumnCursorWrapper<db::LongType>> getLongColumn(std::string colName) override;

    std::shared_ptr<ColumnCursorWrapper<db::DoubleType>> getDoubleColumn(std::string colName) override;

    bool hasMore() override;

    void reset() override;

protected:
    bool addColumn(std::shared_ptr<arrow::Column> column, GenericColumnCursor::Encoding encoding);

    uint64_t getPosition();

private:
    std::shared_ptr<arrow::Table> _table;
    uint64_t _position;
    uint64_t _size;
    std::unordered_map<std::string, std::shared_ptr<GenericColumnCursor>> _cursors;
    bool _initial_state;
};


#endif // SCAN_TABLE_CURSOR_H
