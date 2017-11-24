

#ifndef SCAN_TABLE_CURSOR_H
#define SCAN_TABLE_CURSOR_H

#include <unordered_map>
#include <arrow/table.h>
#include "columns/ColumnCursorWrapper.h"
#include "columns/GenericColumnCursor.h"
#include "TableCursor.h"


class GenericColumnCursor;

/**
 * Table cursor implementation for scanning a table.
 */
class ScanTableCursor : public TableCursor {
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

    std::shared_ptr<GenericColumnCursor> getColumn(std::string colName);

    bool hasMore();

    void reset();

    uint64_t getPosition();

protected:
    bool addColumn(std::shared_ptr<arrow::Column> column, GenericColumnCursor::Encoding encoding);

private:
    std::shared_ptr<arrow::Table> _table;
    uint64_t _position;
    uint64_t _size;
    std::unordered_map<std::string, std::shared_ptr<GenericColumnCursor>> _cursors;
    bool _initial_state;
};


#endif // SCAN_TABLE_CURSOR_H
