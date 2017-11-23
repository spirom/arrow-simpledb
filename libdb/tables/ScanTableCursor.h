

#ifndef SCANTABLECURSOR_H
#define SCANTABLECURSOR_H

#include <unordered_map>
#include <arrow/table.h>
#include "columns/ColumnCursorWrapper.h"
#include "TableCursor.h"


class GenericColumnCursor;

class ScanTableCursor : public TableCursor {
public:
    explicit ScanTableCursor(
            std::shared_ptr<arrow::Table> table,
            std::vector<GenericColumnCursor::Encoding> encodings
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


#endif // SCANTABLECURSOR_H
