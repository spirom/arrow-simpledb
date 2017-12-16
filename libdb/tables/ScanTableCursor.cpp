
#include "ScanTableCursor.h"

using namespace db;

ScanTableCursor::ScanTableCursor(
        std::shared_ptr<arrow::Table> table,
        std::vector<db::ColumnEncoding> encodings
    )
: _table(table)
{
    for (int i = 0; i < table->num_columns(); i++) {
        addColumn(table->column(i), encodings.at(i));
    }
    _size = (uint64_t) table->num_rows();
    reset();
}

ScanTableCursor::ScanTableCursor(
        std::shared_ptr<arrow::Table> table
)
        : _table(table)
{
    for (int i = 0; i < table->num_columns(); i++) {
        addColumn(table->column(i), db::ColumnEncoding::PLAIN);
    }
    _size = (uint64_t) table->num_rows();
    reset();
}

std::shared_ptr<ColumnCursorWrapper<db::StringType>>
ScanTableCursor::getStringColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<ColumnCursorWrapper<db::StringType>>(_cursors[colName]);
}

std::shared_ptr<ColumnCursorWrapper<db::LongType>>
ScanTableCursor::getLongColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<ColumnCursorWrapper<db::LongType>>(_cursors[colName]);
}

std::shared_ptr<ColumnCursorWrapper<db::DoubleType>>
ScanTableCursor::getDoubleColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<ColumnCursorWrapper<db::DoubleType>>(_cursors[colName]);
}

std::shared_ptr<GenericColumnCursor>
ScanTableCursor::getColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return _cursors[colName];
}

bool
ScanTableCursor::addColumn(std::shared_ptr<arrow::Column> column, db::ColumnEncoding encoding)
{
    switch (column->type()->id()) {
        case arrow::Type::INT64: {
            _cursors[column->name()] =
                    std::make_shared<ColumnCursorWrapper<db::LongType>>(column, encoding, *this);
            return true;
        }
        case arrow::Type::DOUBLE: {
            _cursors[column->name()] =
                    std::make_shared<ColumnCursorWrapper<db::DoubleType>>(column, encoding, *this);
            return true;
        }
        case arrow::Type::STRING: {
            _cursors[column->name()] =
                    std::make_shared<ColumnCursorWrapper<db::StringType>>(column, encoding, *this);
            return true;
        }
        default:
            return false;
    }

}

bool
ScanTableCursor::hasMore()
{
    if (_size == 0) return false;
    if (_initial_state) {
        _initial_state = false;
        _position = 0;
        return true;
    }
    if ((_position + 1) >= _size) return false;
    _position++;
    return true;
}

void
ScanTableCursor::reset()
{
    _position = 0;
    _initial_state = true;
    for (auto cursor =_cursors.begin(); cursor != _cursors.end(); cursor++) {
        cursor->second->reset();
    }
}

uint64_t
ScanTableCursor::getPosition()
{
    return _position;
}

