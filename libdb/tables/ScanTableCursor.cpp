
#include <columns/BaseColumnCursor.h>
#include "ScanTableCursor.h"

using namespace db;

ScanTableCursor::ScanTableCursor(
        std::shared_ptr<arrow::Table> table,
        std::vector<db::ColumnEncoding> encodings
    )
: _table(table)
{
    for (int i = 0; i < table->num_columns(); i++) {
        addColumn(table->column(i), table->field(i)->name(), encodings.at(i));
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
        addColumn(table->column(i), table->field(i)->name(), db::ColumnEncoding::PLAIN);
    }
    _size = (uint64_t) table->num_rows();
    reset();
}

std::shared_ptr<BaseColumnCursor<db::StringType>>
ScanTableCursor::getStringColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<BaseColumnCursor<db::StringType>>(_cursors[colName]);
}

std::shared_ptr<BaseColumnCursor<db::LongType>>
ScanTableCursor::getLongColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<BaseColumnCursor<db::LongType>>(_cursors[colName]);
}

std::shared_ptr<BaseColumnCursor<db::DoubleType>>
ScanTableCursor::getDoubleColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return std::dynamic_pointer_cast<BaseColumnCursor<db::DoubleType>>(_cursors[colName]);
}

std::shared_ptr<GenericColumnCursor>
ScanTableCursor::getColumn(std::string colName)
{
    if (_cursors.count(colName) == 0) return nullptr;
    return _cursors[colName];
}

bool
ScanTableCursor::addColumn(std::shared_ptr<arrow::ChunkedArray> column, const std::string& column_name, db::ColumnEncoding encoding)
{
    switch (column->type()->id()) {
        case arrow::Type::INT64: {
            _cursors[column_name] = BaseColumnCursor<db::LongType>::makeCursor(column, encoding, *this);
            return true;
        }
        case arrow::Type::DOUBLE: {
            _cursors[column_name] = BaseColumnCursor<db::DoubleType>::makeCursor(column, encoding, *this);
            return true;
        }
        case arrow::Type::STRING: {
            _cursors[column_name] = BaseColumnCursor<db::StringType>::makeCursor(column, encoding, *this);
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
ScanTableCursor::getPosition() const
{
    return _position;
}

