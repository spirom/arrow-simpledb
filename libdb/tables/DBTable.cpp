

#include "DBTable.h"

using arrow::Table;
using arrow::Field;

DBTable::DBTable(
        std::vector<std::string> names,
        std::vector<std::shared_ptr<arrow::DataType>> types,
        std::vector<GenericColumnCursor::Encoding> encodings)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    _encodings = encodings;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (uint64_t i = 0; i < names.size(); i++) {
        std::shared_ptr<arrow::DataType> tp = types.at(i);
        std::shared_ptr<Field> field = arrow::field(names.at(i), tp);
        schema_vector.push_back(field);
    }

    _schema = std::make_shared<arrow::Schema>(schema_vector);

    // TODO: create column builders of the right type
}

void
DBTable::addRow(std::vector<std::shared_ptr<DBGenValue>> &values)
{

}

// TODO: need a way to start a new chunk

void
DBTable::make() {
    // TODO: flush each of the column builders
    _table = std::make_shared<Table>(_schema, _columns);
}

std::shared_ptr<ScanTableCursor>
DBTable::getScanCursor()
{
    std::shared_ptr<ScanTableCursor> tc = std::make_shared<ScanTableCursor>(_table, _encodings);
    return tc;
}

std::shared_ptr<arrow::Table>
DBTable::getTable()
{
    return _table;
}