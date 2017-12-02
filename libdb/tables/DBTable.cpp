

#include "DBTable.h"

using arrow::Table;
using arrow::Field;

DBTable::DBTable(
        std::vector<std::string> names,
        std::vector<std::shared_ptr<arrow::DataType>> types,
        std::vector<GenericColumnCursor::Encoding> encodings)
{

    _encodings = encodings;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (uint64_t i = 0; i < names.size(); i++) {
        std::shared_ptr<arrow::DataType> tp = types.at(i);
        std::shared_ptr<Field> field = arrow::field(names.at(i), tp);
        schema_vector.push_back(field);

        switch (tp->id()) {
            case arrow::Type::INT64: {
                _builders.push_back(std::make_shared<DBColumnBuilder<arrow::Int64Type>>(field));
                break;
            }
            case arrow::Type::DOUBLE: {
                _builders.push_back(std::make_shared<DBColumnBuilder<arrow::DoubleType>>(field));
                break;
            }
            case arrow::Type::STRING: {
                _builders.push_back(std::make_shared<DBColumnBuilder<arrow::StringType>>(field));
                break;
            }
            default:
                // TODO: handle this
                break;
        }
    }

    _schema = std::make_shared<arrow::Schema>(schema_vector);

    // TODO: create column builders of the right type
}

void
DBTable::addRow(std::vector<std::shared_ptr<DBGenValue>> values)
{

}

// TODO: need a way to start a new chunk

void
DBTable::make() {
    for (auto builder : _builders) {
        _columns.push_back(builder->getColumn());
    }
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

std::shared_ptr<DBGenValue>
DBTable::int64(int64_t i)
{
    return std::make_shared<DBValue<int64_t>>(i);
}

std::shared_ptr<DBGenValue>
DBTable::float64(double d)
{
    return std::make_shared<DBValue<double>>(d);
}
