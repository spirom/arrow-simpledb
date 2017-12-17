

#include <iostream>
#include "DBTable.h"

using arrow::Table;
using arrow::Field;

using namespace db;

DBTable::DBTable(
        std::vector<std::string> names,
        std::vector<std::shared_ptr<db::DataType>> types,
        std::vector<db::ColumnEncoding> encodings,
        arrow::MemoryPool *pool)
{

    _encodings = encodings;

    std::vector<std::shared_ptr<arrow::Field>> schema_vector;

    for (uint64_t i = 0; i < names.size(); i++) {
        std::shared_ptr<db::DataType> tp = types.at(i);
        std::shared_ptr<Field> field = arrow::field(names.at(i), tp->getArrowType());
        db::ColumnEncoding encoding = encodings.at(i);
        schema_vector.push_back(field);

        switch (tp->id()) {
            case db::ColumnType::LONG_TYPE: {
                _builders.push_back(std::make_shared<DBColumnBuilder<db::LongType>>(field, encoding, pool));
                break;
            }
            case db::ColumnType::DOUBLE_TYPE: {
                _builders.push_back(std::make_shared<DBColumnBuilder<db::DoubleType>>(field, encoding, pool));
                break;
            }
            case db::ColumnType::STRING_TYPE: {
                _builders.push_back(std::make_shared<DBColumnBuilder<db::StringType>>(field, encoding, pool));
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
DBTable::addRow(std::vector<std::shared_ptr<db::GenValue>> values)
{
    for (size_t i = 0; i < values.size(); i++) {
        auto builder = _builders.at(i);
        auto v = values.at(i);
        builder->add(v);
    }
}

void
DBTable::endChunk()
{
    for (auto builder : _builders) {
        builder->endChunk();
    }
}

void
DBTable::make() {
    _columns.clear();
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

void
DBTable::dump()
{
    for (int i = 0; i < _table->num_columns(); i++) {
        std::shared_ptr<arrow::Column> col = _table->column(i);
        std::cout << "*** COLUMN " << i << " : " << col->name() << std::endl;
        std::cout << "Num chunks: " << col->data()->num_chunks() << std::endl;
        for (int c = 0; c < col->data()->num_chunks(); c++) {
            std::cout << "Chunk " << c
                      << " length: " << col->data()->chunk(c)->length()
                      << " null count: " << col->data()->chunk(c)->null_count()
                    << std::endl;
            std::shared_ptr<arrow::DictionaryArray> da = std::dynamic_pointer_cast<arrow::DictionaryArray>(
                    col->data()->chunk(c));
            if (da == nullptr) {
                std::cout << "SIMPLE array length: " << col->data()->chunk(c) << std::endl;
            } else {
                std::cout << "CHUNKED dict array: " << da.get() << std::endl;

                std::cout << "dict array length: " << da->length() << std::endl;
                std::cout << "dict array dict length: " << da->dictionary()->length() << std::endl;
                std::cout << "dict array indices length: " << da->indices()->length() << std::endl;

                std::cout << "dict array dictionary type: " << da->dictionary()->type_id() << std::endl;

                std::cout << "dict array indices type: " << da->indices()->type_id() << std::endl;

                std::shared_ptr<arrow::StringArray> stringDict =
                        std::dynamic_pointer_cast<arrow::StringArray>(da->dictionary());
                if (stringDict == nullptr) {

                } else {
                    for (int de = 0; de < stringDict->length(); de++) {
                        std::cout << "Dict entry " << de << " : " << stringDict->GetString(de) << std:: endl;
                    }
                }

                std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> indices =
                        std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(da->indices());
                if (indices != nullptr) {
                    for (int ie = 0; ie < indices->length(); ie++) {
                        if (indices->IsNull(ie)) {
                            std::cout << "Index entry " << ie << " : IS NULL" << std:: endl;
                        } else {
                            std::cout << "Index entry " << ie << " : " << (int)indices->Value(ie) << std:: endl;
                        }

                    }
                }


            }
        }
    }


}
