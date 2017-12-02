

#ifndef DB_TABLE_H
#define DB_TABLE_H


#include <columns/GenericColumnCursor.h>
#include <vector>
#include <arrow/api.h>
#include "ScanTableCursor.h"
#include "DBColumnBuilder.h"

class DBGenValue {};

template <typename T>
class DBValue : public DBGenValue {
public:
    DBValue(T value) : _value(value) {};
private:
    T _value;
};


class DBTable {
public:
    explicit DBTable(
            std::vector<std::string> names,
            std::vector<std::shared_ptr<arrow::DataType>> types,
            std::vector<GenericColumnCursor::Encoding> encodings);

    void make();

    std::shared_ptr<ScanTableCursor> getScanCursor();

    std::shared_ptr<arrow::Table> getTable();

    void addRow(std::vector<std::shared_ptr<DBGenValue>> values);

    static std::shared_ptr<DBGenValue> int64(int64_t i);
    static std::shared_ptr<DBGenValue> float64(double d);


private:
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<arrow::Table> _table;
    std::vector<GenericColumnCursor::Encoding> _encodings;
    std::vector<std::shared_ptr<arrow::Column>> _columns;

    std::vector<std::shared_ptr<DBGenColumnBuilder>> _builders;
};


#endif // DB_TABLE_H
