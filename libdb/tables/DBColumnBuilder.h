

#ifndef DB_COLUMN_BUILDER_H
#define DB_COLUMN_BUILDER_H

#include <columns/ColumnTypeTrait.h>

class DBGenColumnBuilder {
public:
    virtual std::shared_ptr<arrow::Column> getColumn() = 0;
    virtual ~DBGenColumnBuilder() = default;
};

template <typename T>
class DBColumnBuilder : public ColumnTypeTrait<T>, public DBGenColumnBuilder {
public:
    explicit DBColumnBuilder( std::shared_ptr<arrow::Field> field);
    void add(typename ColumnTypeTrait<T>::ReturnType element);

    std::shared_ptr<arrow::Column> getColumn() override;
private:
    typename ColumnTypeTrait<T>::BuilderType _builder;
    std::shared_ptr<arrow::Field> _field;
};


#endif // DB_COLUMN_BUILDER_H
