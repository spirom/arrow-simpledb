

#ifndef DB_COLUMN_BUILDER_H
#define DB_COLUMN_BUILDER_H

#include <columns/ColumnTypeTrait.h>

class DBGenValue {

public:

    virtual ~DBGenValue() = default;

};

template <typename T>
class DBValue : public DBGenValue {

public:

    DBValue(T value) : _value(value) {};

    T get() { return _value; }

private:
    T _value;
};

class DBGenColumnBuilder {

public:

    virtual std::shared_ptr<arrow::Column> getColumn() = 0;

    virtual void add(std::shared_ptr<DBGenValue> value) = 0;

    virtual ~DBGenColumnBuilder() = default;
};

template <typename T>
class DBColumnBuilder : public ColumnTypeTrait<T>, public DBGenColumnBuilder {

public:

    explicit DBColumnBuilder( std::shared_ptr<arrow::Field> field);

    virtual void add(std::shared_ptr<DBGenValue> value);

    std::shared_ptr<arrow::Column> getColumn() override;

protected:

    void add(typename ColumnTypeTrait<T>::ReturnType element);

private:

    typename ColumnTypeTrait<T>::BuilderType _builder;

    std::shared_ptr<arrow::Field> _field;
};


#endif // DB_COLUMN_BUILDER_H
