

#include "DBColumnBuilder.h"

template <typename T>
DBColumnBuilder<T>::DBColumnBuilder(std::shared_ptr<arrow::Field> field)
{
    _field = field;
}

template < typename T>
void
DBColumnBuilder<T>::add(std::shared_ptr<DBGenValue> value) {
    add(std::dynamic_pointer_cast<DBValue<typename ColumnTypeTrait<T>::ReturnType>>(value)->get());
}


template <typename T>
void
DBColumnBuilder<T>::add(typename ColumnTypeTrait<T>::ReturnType element)
{
    _builder.Append(element);
}

template <typename T>
std::shared_ptr<arrow::Column>
DBColumnBuilder<T>::getColumn()
{
    std::shared_ptr<arrow::Array> array;
    _builder.Finish(&array);
    return std::make_shared<arrow::Column>(_field, array);
}

template class DBColumnBuilder<arrow::Int64Type>;
template class DBColumnBuilder<arrow::DoubleType>;
template class DBColumnBuilder<arrow::StringType>;