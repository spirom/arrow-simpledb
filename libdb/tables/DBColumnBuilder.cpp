

#include "DBColumnBuilder.h"

template <typename T>
DBColumnBuilder<T>::DBColumnBuilder(std::shared_ptr<arrow::Field> field)
{
    _field = field;
}

template <typename T>
void
DBColumnBuilder<T>::add(typename ColumnTypeTrait<T>::ReturnType element)
{

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