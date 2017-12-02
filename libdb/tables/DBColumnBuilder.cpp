

#include <iostream>
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
void
DBColumnBuilder<T>::endChunk()
{
    if (_builder.length() != 0) {
        std::shared_ptr<arrow::Array> array;
        _builder.Finish(&array);
        _chunks.push_back(array);
    }
}


template <typename T>
std::shared_ptr<arrow::Column>
DBColumnBuilder<T>::getColumn()
{
    endChunk(); // just in case it wasn't explicitly called
    // std::cout << "Finishing [" << _chunks.size() << "]" << std::endl;
    return std::make_shared<arrow::Column>(_field, _chunks);
}

template class DBColumnBuilder<arrow::Int64Type>;
template class DBColumnBuilder<arrow::DoubleType>;
template class DBColumnBuilder<arrow::StringType>;