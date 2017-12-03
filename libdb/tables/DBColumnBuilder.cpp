

#include <iostream>
#include <columns/GenericColumnCursor.h>
#include "DBColumnBuilder.h"

template <typename T>
DBColumnBuilder<T>::DBColumnBuilder(std::shared_ptr<arrow::Field> field,
                                    GenericColumnCursor::Encoding encoding)
{
    _encoding = encoding;
    if (encoding == GenericColumnCursor::Encoding::DICT) {
        _dictBuilder.reset(new typename ColumnTypeTrait<T>::DictionaryBuilderType(arrow::default_memory_pool()));
    } else {
        _builder.reset(new typename ColumnTypeTrait<T>::BuilderType());
    }
    _haveData = false;
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
    _haveData = true;
    if (_encoding == GenericColumnCursor::Encoding::DICT) {
        _dictBuilder->Append(element);
    } else {
        _builder->Append(element);
    }
}

template <typename T>
void
DBColumnBuilder<T>::endChunk()
{
    // Arrow doesn't like to create columns with no chunks so if we don't have any at all
    // we create an empty one
    if (_haveData || _chunks.size() == 0) {
        if (_encoding == GenericColumnCursor::Encoding::DICT) {
            std::shared_ptr<arrow::Array> array;
            _dictBuilder->Finish(&array);
            _chunks.push_back(array);
        } else {
            std::shared_ptr<arrow::Array> array;
            _builder->Finish(&array);
            _chunks.push_back(array);
        }
        _haveData = false;
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