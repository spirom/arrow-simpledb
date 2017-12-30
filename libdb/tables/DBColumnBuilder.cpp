

#include <iostream>
#include <columns/GenericColumnCursor.h>
#include <columns/DBSchema.h>
#include "DBColumnBuilder.h"

using namespace db;

template <typename T>
DBColumnBuilder<T>::DBColumnBuilder(std::shared_ptr<arrow::Field> field,
                                    db::ColumnEncoding encoding,
                                    arrow::MemoryPool *pool)
{
    _encoding = encoding;
    if (encoding == db::ColumnEncoding::DICT) {
        _dictBuilder.reset(new typename T::DictionaryBuilderType(pool));
    } else {
        _builder.reset(new typename T::BuilderType(pool));
    }
    _haveData = false;
    _field = field;
}

template < typename T>
void
DBColumnBuilder<T>::add(std::shared_ptr<db::GenValue> value) {
    // it may be a NullValue
    if (std::dynamic_pointer_cast<db::NullValue>(value) != nullptr) {
        addNull();
    } else {
        add(std::dynamic_pointer_cast<db::Value<typename T::ElementType>>(value)->get());
    }
}

template <typename T>
void
DBColumnBuilder<T>::addNull()
{
    _haveData = true;
    if (_encoding == db::ColumnEncoding::DICT) {
        _dictBuilder->AppendNull();
    } else {
        _builder->AppendNull();
    }
}

template <typename T>
void
DBColumnBuilder<T>::add(typename T::ElementType element)
{
    _haveData = true;
    if (_encoding == db::ColumnEncoding::DICT) {
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
        if (_encoding == db::ColumnEncoding::DICT) {
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

template class db::DBColumnBuilder<db::LongType>;
template class db::DBColumnBuilder<db::DoubleType>;
template class db::DBColumnBuilder<db::StringType>;