

#include <iostream>
#include <columns/GenericColumnCursor.h>
#include <core/DBSchema.h>
#include <core/Status.h>
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
Status
DBColumnBuilder<T>::add(std::shared_ptr<db::GenValue> value) {
    // it may be a NullValue
    if (std::dynamic_pointer_cast<db::NullValue>(value) != nullptr) {
        DB_RETURN_NOT_OK(addNull());
    } else {
        DB_RETURN_NOT_OK(add(std::dynamic_pointer_cast<db::Value<typename T::ElementType>>(value)->get()));
    }
    return Status::OK();
}

template <typename T>
Status
DBColumnBuilder<T>::addNull()
{
    _haveData = true;
    if (_encoding == db::ColumnEncoding::DICT) {
        arrow::Status status = _dictBuilder->AppendNull();
        if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
    } else {
        arrow::Status status = _builder->AppendNull();
        if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
    }
    return Status::OK();
}

template <typename T>
Status
DBColumnBuilder<T>::add(typename T::ElementType element)
{
    _haveData = true;
    if (_encoding == db::ColumnEncoding::DICT) {
        arrow::Status status = _dictBuilder->Append(element);
        if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
    } else {
        arrow::Status status = _builder->Append(element);
        if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
    }
    return Status::OK();
}

template <typename T>
Status
DBColumnBuilder<T>::endChunk()
{
    // Arrow doesn't like to create columns with no chunks so if we don't have any at all
    // we create an empty one
    if (_haveData || _chunks.size() == 0) {
        if (_encoding == db::ColumnEncoding::DICT) {
            std::shared_ptr<arrow::Array> array;
            arrow::Status status = _dictBuilder->Finish(&array);
            if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
            _chunks.push_back(array);
        } else {
            std::shared_ptr<arrow::Array> array;
            arrow::Status status = _builder->Finish(&array);
            if (!status.ok()) return db::Status(db::StatusCode::OutOfMemory);
            _chunks.push_back(array);
        }
        _haveData = false;
    }
    return Status::OK();
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