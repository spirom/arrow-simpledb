

#ifndef COLUMN_TYPE_TRAIT_H
#define COLUMN_TYPE_TRAIT_H

#include <arrow/api.h>

template <typename T>
class ColumnTypeTrait {

};

template <>
class ColumnTypeTrait<arrow::StringType>
{
public:
    typedef typename arrow::StringArray ArrayType;
    typedef std::string ReturnType;
    typedef arrow::StringBuilder BuilderType;
    typedef arrow::StringDictionaryBuilder DictionaryBuilderType;
};


template <>
class ColumnTypeTrait<arrow::Int64Type>
{
public:
    typedef typename arrow::NumericArray<arrow::Int64Type> ArrayType;
    typedef typename arrow::Int64Type::c_type ReturnType;
    typedef arrow::Int64Builder BuilderType;
    typedef arrow::DictionaryBuilder<arrow::Int64Type> DictionaryBuilderType;
};

template <>
class ColumnTypeTrait<arrow::DoubleType>
{
public:
    typedef typename arrow::NumericArray<arrow::DoubleType> ArrayType;
    typedef typename arrow::DoubleType::c_type ReturnType;
    typedef arrow::DoubleBuilder BuilderType;
    // TODO: what if there _is_ no corresponding dictionary type ???
    typedef arrow::DoubleBuilder DictionaryBuilderType;
};

#endif // COLUMN_TYPE_TRAIT_H
