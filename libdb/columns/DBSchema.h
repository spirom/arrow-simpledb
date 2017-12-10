

#ifndef DB_SCHEMA_H
#define DB_SCHEMA_H

#include <arrow/api.h>

namespace db {

    enum ColumnType {
        STRING_TYPE, LONG_TYPE, DOUBLE_TYPE
    };

    class DataType {
    public:
        virtual ~DataType() = default;
        virtual ColumnType id() = 0;
        virtual std::shared_ptr<arrow::DataType> getArrowType() = 0;
    };

    class StringType : public DataType {
    public:
        typedef typename arrow::StringArray ArrayType;
        typedef std::string ReturnType;
        typedef arrow::StringBuilder BuilderType;
        typedef arrow::StringDictionaryBuilder DictionaryBuilderType;
        const ColumnType TYPE_ID = ::db::STRING_TYPE;
        ColumnType id() override { return TYPE_ID; };
        std::shared_ptr<arrow::DataType> getArrowType() override { return arrow::utf8(); }
    };

    class LongType : public DataType  {
    public:
        typedef typename arrow::NumericArray<arrow::Int64Type> ArrayType;
        typedef typename arrow::Int64Type::c_type ReturnType;
        typedef arrow::Int64Builder BuilderType;
        typedef arrow::DictionaryBuilder<arrow::Int64Type> DictionaryBuilderType;
        const ColumnType TYPE_ID = ::db::LONG_TYPE;
        ColumnType id() override { return TYPE_ID; };
        std::shared_ptr<arrow::DataType> getArrowType() override { return arrow::int64(); }
    };

    class DoubleType : public DataType  {
    public:
        typedef typename arrow::NumericArray<arrow::DoubleType> ArrayType;
        typedef typename arrow::DoubleType::c_type ReturnType;
        typedef arrow::DoubleBuilder BuilderType;
        // TODO: what if there _is_ no corresponding dictionary type ???
        typedef arrow::DoubleBuilder DictionaryBuilderType;
        const ColumnType TYPE_ID = ::db::DOUBLE_TYPE;
        ColumnType id() override { return TYPE_ID; };
        std::shared_ptr<arrow::DataType> getArrowType() override { return arrow::float64(); }
    };

    std::shared_ptr<DataType> string_type();

    std::shared_ptr<DataType> long_type();

    std::shared_ptr<DataType> double_type();

};


#endif //DB_SCHEMA_H
