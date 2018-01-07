

#ifndef DB_COLUMN_BUILDER_H
#define DB_COLUMN_BUILDER_H

#include <arrow/api.h>

namespace db {

    class DBGenColumnBuilder {

    public:

        virtual Status add(std::shared_ptr<db::GenValue> value) = 0;

        virtual Status endChunk() = 0;

        virtual std::shared_ptr<arrow::Column> getColumn() = 0;

        virtual ~DBGenColumnBuilder() = default;

    private:

        virtual Status addNull() = 0;
    };

    template<typename T>
    class DBColumnBuilder : public DBGenColumnBuilder {

    public:

        explicit DBColumnBuilder(std::shared_ptr<arrow::Field> field,
                                 db::ColumnEncoding encoding,
                                 arrow::MemoryPool *pool = arrow::default_memory_pool());

        Status add(std::shared_ptr<db::GenValue> value) override;

        Status endChunk() override;

        std::shared_ptr<arrow::Column> getColumn() override;

    protected:

        Status add(typename T::ElementType element);

        Status addNull() override;

    private:

        bool _haveData;

        db::ColumnEncoding _encoding;

        std::unique_ptr<typename T::BuilderType> _builder;
        std::unique_ptr<typename T::DictionaryBuilderType> _dictBuilder;

        arrow::ArrayVector _chunks;

        std::shared_ptr<arrow::Field> _field;
    };

};


#endif // DB_COLUMN_BUILDER_H
