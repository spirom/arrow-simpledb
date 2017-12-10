

#ifndef DB_COLUMN_BUILDER_H
#define DB_COLUMN_BUILDER_H

#include <arrow/api.h>

namespace db {

    class DBGenColumnBuilder {

    public:

        virtual void add(std::shared_ptr<db::GenValue> value) = 0;

        virtual void endChunk() = 0;

        virtual std::shared_ptr<arrow::Column> getColumn() = 0;

        virtual ~DBGenColumnBuilder() = default;
    };

    template<typename T>
    class DBColumnBuilder : public DBGenColumnBuilder {

    public:

        explicit DBColumnBuilder(std::shared_ptr<arrow::Field> field, db::ColumnEncoding encoding);

        void add(std::shared_ptr<db::GenValue> value) override;

        void endChunk() override;

        std::shared_ptr<arrow::Column> getColumn() override;

    protected:

        void add(typename T::ElementType element);

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
