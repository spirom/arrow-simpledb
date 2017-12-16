

#ifndef GREATERTHANFILTER_H
#define GREATERTHANFILTER_H

#include <string>
#include "filters/Filter.h"
#include "tables/TableCursor.h"
#include "columns/BaseColumnCursor.h"

namespace db {

    template<typename T>
    class GreaterThanFilter : public Filter {
    public:

        GreaterThanFilter(std::string column_name, typename T::ElementType value);

        void initialize(TableCursor &table_cursor) override;

        bool evaluate() override;

    private:

        std::string _column_name;

        double _value;

        std::shared_ptr<BaseColumnCursor<T>> _cursor;
    };

};


#endif // GREATERTHANFILTER_H
