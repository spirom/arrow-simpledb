

#ifndef ANDFILTER_H
#define ANDFILTER_H

#include <string>
#include "filters/Filter.h"
#include "tables/TableCursor.h"

namespace db {

    class AndFilter : public Filter {
    public:

        AndFilter(std::string column_name, std::shared_ptr<Filter> left, std::shared_ptr<Filter> right);

        void initialize(TableCursor &table_cursor) override;

        bool evaluate() override;

    private:

        std::string _column_name;

        std::shared_ptr<Filter> _left;

        std::shared_ptr<Filter> _right;

    };

};


#endif // ANDFILTER_H
