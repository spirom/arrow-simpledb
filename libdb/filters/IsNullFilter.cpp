

#include "IsNullFilter.h"

namespace db {

    IsNullFilter::IsNullFilter(std::string column_name)
            : _column_name(column_name)
    {

    }

    void
    IsNullFilter::initialize(TableCursor& table_cursor) {
        _cursor = table_cursor.getColumn(_column_name);
    }

    bool
    IsNullFilter::evaluate()
    {
        return _cursor->isNull();
    }

};

