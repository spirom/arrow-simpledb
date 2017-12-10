

#ifndef LESSTHANFILTER_H
#define LESSTHANFILTER_H

#include <string>
#include "filters/Filter.h"
#include "tables/TableCursor.h"
#include "columns/ColumnCursorWrapper.h"

template <typename T>
class LessThanFilter  : public Filter {
public:

    LessThanFilter(std::string column_name, typename T::ElementType value);

    void initialize(TableCursor &table_cursor) override;

    bool evaluate() override;

private:

    std::string _column_name;

    double _value;

    std::shared_ptr <ColumnCursorWrapper<T>> _cursor;
};


#endif // LESSTHANFILTER_H
