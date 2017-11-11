

#ifndef TABLECURSOR_H
#define TABLECURSOR_H

template <typename T>
class ColumnCursorWrapper;

#include <cstdint>
#include <memory>
#include "columns/GenericColumnCursor.h"

class TableCursor {
public:
    virtual std::shared_ptr<GenericColumnCursor> getColumn(std::string colName) = 0;

    virtual bool hasMore() = 0;

    virtual void reset() = 0;

    virtual uint64_t getPosition() = 0;
};


#endif // TABLECURSOR_H
