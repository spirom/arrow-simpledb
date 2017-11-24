

#ifndef TABLE_CURSOR_H
#define TABLE_CURSOR_H


#include <cstdint>
#include <memory>

class GenericColumnCursor;
class FilterProjectTableCursor;

/**
 * Common base of all table cursors, exposing row iteration and access to columns.
 */
class TableCursor {

    friend class GenericColumnCursor;
    friend class FilterProjectTableCursor;
public:

    /**
     * Get access to the specified column, always synchronized with this cursor's position.
     * @param colName
     * @return
     */
    virtual std::shared_ptr<GenericColumnCursor> getColumn(std::string colName) = 0;

    /**
     * True if there are more rows.
     * @return
     */
    virtual bool hasMore() = 0;

    /**
     * Reaset to start and ready to iterate again.
     */
    virtual void reset() = 0;

protected:
    virtual uint64_t getPosition() = 0;
};


#endif // TABLE_CURSOR_H
