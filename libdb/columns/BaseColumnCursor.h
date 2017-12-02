

#ifndef BASECOLUMNCURSOR_H
#define BASECOLUMNCURSOR_H

#include "ColumnTypeTrait.h"

template <typename T>
class BaseColumnCursor : public ColumnTypeTrait<T> {
public:

    /**
     * Will next() produce another element?
     * @return
     */
    virtual bool hasMore() = 0;

    /**
     * Move to the next element.
     * @return True if an element is available, false otherwise (end of column.)
     */
    virtual bool next() = 0;

    /**
     * Is the element at the current position null?
     * @return
     */
    virtual bool isNull() = 0;

    /**
     * Get value at current position.
     * @return
     */
    virtual typename ColumnTypeTrait<T>::ReturnType get() = 0;

    /**
     * Reset to the first element, if any.
     */
    virtual void reset() = 0;

    /**
     * Seek to the given position.
     * @param to zero-based ordinal position of element in column
     * @return True if successful.
     */
    virtual bool seek(uint64_t to) = 0;

    virtual ~BaseColumnCursor() {}

};


#endif // BASECOLUMNCURSOR_H
