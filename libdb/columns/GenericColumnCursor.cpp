

#include "GenericColumnCursor.h"

#include "tables/TableCursor.h"

GenericColumnCursor::GenericColumnCursor(TableCursor &table_cursor)
        : _table_cursor(table_cursor)
{

}

uint64_t
GenericColumnCursor::get_table_cursor_position()
{
    return _table_cursor.getPosition();
}