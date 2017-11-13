

#include "GenericColumnCursor.h"

GenericColumnCursor::GenericColumnCursor(TableCursor &table_cursor)
        : _table_cursor(table_cursor)
{

}

TableCursor&
GenericColumnCursor::get_table_cursor()
{
    return _table_cursor;
}