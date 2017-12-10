
#include "DBSchema.h"

std::shared_ptr<db::DataType>
db::string_type()
{
    return std::make_shared<::db::StringType>();
}

std::shared_ptr<db::DataType>
db::long_type()
{
    return std::make_shared<::db::LongType>();
}

std::shared_ptr<db::DataType>
db::double_type()
{
    return std::make_shared<::db::DoubleType>();
}