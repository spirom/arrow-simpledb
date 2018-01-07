
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

std::shared_ptr<db::GenValue>
db::long_val(int64_t i)
{
    return std::make_shared<::db::Value<int64_t>>(i);
}

std::shared_ptr<db::GenValue>
db::double_val(double d)
{
    return std::make_shared<::db::Value<double>>(d);
}

std::shared_ptr<db::GenValue>
db::string_val(std::string s)
{
    return std::make_shared<::db::Value<std::string>>(s);
}

std::shared_ptr<db::GenValue>
db::null_val()
{
    return std::make_shared<::db::NullValue>();
}
