require "DB_KEY"

local handler = {}

handler[KEY_DIAMOND_INVEST_GLOBAL] = function(data_table)
    data_table.local_open_id = data_table.local_open_id + 1
    return data_table
end

return handler
