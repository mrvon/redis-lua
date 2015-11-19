local util = require "util"

require "DB_KEY"

local handler = {}

handler[KEY_GAME_ID_LIST] = function(a_table, b_table)
    local a_table = a_table or {}
    local b_table = b_table or {}

    local n_table = util.union_list(a_table, b_table)
    return n_table
end

return handler
