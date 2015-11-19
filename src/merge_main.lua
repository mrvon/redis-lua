local redis                     = require "redis"
local seri                      = require "seri"
local db_conf                   = require "db_conf"
local merge_player_data_handler = require "merge_player_data_handler"
local merge_server_data_handler = require "merge_server_data_handler"

require "DB_KEY"


function connect_db()
    local db_client = redis.connect(conf.ip, conf.port)

    if conf.auth then
        db_client:auth(conf.auth)
    end

    assert(db_client:ping())

    return db_client
end

function fetch_pattern_key_list(db_client, pattern)
    local all_list = db_client:keys("*")
    local key_list = {}

    for _, hash_name in pairs(all_list) do
        if string.match(hash_name, pattern) then
            table.insert(key_list, hash_name)
        end
    end

    return key_list
end

function union_list()
end

function player_key_pattern()
    return "^" .. KEY_GAME_ID .. "[%d]+_[%d]+" .. "$"
end

assert(    string.match(KEY_GAME_ID .. "1_1", player_key_pattern()))
assert(    string.match(KEY_GAME_ID .. "81_1", player_key_pattern()))
assert(    string.match(KEY_GAME_ID .. "81_0190", player_key_pattern()))
assert(not string.match(KEY_GAME_ID .. "a_1", player_key_pattern()))
assert(not string.match(KEY_GAME_ID .. "1a_10", player_key_pattern()))
assert(not string.match(KEY_GAME_ID .. "110", player_key_pattern()))
assert(not string.match(KEY_GAME_ID .. "1_1p", player_key_pattern()))
assert(not string.match("h" .. KEY_GAME_ID .. "1_1", player_key_pattern()))


function server_key_pattern()
    return "^" .. KEY_GAME_SERVER .. "[%d]+" .. "$"
end

assert(    string.match(KEY_GAME_SERVER .. "1", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "a1", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "1a", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. ",", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "1_1", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "a_1", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "1a_10", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "1_1p", server_key_pattern()))
assert(not string.match(KEY_GAME_SERVER .. "p1_1", server_key_pattern()))
assert(not string.match("H" .. KEY_GAME_SERVER .. "1_1", server_key_pattern()))


function merge_player_data(source_client, destination_client)
    local player_key_list = fetch_pattern_key_list(source_client, player_key_pattern())

    for _, hash_name in pairs(player_key_list) do
        if destination_client:exists(hash_name) then
            destination_client:del(hash_name)
            print(string.format("Error: Player Main Key(%s) ALREADY EXIST.", hash_name))
        end

        local sub_system_key_list = source_client:hkeys(hash_name)

        for _, sub_system_key in pairs(sub_system_key_list) do
            local data_string = source_client:hget(hash_name, sub_system_key)

            local handler = merge_player_data_handler[sub_system_key]

            if handler then
                data_string = seri.pack(handler(seri.unpack(data_string)))
            end

            destination_client:hset(hash_name, sub_system_key, data_string)
        end
    end
end

-- function merge_server_data(source_client_a, source_client_b, destination_client)
--     local a_server_key = fetch_pattern_key_list(source_client_a, server_key_pattern())

--     for _, hash_name in pairs(server_key_list) do

--         local sub_system_key_list = source_client_a:hkeys(hash_name)

--         for _, sub_system_key in pairs(sub_system_key_list) do
--             local handler = merge_server_data_handler[sub_system_key]

--             if handler then
--                 local a_data_string = source_client_a:hget(hash_name, sub_system_key)
--                 local b_data_string = source_client_b:hget(hash_name, sub_system_key)

--                 local c_data_string = seri.pack(handler(seri.unpack(a_data_string), seri.unpack(b_data_string)))
--                 destination_client:hset(hash_name, sub_system_key, c_data_string)
--             end
--         end

--     end
-- end

function main()
    local source_a = connect_db(db_conf.source_db_a)
    local source_b = connect_db(db_conf.source_db_b)
    local destination_c = connect_db(db_conf.destination_db)

    merge_player_data(source_a, destination_c)
    merge_player_data(source_b, destination_c)

    -- merge_server_data(source_a, source_b, destination_c)
end

main()
