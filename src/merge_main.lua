local redis   = require "redis"
local seri    = require "seri"
local db_conf = require "db_conf"

local merge_server_data_handler = require "merge_server_data_handler"

require "DB_KEY"

local source_db_client = redis.connect(db_conf.source_db.ip, db_conf.source_db.port)
local destination_db_client = redis.connect(db_conf.destination_db.ip, db_conf.destination_db.port)

function auth_connection()
    if db_conf.source_db.auth then
        source_db_client:auth(db_conf.source_db.auth)
    end
    if db_conf.destination_db.auth then
        destination_db_client:auth(db_conf.destination_db.auth)
    end

    assert(source_db_client:ping())
    assert(destination_db_client:ping())
end

--------------------------------------------------------------------------------

function fetch_pattern_key_list(pattern)
    local all_list = source_db_client:keys("*")
    local key_list = {}

    for _, hash_name in pairs(all_list) do
        if string.match(hash_name, pattern) then
            table.insert(key_list, hash_name)
        end
    end

    return key_list
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


function merge_player_data()
    local player_key_list = fetch_pattern_key_list(player_key_pattern())

    for _, hash_name in pairs(player_key_list) do
        if destination_db_client:exists(hash_name) then
            destination_db_client:del(hash_name)
            print(string.format("WARNING: Hash(%s) ALREADY EXIST", hash_name))
        end

        local sub_system_key_list = source_db_client:hkeys(hash_name)

        for _, sub_system_key in pairs(sub_system_key_list) do
            local data_string = source_db_client:hget(hash_name, sub_system_key)

            destination_db_client:hset(hash_name, sub_system_key, data_string)
        end
    end
end

function merge_server_data()
    local server_key_list = fetch_pattern_key_list(server_key_pattern())

    for _, hash_name in pairs(server_key_list) do

        local sub_system_key_list = source_db_client:hkeys(hash_name)

        for _, sub_system_key in pairs(sub_system_key_list) do
            local data_string = source_db_client:hget(hash_name, sub_system_key)

            local handler = merge_server_data_handler[sub_system_key]
            if handler then
                local new_data_string = seri.pack(handler(seri.unpack(data_string)))
                print(data_string)
                print(new_data_string)
                source_db_client:hset(hash_name, sub_system_key, new_data_string)
            end
        end

    end
end

function main()
    auth_connection()
    merge_player_data()
    merge_server_data()
end

main()
