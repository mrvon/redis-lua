local redis = require "redis"
local connect_conf, merge_rule = require "merge_conf"

local source_db_client = redis.connect(connect_conf.source_db.ip, connect_conf.source_db.port)
local destination_db_client = redis.connect(connect_conf.destination_db.ip, connect_conf.destination_db.port)

function auth_connection()
    if connect_conf.source_db.auth then
        source_db_client:auth(connect_conf.source_db.auth)
    end
    if connect_conf.destination_db.auth then
        destination_db_client:auth(connect_conf.destination_db.auth)
    end

    assert(source_db_client:ping())
    assert(destination_db_client:ping())
end

local server_id = 1
local KEY_GAME_ID = "game_ID_"
local KEY_GAME_SERVER = "game_server_"

function merge_player_data()
    local pattern = string.format("%s*_%d", KEY_GAME_ID, server_id)
    local player_key_list = source_db_client:keys(pattern)

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
end

function main()
    auth_connection()
    merge_player_data()
    merge_server_data()
end

main()
