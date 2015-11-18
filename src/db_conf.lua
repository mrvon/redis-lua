local connect_conf = {
    source_db = {
        ip = '127.0.0.1',
        port = 3001,
        auth = 'zhenlong',
    },

    destination_db = {
        ip = '127.0.0.1',
        port = 6379,
    },
}

local merge_rule = {
}

return connect_conf, merge_rule
