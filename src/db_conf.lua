local db_conf = {
    source_db_a = {
        ip = '127.0.0.1',
        port = 3001,
        auth = 'zhenlong',
        server_id = 1,
    },

    source_db_b = {
        ip = '127.0.0.1',
        port = 3101,
        auth = 'zhenlong',
        server_id = 2,
    },

    destination_db = {
        ip = '127.0.0.1',
        port = 6379,
        server_id = 3,
    },
}

return db_conf
