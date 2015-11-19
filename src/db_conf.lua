local db_conf = {
    source_db_a = {
        ip = '127.0.0.1',
        port = 3001,
        auth = 'zhenlong',
    },

    source_db_b = {
        ip = '127.0.0.1',
        port = 3101,
        auth = 'zhenlong',
    },

    destination_db = {
        ip = '127.0.0.1',
        port = 6379,
    },
}

return db_conf
