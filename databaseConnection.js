const mysql = require('mysql2/promise');

const connectionDb = async () => {
    const connection = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: '12345678',
        database: 'sfco_crm_perf'
    });
    return connection;
};

module.exports = { connectionDb };

