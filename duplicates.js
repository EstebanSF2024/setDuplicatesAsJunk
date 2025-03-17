
const { connectionDb } = require('./databaseConnection');

const duplicates = async () => {
    const connection = await connectionDb();
    const [rows] = await connection.execute("SELECT * FROM tblleads");

    for (const lead of rows) {

        // 📌 Buscar todos los leads duplicados
        const [duplicates] = await connection.execute(
            `SELECT * FROM tblleads WHERE name = ? AND address LIKE CONCAT('%', ?, '%') AND phonenumber = ?`,
            [lead.name, lead.address, lead.phonenumber]
        );

        if (duplicates.length > 1) {
            console.log(`🔍 Encontrados ${duplicates.length} duplicados para: ${lead.name}`);

        }
    }
}

duplicates();

