const mysql = require('mysql2/promise');

(async () => {
    const connection = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'sfco_crm_perf',
        port: 3307 
    });

    console.log("✅ Conectado a la base de datos MySQL!");

    try {
        const [duplicates] = await connection.execute(`
            SELECT t1.*
            FROM tblleads t1
            JOIN (
                SELECT name, phonenumber
                FROM tblleads
                WHERE junk = 0
                GROUP BY name, phonenumber
                HAVING COUNT(*) > 1
            ) t2
            ON t1.name = t2.name AND t1.phonenumber = t2.phonenumber
            WHERE t1.junk = 0
            AND EXISTS (
                SELECT 1 FROM tblleads t3
                WHERE t3.name = t1.name
                AND t3.phonenumber = t1.phonenumber
                AND t3.address LIKE CONCAT('%', t1.address, '%')
                AND t3.id <> t1.id
            )
            ORDER BY t1.name, t1.phonenumber, t1.address;
        `);

        console.log(`🔍 Encontrados ${duplicates.length} registros duplicados.`);

        //  Agrupar duplicados por `name` y `phonenumber`
        const groupedLeads = {};
        for (const lead of duplicates) {
            const key = `${lead.name}_${lead.phonenumber}`;
            if (!groupedLeads[key]) groupedLeads[ key] = [];
            groupedLeads[key].push(lead);
        }

        for (const key in groupedLeads) {
            const leads = groupedLeads[key];
 
            //  Calcular puntajes y ordenar
            const scoredLeads = leads.map(lead => {
                let filledFieldsCount = Object.values(lead).filter(v => v !== null && v !== "" && v !== "none" && v !== 0).length;
                let statusScore = lead.status !== 13 ? 5 : 0; // 📌 Si no es "new", +5 puntos
                return { ...lead, score: filledFieldsCount + statusScore };
            });

            scoredLeads.sort((a, b) => b.score - a.score);

            //  Mantener el mejor registro
            const bestLead = scoredLeads[0];
            console.log(`✅ Manteniendo ID ${bestLead.id} como válido.`);

            //  Marcar los demás como `junk = 1`
            for (let i = 1; i < scoredLeads.length; i++) {
                await connection.execute(
                    `UPDATE tblleads SET junk = 1 WHERE id = ?`,
                    [scoredLeads[i].id]
                );
                console.log(`⚠️ Marcado como junk: ID ${scoredLeads[i].id}`);
            }
        }

        console.log("✅ Proceso de limpieza de duplicados completado.");
    } catch (error) {
        console.error("❌ Error:", error);
    } finally {
        await connection.end();
        console.log("🔌 Conexión cerrada.");
    }
})();
