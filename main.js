const mysql = require('mysql2/promise');
const fs = require('fs');

(async () => {
    const connection = await mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'sfco_crm_perf',
        port: 3308
    });

    console.log("✅ Conectado a la base de datos MySQL!");
    
    // Preparar archivo de reporte
    const reportData = [];
    const reportFileName = `duplicados_${new Date().toISOString().split('T')[0]}.csv`;
    
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

        // Agrupar duplicados por `name` y `phonenumber`
        const groupedLeads = {};
        for (const lead of duplicates) {
            const key = `${lead.name}_${lead.phonenumber}`;
            if (!groupedLeads[key]) groupedLeads[key] = [];
            groupedLeads[key].push(lead);
        }

        // Cabecera del CSV
        reportData.push('Grupo,Nombre,Teléfono,ID,Acción,Puntuación,Campos Completos,Estado');

        for (const key in groupedLeads) {
            const leads = groupedLeads[key];
            
            // Calcular puntajes y ordenar
            const scoredLeads = leads.map(lead => {
                let filledFieldsCount = Object.values(lead).filter(v => v !== null && v !== "" && v !== "none" && v !== 0).length;
                let statusScore = lead.status !== 13 ? 5 : 0; // 📌 Si no es "new", +5 puntos
                return { ...lead, score: filledFieldsCount + statusScore, filledFieldsCount };
            });
            
            scoredLeads.sort((a, b) => b.score - a.score);
            
            // Mantener el mejor registro
            const bestLead = scoredLeads[0];
            console.log(`✅ Manteniendo ID ${bestLead.id} como válido.`);
            
            // Añadir al reporte el registro que se mantiene
            reportData.push(`${key},${bestLead.name},${bestLead.phonenumber},${bestLead.id},MANTENER,${bestLead.score},${bestLead.filledFieldsCount},${bestLead.status}`);
            
            // Marcar los demás como `junk = 1`
            for (let i = 1; i < scoredLeads.length; i++) {
                await connection.execute(
                    `UPDATE tblleads SET junk = 1 WHERE id = ?`,
                    [scoredLeads[i].id]
                );
                console.log(`⚠️ Marcado como junk: ID ${scoredLeads[i].id}`);
                
                // Añadir al reporte los registros marcados como junk
                reportData.push(`${key},${scoredLeads[i].name},${scoredLeads[i].phonenumber},${scoredLeads[i].id},JUNK,${scoredLeads[i].score},${scoredLeads[i].filledFieldsCount},${scoredLeads[i].status}`);
            }
        }

        // Escribir el archivo de reporte
        fs.writeFileSync(reportFileName, reportData.join('\n'), 'utf8');
        console.log(`📊 Reporte generado: ${reportFileName}`);
        
        console.log("✅ Proceso de limpieza de duplicados completado.");
    } catch (error) {
        console.error("❌ Error:", error);
    } finally {
        await connection.end();
        console.log("🔌 Conexión cerrada.");
    }
})();