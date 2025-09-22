'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {

const getGruposMateria = async () => {
      const result = await queryInterface.sequelize.query(
        'SELECT id, grupo, materia_id FROM grupos_materia ORDER BY id',
        { type: Sequelize.QueryTypes.SELECT }
      );
      if (result.length === 0) {
        throw new Error('No se encontraron grupos de materia en la base de datos');
      }
      return result;
    };

    

    // Obtener grupos de materia y aulas existentes
    const gruposMateria = await getGruposMateria();

    // Primero obtenemos los IDs reales de los estudiantes
    const estudiantes = await queryInterface.sequelize.query(
      'SELECT id, registro FROM estudiantes ORDER BY id',
      { type: Sequelize.QueryTypes.SELECT }
    );

    // Verificamos que tengamos al menos los estudiantes necesarios
    if (estudiantes.length < 5) {
      throw new Error('No se encontraron suficientes estudiantes para crear las inscripciones');
    }

    await queryInterface.bulkInsert('inscripciones', [
      {
        fecha: new Date('2024-02-15'),
        gestion: 2024,
        estudiante_id: estudiantes[0].id, // Primer estudiante (EST001)
        grupo_materia_id: gruposMateria[1].id, // Primer grupo (Matemáticas I - Grupo A)
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        fecha: new Date('2024-02-20'),
        gestion: 2024,
        estudiante_id: estudiantes[1].id, // Segundo estudiante (EST002)
        grupo_materia_id: gruposMateria[0].id, // Primer grupo (Matemáticas I - Grupo A)

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        fecha: new Date('2024-02-18'),
        gestion: 2024,
        estudiante_id: estudiantes[2].id, // Tercer estudiante (EST003)
        grupo_materia_id: gruposMateria[2].id, // Primer grupo (Matemáticas I - Grupo A)

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        fecha: new Date('2024-02-25'),
        gestion: 2024,
        estudiante_id: estudiantes[3].id, // Cuarto estudiante (EST004)
        grupo_materia_id: gruposMateria[4].id, // Primer grupo (Matemáticas I - Grupo A)

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        fecha: new Date('2023-08-10'),
        gestion: 2023,
        estudiante_id: estudiantes[0].id, // Primer estudiante otra vez (EST001)
        grupo_materia_id: gruposMateria[6].id, // Primer grupo (Matemáticas I - Grupo A)

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        fecha: new Date('2023-08-15'),
        gestion: 2023,
        estudiante_id: estudiantes[4].id, // Quinto estudiante (EST005)
        grupo_materia_id: gruposMateria[3].id, // Primer grupo (Matemáticas I - Grupo A)

        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('inscripciones', null, {});
  }
};