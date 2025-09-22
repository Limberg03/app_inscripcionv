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
    
    const estudiantes = await queryInterface.sequelize.query(
      'SELECT id, registro FROM estudiantes ORDER BY id',
      { type: Sequelize.QueryTypes.SELECT }
    );

    await queryInterface.bulkInsert('notas', [
      // Estudiante 1 - Diferentes materias
      {
        calificacion: 85.50,
        observacion: 'Buen desempeño en matemáticas',
        grupo_materia_id: gruposMateria[0].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[0].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        calificacion: 92.00,
        observacion: 'Excelente en programación',
        grupo_materia_id: gruposMateria[2].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[0].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        calificacion: 78.25,
        observacion: 'Necesita mejorar en física',
        grupo_materia_id: gruposMateria[4].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[0].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estudiante 2
      {
        calificacion: 90.75,
        observacion: 'Muy buen estudiante',
        grupo_materia_id: gruposMateria[1].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[1].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        calificacion: 88.50,
        observacion: 'Participación activa',
        grupo_materia_id: gruposMateria[3].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[1].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estudiante 3
      {
        calificacion: 76.00,
        observacion: 'Esfuerzo constante',
        grupo_materia_id: gruposMateria[0].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[2].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        calificacion: 82.75,
        observacion: 'Buena comprensión',
        grupo_materia_id: gruposMateria[6].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[2].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estudiante 4
      {
        calificacion: 95.00,
        observacion: 'Estudiante destacado',
        grupo_materia_id: gruposMateria[7].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[2].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        calificacion: 87.25,
        observacion: 'Muy buena actitud',
        grupo_materia_id: gruposMateria[2].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[3].id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estudiante 5
      {
        calificacion: 81.50,
        observacion: 'Progreso satisfactorio',
        grupo_materia_id: gruposMateria[1].id, // Primer grupo (Matemáticas I - Grupo A)

         estudiante_id: estudiantes[4].id,
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('notas', null, {});
  }
};