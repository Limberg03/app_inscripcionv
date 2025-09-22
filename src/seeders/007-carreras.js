'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    // Obtener los planes de estudio existentes ordenados por ID
    const planesEstudio = await queryInterface.sequelize.query(
      'SELECT id, nombre FROM planes_estudio ORDER BY id',
      { type: Sequelize.QueryTypes.SELECT }
    );

    // Verificar que tengamos al menos 5 planes de estudio
    if (planesEstudio.length < 5) {
      throw new Error('No se encontraron suficientes planes de estudio para crear las carreras');
    }

    await queryInterface.bulkInsert('carreras', [
      {
        nombre: 'Ingeniería de Sistemas',
        codigo: 'ING-SIS',
        modalidad: 'Presencial',
        estado: true,
        plan_estudio_id: planesEstudio[0].id, // Plan 2020 - Ingeniería
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Ingeniería Industrial',
        codigo: 'ING-IND',
        modalidad: 'Presencial',
        estado: true,
        plan_estudio_id: planesEstudio[0].id, // Plan 2020 - Ingeniería
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Matemáticas',
        codigo: 'MAT',
        modalidad: 'Presencial',
        estado: true,
        plan_estudio_id: planesEstudio[1].id, // Plan 2018 - Ciencias Exactas
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Física',
        codigo: 'FIS',
        modalidad: 'Presencial',
        estado: false,
        plan_estudio_id: planesEstudio[1].id, // Plan 2018 - Ciencias Exactas
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Administración de Empresas',
        codigo: 'ADM-EMP',
        modalidad: 'Semipresencial',
        estado: true,
        plan_estudio_id: planesEstudio[2].id, // Plan 2021 - Administración
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Derecho',
        codigo: 'DER',
        modalidad: 'Presencial',
        estado: true,
        plan_estudio_id: planesEstudio[3].id, // Plan 2019 - Derecho
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Medicina',
        codigo: 'MED',
        modalidad: 'Presencial',
        estado: true,
        plan_estudio_id: planesEstudio[4].id, // Plan 2022 - Medicina
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('carreras', null, {});
  }
};