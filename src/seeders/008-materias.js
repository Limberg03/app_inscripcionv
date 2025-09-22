'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    // Obtener niveles
    const niveles = await queryInterface.sequelize.query(
      'SELECT id, nombre FROM niveles ORDER BY id',
      { type: Sequelize.QueryTypes.SELECT }
    );

    // ⬇️ AGREGAR ESTA CONSULTA PARA PLANES DE ESTUDIO:
    const planesEstudio = await queryInterface.sequelize.query(
      'SELECT id, nombre FROM planes_estudio ORDER BY id LIMIT 1',
      { type: Sequelize.QueryTypes.SELECT }
    );

    // Verificar que existan niveles y planes
    if (niveles.length < 5) {
      throw new Error('No se encontraron suficientes niveles para crear las materias');
    }

    if (planesEstudio.length === 0) {
      throw new Error('No se encontraron planes de estudio');
    }

    const planEstudioId = planesEstudio[0].id; // Usar el primer plan de estudio

    await queryInterface.bulkInsert('materias', [
      // Primer Semestre
      {
        nombre: 'Matemáticas I',
        sigla: 'MAT-I',
        creditos: 4,
        nivel_id: niveles[0].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Introducción a la Programación',
        sigla: 'PROG-I',
        creditos: 4,
        nivel_id: niveles[0].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Física I',
        sigla: 'FIS-I',
        creditos: 4,
        nivel_id: niveles[0].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      // Segundo Semestre
      {
        nombre: 'Matemáticas II',
        sigla: 'MAT-II',
        creditos: 4,
        nivel_id: niveles[1].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Programación Orientada a Objetos',
        sigla: 'POO',
        creditos: 4,
        nivel_id: niveles[1].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Física II',
        sigla: 'FIS-II',
        creditos: 4,
        nivel_id: niveles[1].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      // Tercer Semestre
      {
        nombre: 'Estructura de Datos',
        sigla: 'EST-DAT',
        creditos: 4,
        nivel_id: niveles[2].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Base de Datos I',
        sigla: 'BD-I',
        creditos: 4,
        nivel_id: niveles[2].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      // Cuarto Semestre
      {
        nombre: 'Algoritmos y Complejidad',
        sigla: 'ALG-COMP',
        creditos: 4,
        nivel_id: niveles[3].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Base de Datos II',
        sigla: 'BD-II',
        creditos: 4,
        nivel_id: niveles[3].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      // Quinto Semestre
      {
        nombre: 'Ingeniería de Software I',
        sigla: 'ING-SW-I',
        creditos: 4,
        nivel_id: niveles[4].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Redes de Computadoras',
        sigla: 'REDES',
        creditos: 4,
        nivel_id: niveles[4].id,
        plan_estudio_id: planEstudioId, 
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('materias', null, {});
  }
};