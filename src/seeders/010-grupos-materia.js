'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    // Helper para obtener materias existentes
    const getMaterias = async () => {
      const result = await queryInterface.sequelize.query(
        'SELECT id, nombre FROM materias ORDER BY id',
        { type: Sequelize.QueryTypes.SELECT }
      );
      if (result.length === 0) {
        throw new Error('No se encontraron materias en la base de datos');
      }
      return result;
    };

    // Helper para obtener docentes existentes
    const getDocentes = async () => {
      const result = await queryInterface.sequelize.query(
        'SELECT id, nombre FROM docentes ORDER BY id',
        { type: Sequelize.QueryTypes.SELECT }
      );
      if (result.length === 0) {
        throw new Error('No se encontraron docentes en la base de datos');
      }
      return result;
    };

    const getHorarios = async () => {
      const result = await queryInterface.sequelize.query(
        'SELECT id, dia FROM horarios ORDER BY id',
        { type: Sequelize.QueryTypes.SELECT }
      );
      if (result.length === 0) {
        throw new Error('No se encontraron horarios en la base de datos');
      }
      return result;
    };

    // Obtener materias y docentes existentes
    const materias = await getMaterias();
    const docentes = await getDocentes();
    const Horarios = await getHorarios();


    await queryInterface.bulkInsert('grupos_materia', [
      {
        grupo: 'SB',
        estado: true,
        materia_id: materias[0].id, // Matemáticas I
        docente_id: docentes[0].id, // Dr. Roberto Carlos Mendoza
        horario_id: Horarios[0].id, 
        cupo: 2,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'SC',
        estado: true,
        materia_id: materias[0].id, // Matemáticas I
        docente_id: docentes[1].id, // Ing. María Elena Vargas
        horario_id: Horarios[1].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'SA',
        estado: true,
        materia_id: materias[1].id, // Introducción a la Programación
        docente_id: docentes[2].id, // Lic. José Antonio Quispe
        horario_id: Horarios[2].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'S1',
        estado: true,
        materia_id: materias[1].id, // Introducción a la Programación
        docente_id: docentes[3].id, // Dra. Carmen Rosa Silva
        horario_id: Horarios[3].id, 
        cupo: 10,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'R1',
        estado: true,
        materia_id: materias[2].id, // Física I
        docente_id: docentes[4].id, // Ing. Pedro Luis Mamani
        horario_id: Horarios[5].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'A',
        estado: true,
        materia_id: materias[3].id, // Matemáticas II
        docente_id: docentes[0].id, // Dr. Roberto Carlos Mendoza
        horario_id: Horarios[4].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'SF',
        estado: true,
        materia_id: materias[4].id, // Programación Orientada a Objetos
        docente_id: docentes[2].id, // Lic. José Antonio Quispe
        horario_id: Horarios[0].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'Z1',
        estado: true,
        materia_id: materias[6].id, // Estructura de Datos
        docente_id: docentes[5].id, // Lic. Ana Patricia López
        horario_id: Horarios[0].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'Z2',
        estado: true,
        materia_id: materias[7].id, // Base de Datos I
        docente_id: docentes[6].id, // Dr. Fernando Gonzalez
        horario_id: Horarios[1].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      },
      {
        grupo: 'Z3',
        estado: true,
        materia_id: materias[8].id, // Algoritmos y Complejidad
        docente_id: docentes[7].id, // Ing. Claudia Morales
        horario_id: Horarios[3].id, 
        cupo: 10,

        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('grupos_materia', null, {});
  }
};