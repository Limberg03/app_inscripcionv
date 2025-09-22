'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.bulkInsert('aulas', [
      {
        nombre: 'Aula 101',
        capacidad: 40,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Aula 102',
        capacidad: 35,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Aula 201',
        capacidad: 50,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Aula 202',
        capacidad: 45,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Laboratorio A',
        capacidad: 25,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Laboratorio B',
        capacidad: 30,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Auditorio',
        capacidad: 100,
        estado: true,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Aula Virtual',
        capacidad: 20,
        estado: false,
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('aulas', null, {});
  }
};