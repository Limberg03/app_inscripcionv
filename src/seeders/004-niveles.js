'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.bulkInsert('niveles', [
      {
        nombre: 'Primer Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Segundo Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Tercer Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Cuarto Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Quinto Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Sexto Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Séptimo Semestre',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Octavo Semestre',
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('niveles', null, {});
  }
};