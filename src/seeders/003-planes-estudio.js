'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.bulkInsert('planes_estudio', [
      {
        nombre: 'Plan de Estudios 2020 - Ingeniería',
        fecha_inicio: new Date('2020-01-01'),
        fecha_fin: null,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Plan de Estudios 2018 - Ciencias Exactas',
        fecha_inicio: new Date('2018-01-01'),
        fecha_fin: new Date('2023-12-31'),
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Plan de Estudios 2021 - Administración',
        fecha_inicio: new Date('2021-01-01'),
        fecha_fin: null,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Plan de Estudios 2019 - Derecho',
        fecha_inicio: new Date('2019-01-01'),
        fecha_fin: null,
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Plan de Estudios 2022 - Medicina',
        fecha_inicio: new Date('2022-01-01'),
        fecha_fin: null,
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('planes_estudio', null, {});
  }
};