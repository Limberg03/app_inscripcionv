'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    

    await queryInterface.bulkInsert('horarios', [
      // Matemáticas I - Grupo A
      {
        dia: 'Lun-mie-vie',
        hora_inicio: '08:00:00',
        hora_fin: '10:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        dia:'ma-jue',
        hora_inicio: '08:00:00',
        hora_fin: '10:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // Matemáticas I - Grupo B
      {
        dia: 'Lun-mie-vie',
        hora_inicio: '10:00:00',
        hora_fin: '12:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // Programación I - Grupo A
      {
        dia: 'ma-jue',
        hora_inicio: '14:00:00',
        hora_fin: '16:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        dia: 'ma-jue',
        hora_inicio: '16:00:00',
        hora_fin: '18:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // Programación I - Grupo B
      {
        dia: 'ma-jue',
        hora_inicio: '18:00:00',
        hora_fin: '20:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // Física I - Grupo A
      {
        dia: 'ma-jue',
        hora_inicio: '07:00:00',
        hora_fin: '08:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // POO - Grupo A
      {
        dia: 'ma-jue',
        hora_inicio: '20:00:00',
        hora_fin: '22:00:00',
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estructura de Datos - Grupo A
      {
        dia: 'sab',
        hora_inicio: '08:00:00',
        hora_fin: '11:00:00',
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('horarios', null, {});
  }
};