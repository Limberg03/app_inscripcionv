'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.bulkInsert('docentes', [
      {
        nombre: 'Dr. Roberto Carlos Mendoza',
        telefono: '78123456',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Ing. María Elena Vargas',
        telefono: '79987654',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Lic. José Antonio Quispe',
        telefono: '76456123',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Dra. Carmen Rosa Silva',
        telefono: '77789012',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Ing. Pedro Luis Mamani',
        telefono: '75345678',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Lic. Ana Patricia López',
        telefono: '74234567',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Dr. Fernando Gonzalez',
        telefono: '73123456',
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        nombre: 'Ing. Claudia Morales',
        telefono: '72987654',
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('docentes', null, {});
  }
};