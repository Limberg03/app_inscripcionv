'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.bulkInsert('estudiantes', [
      {
        registro: 'EST001',
        nombre: 'Juan Pérez García',
        telefono: '77123456',
        fecha_nac: new Date('1999-05-15'),
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        registro: 'EST002',
        nombre: 'María González López',
        telefono: '78987654',
        fecha_nac: new Date('2000-08-22'),
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        registro: 'EST003',
        nombre: 'Carlos Ramírez Silva',
        telefono: '79456123',
        fecha_nac: new Date('1998-12-10'),
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        registro: 'EST004',
        nombre: 'Ana Rodríguez Mamani',
        telefono: '76789012',
        fecha_nac: new Date('2001-03-18'),
        created_at: new Date(),
        updated_at: new Date()
      },
      {
        registro: 'EST005',
        nombre: 'Luis Fernando Choque',
        telefono: '75345678',
        fecha_nac: new Date('1999-09-05'),
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('estudiantes', null, {});
  }
};