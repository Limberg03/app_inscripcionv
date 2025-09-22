'use strict';
const bcrypt = require('bcryptjs');

module.exports = {
  async up(queryInterface, Sequelize) {
    const salt = await bcrypt.genSalt(12);
    
    // Contraseñas encriptadas para los usuarios de prueba
    const hashedPasswords = {
      admin: await bcrypt.hash('Admin123!', salt),
      docente: await bcrypt.hash('Docente123!', salt),
      estudiante: await bcrypt.hash('Estudiante123!', salt),
      usuario: await bcrypt.hash('Usuario123!', salt)
    };

    await queryInterface.bulkInsert('usuarios', [
      {
        nombre: 'Super',
        apellido: 'Administrador',
        email: 'admin@universidad.edu.bo',
        username: 'admin',
        password: hashedPasswords.admin,
        rol: 'admin',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'Juan Carlos',
        apellido: 'Pérez García',
        email: 'juan.perez@universidad.edu.bo',
        username: 'jperez',
        password: hashedPasswords.docente,
        rol: 'docente',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'María Elena',
        apellido: 'Rodríguez Mamani',
        email: 'maria.rodriguez@estudiante.universidad.edu.bo',
        username: 'mrodriguez',
        password: hashedPasswords.estudiante,
        rol: 'estudiante',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'Carlos Alberto',
        apellido: 'Mendoza Quispe',
        email: 'carlos.mendoza@estudiante.universidad.edu.bo',
        username: 'cmendoza',
        password: hashedPasswords.estudiante,
        rol: 'estudiante',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'Ana Lucia',
        apellido: 'Vargas Torrez',
        email: 'ana.vargas@universidad.edu.bo',
        username: 'avargas',
        password: hashedPasswords.docente,
        rol: 'docente',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'Roberto',
        apellido: 'Jiménez Cruz',
        email: 'roberto.jimenez@universidad.edu.bo',
        username: 'rjimenez',
        password: hashedPasswords.usuario,
        rol: 'usuario',
        estado: true,
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      },
      {
        nombre: 'Sofia Isabel',
        apellido: 'Herrera Flores',
        email: 'sofia.herrera@estudiante.universidad.edu.bo',
        username: 'sherrera',
        password: hashedPasswords.estudiante,
        rol: 'estudiante',
        estado: false, // Usuario inactivo para pruebas
        ultimo_acceso: null,
        fecha_creacion: new Date(),
        fecha_actualizacion: new Date()
      }
    ], {});

    console.log('✅ Usuarios de prueba creados exitosamente');
    console.log('📋 Credenciales de acceso:');
    console.log('👨‍💼 Admin: admin / Admin123!');
    console.log('👨‍🏫 Docente 1: jperez / Docente123!');
    console.log('👨‍🏫 Docente 2: avargas / Docente123!');
    console.log('🎓 Estudiante 1: mrodriguez / Estudiante123!');
    console.log('🎓 Estudiante 2: cmendoza / Estudiante123!');
    console.log('👤 Usuario: rjimenez / Usuario123!');
    console.log('❌ Inactivo: sherrera / Estudiante123! (estado: false)');
  },

  async down(queryInterface, Sequelize) {
    await queryInterface.bulkDelete('usuarios', null, {});
    console.log('✅ Usuarios de prueba eliminados');
  }
};