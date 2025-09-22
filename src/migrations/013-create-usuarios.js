'use strict';

module.exports = {
  async up(queryInterface, Sequelize) {
    await queryInterface.createTable('usuarios', {
      id: {
        type: Sequelize.INTEGER,
        primaryKey: true,
        autoIncrement: true,
        allowNull: false
      },
      nombre: {
        type: Sequelize.STRING(100),
        allowNull: false
      },
      apellido: {
        type: Sequelize.STRING(100),
        allowNull: false
      },
      email: {
        type: Sequelize.STRING(255),
        allowNull: false,
        unique: true
      },
      username: {
        type: Sequelize.STRING(50),
        allowNull: false,
        unique: true
      },
      password: {
        type: Sequelize.STRING(255),
        allowNull: false
      },
      rol: {
        type: Sequelize.ENUM('admin', 'docente', 'estudiante', 'usuario'),
        allowNull: false,
        defaultValue: 'usuario'
      },
      estado: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        defaultValue: true
      },
      ultimo_acceso: {
        type: Sequelize.DATE,
        allowNull: true
      },
      fecha_creacion: {
        type: Sequelize.DATE,
        allowNull: false,
        defaultValue: Sequelize.literal('CURRENT_TIMESTAMP')
      },
      fecha_actualizacion: {
        type: Sequelize.DATE,
        allowNull: false,
        defaultValue: Sequelize.literal('CURRENT_TIMESTAMP')
      }
    });

    // Crear índices para mejorar el rendimiento
    await queryInterface.addIndex('usuarios', ['email'], {
      name: 'usuarios_email_idx'
    });

    await queryInterface.addIndex('usuarios', ['username'], {
      name: 'usuarios_username_idx'
    });

    await queryInterface.addIndex('usuarios', ['rol'], {
      name: 'usuarios_rol_idx'
    });

    await queryInterface.addIndex('usuarios', ['estado'], {
      name: 'usuarios_estado_idx'
    });

    await queryInterface.addIndex('usuarios', ['fecha_creacion'], {
      name: 'usuarios_fecha_creacion_idx'
    });
  },

  async down(queryInterface, Sequelize) {
    // Eliminar índices primero
    await queryInterface.removeIndex('usuarios', 'usuarios_email_idx');
    await queryInterface.removeIndex('usuarios', 'usuarios_username_idx');
    await queryInterface.removeIndex('usuarios', 'usuarios_rol_idx');
    await queryInterface.removeIndex('usuarios', 'usuarios_estado_idx');
    await queryInterface.removeIndex('usuarios', 'usuarios_fecha_creacion_idx');

    // Eliminar la tabla
    await queryInterface.dropTable('usuarios');
  }
};