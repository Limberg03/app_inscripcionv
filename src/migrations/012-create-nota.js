'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('notas', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      calificacion: {
        type: Sequelize.DECIMAL(5, 2),
        allowNull: false
      },
      observacion: {
        type: Sequelize.TEXT,
        allowNull: true
      },
      grupo_materia_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'grupos_materia',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'CASCADE'
      },
      estudiante_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'estudiantes',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'CASCADE'
      },
      created_at: {
        allowNull: false,
        type: Sequelize.DATE,
        defaultValue: Sequelize.NOW
      },
      updated_at: {
        allowNull: false,
        type: Sequelize.DATE,
        defaultValue: Sequelize.NOW
      }
    });

    // Crear índices
    await queryInterface.addIndex('notas', ['calificacion']);
    await queryInterface.addIndex('notas', ['grupo_materia_id']);
    await queryInterface.addIndex('notas', ['estudiante_id']);
    await queryInterface.addIndex('notas', ['grupo_materia_id', 'estudiante_id'], {
      unique: true,
      name: 'notas_unique_constraint'
    });
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('notas');
  }
};