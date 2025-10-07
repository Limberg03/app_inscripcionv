'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('grupos_materia', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      grupo: {
        type: Sequelize.STRING(10),
        allowNull: false
      },
      estado: {
        type: Sequelize.BOOLEAN,
        allowNull: false,
        defaultValue: true
      },
      materia_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'materias',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'RESTRICT'
      },
      docente_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'docentes',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'RESTRICT'
      },
       horario_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'horarios',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'RESTRICT'
      },

       cupo: {
        type: Sequelize.INTEGER,
        allowNull: false
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
    await queryInterface.addIndex('grupos_materia', ['grupo']);
    await queryInterface.addIndex('grupos_materia', ['materia_id']);
    await queryInterface.addIndex('grupos_materia', ['docente_id']);
    await queryInterface.addIndex('grupos_materia', ['horario_id']);

    await queryInterface.addIndex('grupos_materia', ['estado']);

  },
    
  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('grupos_materia');
  }
};