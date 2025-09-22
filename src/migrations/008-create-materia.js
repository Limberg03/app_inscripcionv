'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('materias', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      nombre: {
        type: Sequelize.STRING(100),
        allowNull: false,
        unique: true

      },
      sigla: {
        type: Sequelize.STRING(20),
        allowNull: false,
        unique: true
      },
      creditos: {
        type: Sequelize.INTEGER,
        allowNull: false
      },
      nivel_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'niveles',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'RESTRICT'
      },
 plan_estudio_id: {
        type: Sequelize.INTEGER,
        allowNull: false,
        references: {
          model: 'planes_estudio',
          key: 'id'
        },
        onUpdate: 'CASCADE',
        onDelete: 'RESTRICT'
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
    await queryInterface.addIndex('materias', ['sigla']);
    await queryInterface.addIndex('materias', ['nombre']);
    await queryInterface.addIndex('materias', ['nivel_id']);
     await queryInterface.addIndex('materias', ['plan_estudio_id']); 
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('materias');
  }
};