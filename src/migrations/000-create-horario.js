'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    await queryInterface.createTable('horarios', {
      id: {
        allowNull: false,
        autoIncrement: true,
        primaryKey: true,
        type: Sequelize.INTEGER
      },
      // fecha: {
      //   type: Sequelize.DATE,
      //   allowNull: false
      // },
       dia: {
        type: Sequelize.STRING(100),
        allowNull: false 
      },
      hora_inicio: {
        type: Sequelize.STRING(100),

        allowNull: false
      },
      hora_fin: {
        type: Sequelize.STRING(100),

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

    await queryInterface.addIndex(
      'horarios',
      ['dia', 'hora_inicio', 'hora_fin'], // Columnas que forman la restricción
      {
        unique: true,
      }
    );
    
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.dropTable('horarios');
  }
};