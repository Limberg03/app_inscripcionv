const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const Horario = sequelize.define('Horario', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  dia: {
    type: DataTypes.STRING(80),
    allowNull: false
  },
  horaInicio: {
    type: DataTypes.STRING(60),
    allowNull: false,
    field: 'hora_inicio'
  },
  horaFin: {
    type: DataTypes.STRING(60),
    allowNull: false,
    field: 'hora_fin'
  },
    
}, {
  tableName: 'horarios',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at',
});

module.exports = Horario;