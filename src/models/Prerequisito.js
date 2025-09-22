const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const Prerequisito = sequelize.define('Prerequisito', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  materiaId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'materia_id',
    references: {
      model: 'materias',
      key: 'id'
    }
  },
  requiereId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'requiere_id',
    references: {
      model: 'materias',
      key: 'id'
    }
  }
}, {
  tableName: 'prerequisitos',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at',
  indexes: [
    {
      unique: true,
      fields: ['materia_id', 'requiere_id']
    }
  ]
});

module.exports = Prerequisito;