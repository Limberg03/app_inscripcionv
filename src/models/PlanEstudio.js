const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const PlanEstudio = sequelize.define('PlanEstudio', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  nombre: {
    type: DataTypes.STRING(100),
    allowNull: false,
    validate: {
      notEmpty: true,
      len: [1, 100]
    }
  },
  fechaInicio: {
    type: DataTypes.DATE,
    allowNull: false,
    field: 'fecha_inicio'
  },
  fechaFin: {
    type: DataTypes.DATE,
    allowNull: true,
    field: 'fecha_fin'
  }
}, {
  tableName: 'planes_estudio',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at'
});

module.exports = PlanEstudio;