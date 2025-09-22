const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const Materia = sequelize.define('Materia', {
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
  sigla: {
    type: DataTypes.STRING(20),
    allowNull: false,
    unique: true,
    validate: {
      notEmpty: true,
      len: [1, 20]
    }
  },
  creditos: {
    type: DataTypes.INTEGER,
    allowNull: false,
    validate: {
      min: 1
    }
  },
  nivelId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'nivel_id',
    references: {
      model: 'niveles',
      key: 'id'
    }
  },
  // AGREGAR ESTE CAMPO
  planEstudioId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'plan_estudio_id',
    references: {
      model: 'planes_estudio',
      key: 'id'
    }
  }
}, {
  tableName: 'materias',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at'
});

module.exports = Materia;