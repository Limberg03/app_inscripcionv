const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');


const Estudiante = sequelize.define('Estudiante', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  registro: {
    type: DataTypes.STRING(20),
    allowNull: false,
    unique: true,
    validate: {
      notEmpty: true,
      len: [1, 20]
    }
  },
  nombre: {
    type: DataTypes.STRING(50),
    allowNull: false,
      unique: true, 
    validate: {
      notEmpty: true,
      len: [1, 50]
    }
  },
  telefono: {
    type: DataTypes.STRING(20),
    allowNull: true,
    validate: {
      len: [0, 20]
    }
  },
  fechaNac: {
    type: DataTypes.DATE,
    allowNull: true,
    field: 'fecha_nac'
  }
}, {
  tableName: 'estudiantes',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at'
});

module.exports = Estudiante;