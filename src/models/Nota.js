const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const Nota = sequelize.define('Nota', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  calificacion: {
    type: DataTypes.DECIMAL(5, 2),
    allowNull: false,
    validate: {
      min: 0,
      max: 100
    }
  },   
  observacion: {
    type: DataTypes.TEXT,
    allowNull: true
  },
  grupoMateriaId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'grupo_materia_id',
    references: {
      model: 'grupos_materia',
      key: 'id'
    }
  },
  estudianteId: {
    type: DataTypes.INTEGER,
    allowNull: false,
    field: 'estudiante_id',
    references: {
      model: 'estudiantes',
      key: 'id'
    }
  }
}, {
  tableName: 'notas',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at',
  indexes: [
    {
      unique: true,
      fields: ['grupo_materia_id', 'estudiante_id']
    }
  ]
});

module.exports = Nota;