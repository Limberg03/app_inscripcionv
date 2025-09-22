const { DataTypes } = require('sequelize');
const sequelize = require('../config/database');

const Inscripcion = sequelize.define('Inscripcion', {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true
  },
  fecha: {
    type: DataTypes.DATE,
    allowNull: false,
    defaultValue: DataTypes.NOW,
    validate: {
      isDate: true
    }
  },
  gestion: {
    type: DataTypes.INTEGER,
    allowNull: false,
    validate: {
      isInt: true,
      min: 2000,
      max: 2100
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
},

 

{
  tableName: 'inscripciones',
  timestamps: true,
  createdAt: 'created_at',
  updatedAt: 'updated_at',
   indexes: [
    {
      unique: true,
      fields: ['grupo_materia_id']
    }
  ]
});

module.exports = Inscripcion;