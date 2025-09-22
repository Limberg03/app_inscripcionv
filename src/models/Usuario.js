'use strict';
const { Model } = require('sequelize');
const bcrypt = require('bcryptjs');

module.exports = (sequelize, DataTypes) => {
  class Usuario extends Model {
    static associate(models) {
      // Aquí puedes agregar asociaciones con otros modelos si es necesario
      // Por ejemplo: Usuario.hasMany(models.Inscripcion, { foreignKey: 'usuario_id' });
    }

    // Método para verificar contraseña
    async checkPassword(password) {
      return await bcrypt.compare(password, this.password);
    }

    // Método para obtener datos públicos (sin contraseña)
    toJSON() {
      const values = Object.assign({}, this.get());
      delete values.password;
      return values;
    }
  }

  Usuario.init({
    id: {
      type: DataTypes.INTEGER,
      primaryKey: true,
      autoIncrement: true
    },
    nombre: {
      type: DataTypes.STRING(100),
      allowNull: false,
      validate: {
        notEmpty: { msg: 'El nombre es requerido' },
        len: { args: [2, 100], msg: 'El nombre debe tener entre 2 y 100 caracteres' }
      }
    },
    apellido: {
      type: DataTypes.STRING(100),
      allowNull: false,
      validate: {
        notEmpty: { msg: 'El apellido es requerido' },
        len: { args: [2, 100], msg: 'El apellido debe tener entre 2 y 100 caracteres' }
      }
    },
    email: {
      type: DataTypes.STRING(255),
      allowNull: false,
      unique: {
        msg: 'Este email ya está registrado'
      },
      validate: {
        isEmail: { msg: 'Debe ser un email válido' },
        notEmpty: { msg: 'El email es requerido' }
      }
    },
    username: {
      type: DataTypes.STRING(50),
      allowNull: false,
      unique: {
        msg: 'Este nombre de usuario ya está en uso'
      },
      validate: {
        notEmpty: { msg: 'El nombre de usuario es requerido' },
        len: { args: [3, 50], msg: 'El username debe tener entre 3 y 50 caracteres' },
        isAlphanumeric: { msg: 'El username solo puede contener letras y números' }
      }
    },
    password: {
      type: DataTypes.STRING(255),
      allowNull: false,
      validate: {
        notEmpty: { msg: 'La contraseña es requerida' },
        len: { args: [6, 255], msg: 'La contraseña debe tener al menos 6 caracteres' }
      }
    },
    rol: {
      type: DataTypes.ENUM('admin', 'docente', 'estudiante', 'usuario'),
      allowNull: false,
      defaultValue: 'usuario',
      validate: {
        isIn: {
          args: [['admin', 'docente', 'estudiante', 'usuario']],
          msg: 'El rol debe ser: admin, docente, estudiante o usuario'
        }
      }
    },
    estado: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: true
    },
    ultimo_acceso: {
      type: DataTypes.DATE,
      allowNull: true
    },
    fecha_creacion: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW
    }
  }, {
    sequelize,
    modelName: 'Usuario',
    tableName: 'usuarios',
    timestamps: true,
    createdAt: 'fecha_creacion',
    updatedAt: 'fecha_actualizacion',
    
    // Hooks para encriptar contraseña
    hooks: {
      beforeCreate: async (usuario) => {
        if (usuario.password) {
          const salt = await bcrypt.genSalt(12);
          usuario.password = await bcrypt.hash(usuario.password, salt);
        }
      },
      beforeUpdate: async (usuario) => {
        if (usuario.changed('password')) {
          const salt = await bcrypt.genSalt(12);
          usuario.password = await bcrypt.hash(usuario.password, salt);
        }
      }
    },

    // Índices para mejorar performance
    indexes: [
      { fields: ['email'] },
      { fields: ['username'] },
      { fields: ['rol'] },
      { fields: ['estado'] }
    ]
  });

  return Usuario;
};