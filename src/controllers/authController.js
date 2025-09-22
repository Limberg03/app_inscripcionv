const { Usuario } = require('../models');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize'); // ¡IMPORTAR Op!

// Generar JWT Token
const generateToken = (payload) => {
  return jwt.sign(
    payload,
    process.env.JWT_SECRET || 'mi_clave_secreta_super_segura_2024',
    { 
      expiresIn: process.env.JWT_EXPIRE || '24h',
      issuer: 'academic-system',
      audience: 'academic-users'
    }
  );
};

// Generar Refresh Token (opcional, para mayor seguridad)
const generateRefreshToken = (payload) => {
  return jwt.sign(
    payload,
    process.env.JWT_REFRESH_SECRET || 'mi_refresh_secret_super_seguro_2024',
    { 
      expiresIn: '7d',
      issuer: 'academic-system',
      audience: 'academic-users'
    }
  );
};

const authController = {
  // Registro de usuario
  async register(req, res) {
    try {
      // Verificar errores de validación
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { nombre, apellido, email, username, password, rol } = req.body;

      // Verificar si el usuario ya existe (SINTAXIS CORREGIDA)
      const existingUser = await Usuario.findOne({
        where: {
          [Op.or]: [{ email }, { username }] // Usar Op.or en lugar de $or
        }
      });

      if (existingUser) {
        return res.status(409).json({
          success: false,
          message: 'El email o username ya están registrados',
          field: existingUser.email === email ? 'email' : 'username'
        });
      }

      // Crear nuevo usuario (el password se encripta automáticamente en el hook)
      const nuevoUsuario = await Usuario.create({
        nombre,
        apellido,
        email,
        username,
        password,
        rol: rol || 'usuario'
      });

      // Generar tokens
      const tokenPayload = {
        id: nuevoUsuario.id,
        username: nuevoUsuario.username,
        email: nuevoUsuario.email,
        rol: nuevoUsuario.rol
      };

      const token = generateToken(tokenPayload);
      const refreshToken = generateRefreshToken({ id: nuevoUsuario.id });

      res.status(201).json({
        success: true,
        message: 'Usuario registrado exitosamente',
        data: {
          usuario: nuevoUsuario,
          token,
          refreshToken,
          tokenType: 'Bearer',
          expiresIn: '24h'
        }
      });

    } catch (error) {
      console.error('Error en registro:', error);
      
      // Manejar errores de Sequelize
      if (error.name === 'SequelizeValidationError') {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: error.errors.map(err => ({
            field: err.path,
            message: err.message
          }))
        });
      }

      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(409).json({
          success: false,
          message: 'El email o username ya están registrados'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error interno del servidor',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  },

  // Inicio de sesión
  async login(req, res) {
    try {
      // Verificar errores de validación
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { login, password } = req.body; // login puede ser email o username

      // Buscar usuario por email o username (SINTAXIS CORREGIDA)
      const usuario = await Usuario.findOne({
        where: {
          [Op.or]: [ // Usar Op.or en lugar de $or
            { email: login },
            { username: login }
          ],
          estado: true // Solo usuarios activos
        }
      });

      if (!usuario) {
        return res.status(401).json({
          success: false,
          message: 'Credenciales inválidas'
        });
      }

      // Verificar contraseña
      const isPasswordValid = await usuario.checkPassword(password);
      
      if (!isPasswordValid) {
        return res.status(401).json({
          success: false,
          message: 'Credenciales inválidas'
        });
      }

      // Actualizar último acceso
      await usuario.update({ ultimo_acceso: new Date() });

      // Generar tokens
      const tokenPayload = {
        id: usuario.id,
        username: usuario.username,
        email: usuario.email,
        rol: usuario.rol
      };

      const token = generateToken(tokenPayload);
      const refreshToken = generateRefreshToken({ id: usuario.id });

      res.status(200).json({
        success: true,
        message: 'Inicio de sesión exitoso',
        data: {
          usuario,
          token,
          refreshToken,
          tokenType: 'Bearer',
          expiresIn: '24h'
        }
      });

    } catch (error) {
      console.error('Error en login:', error);
      res.status(500).json({
        success: false,
        message: 'Error interno del servidor',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  },

  // Obtener perfil del usuario autenticado
  async profile(req, res) {
    try {
      // El usuario viene del middleware de autenticación
      const usuario = await Usuario.findByPk(req.user.id, {
        attributes: { exclude: ['password'] }
      });

      if (!usuario) {
        return res.status(404).json({
          success: false,
          message: 'Usuario no encontrado'
        });
      }

      res.status(200).json({
        success: true,
        message: 'Perfil obtenido exitosamente',
        data: usuario
      });

    } catch (error) {
      console.error('Error al obtener perfil:', error);
      res.status(500).json({
        success: false,
        message: 'Error interno del servidor',
        error: process.env.NODE_ENV === 'development' ? error.message : undefined
      });
    }
  },

  // Renovar token (usando refresh token)
  async refreshToken(req, res) {
    try {
      const { refreshToken } = req.body;

      if (!refreshToken) {
        return res.status(400).json({
          success: false,
          message: 'Refresh token requerido'
        });
      }

      // Verificar refresh token
      const decoded = jwt.verify(
        refreshToken,
        process.env.JWT_REFRESH_SECRET || 'mi_refresh_secret_super_seguro_2024'
      );

      // Buscar usuario
      const usuario = await Usuario.findByPk(decoded.id);
      
      if (!usuario || !usuario.estado) {
        return res.status(401).json({
          success: false,
          message: 'Usuario no válido'
        });
      }

      // Generar nuevo token
      const tokenPayload = {
        id: usuario.id,
        username: usuario.username,
        email: usuario.email,
        rol: usuario.rol
      };

      const newToken = generateToken(tokenPayload);
      const newRefreshToken = generateRefreshToken({ id: usuario.id });

      res.status(200).json({
        success: true,
        message: 'Token renovado exitosamente',
        data: {
          token: newToken,
          refreshToken: newRefreshToken,
          tokenType: 'Bearer',
          expiresIn: '24h'
        }
      });

    } catch (error) {
      console.error('Error al renovar token:', error);
      
      if (error.name === 'JsonWebTokenError') {
        return res.status(401).json({
          success: false,
          message: 'Token inválido'
        });
      }

      if (error.name === 'TokenExpiredError') {
        return res.status(401).json({
          success: false,
          message: 'Token expirado'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error interno del servidor'
      });
    }
  },

  // Cerrar sesión (opcional - en JWT no es necesario, pero útil para logs)
  async logout(req, res) {
    try {
      // Actualizar último acceso
      await Usuario.update(
        { ultimo_acceso: new Date() },
        { where: { id: req.user.id } }
      );

      res.status(200).json({
        success: true,
        message: 'Sesión cerrada exitosamente'
      });

    } catch (error) {
      console.error('Error en logout:', error);
      res.status(500).json({
        success: false,
        message: 'Error interno del servidor'
      });
    }
  },

  // Cambiar contraseña
  async changePassword(req, res) {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { currentPassword, newPassword } = req.body;

      // Buscar usuario
      const usuario = await Usuario.findByPk(req.user.id);
      
      if (!usuario) {
        return res.status(404).json({
          success: false,
          message: 'Usuario no encontrado'
        });
      }

      // Verificar contraseña actual
      const isCurrentPasswordValid = await usuario.checkPassword(currentPassword);
      
      if (!isCurrentPasswordValid) {
        return res.status(400).json({
          success: false,
          message: 'La contraseña actual es incorrecta'
        });
      }

      // Actualizar contraseña (se encripta automáticamente en el hook)
      await usuario.update({ password: newPassword });

      res.status(200).json({
        success: true,
        message: 'Contraseña cambiada exitosamente'
      });

    } catch (error) {
      console.error('Error al cambiar contraseña:', error);
      res.status(500).json({
        success: false,
        message: 'Error interno del servidor'
      });
    }
  }
};

module.exports = authController;