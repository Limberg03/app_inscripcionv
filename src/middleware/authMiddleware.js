const jwt = require('jsonwebtoken');
const { Usuario } = require('../models');

// Middleware de autenticación JWT
const authMiddleware = async (req, res, next) => {
  try {
    // Obtener token del header Authorization
    const authHeader = req.header('Authorization');
    
    if (!authHeader) {
      return res.status(401).json({
        success: false,
        message: 'Token de acceso requerido',
        code: 'TOKEN_MISSING'
      });
    }

    // Verificar formato: "Bearer <token>"
    const token = authHeader.startsWith('Bearer ') 
      ? authHeader.slice(7) 
      : authHeader;

    if (!token) {
      return res.status(401).json({
        success: false,
        message: 'Token de acceso requerido',
        code: 'TOKEN_MISSING'
      });
    }

    // Verificar y decodificar el token
    const decoded = jwt.verify(
      token, 
      process.env.JWT_SECRET || 'mi_clave_secreta_super_segura_2024'
    );

    // Buscar usuario en base de datos para verificar que existe y está activo
    const usuario = await Usuario.findByPk(decoded.id, {
      attributes: { exclude: ['password'] }
    });

    if (!usuario) {
      return res.status(401).json({
        success: false,
        message: 'Usuario no encontrado',
        code: 'USER_NOT_FOUND'
      });
    }

    if (!usuario.estado) {
      return res.status(401).json({
        success: false,
        message: 'Usuario inactivo',
        code: 'USER_INACTIVE'
      });
    }

    // Agregar datos del usuario al request para usar en los controladores
    req.user = {
      id: usuario.id,
      username: usuario.username,
      email: usuario.email,
      rol: usuario.rol,
      nombre: usuario.nombre,
      apellido: usuario.apellido
    };

    next();

  } catch (error) {
    console.error('Error en authMiddleware:', error.message);

    // Manejar diferentes tipos de errores JWT
    if (error.name === 'JsonWebTokenError') {
      return res.status(401).json({
        success: false,
        message: 'Token inválido',
        code: 'INVALID_TOKEN'
      });
    }

    if (error.name === 'TokenExpiredError') {
      return res.status(401).json({
        success: false,
        message: 'Token expirado',
        code: 'TOKEN_EXPIRED'
      });
    }

    if (error.name === 'NotBeforeError') {
      return res.status(401).json({
        success: false,
        message: 'Token no válido aún',
        code: 'TOKEN_NOT_ACTIVE'
      });
    }

    res.status(500).json({
      success: false,
      message: 'Error interno del servidor',
      code: 'INTERNAL_ERROR'
    });
  }
};

// Middleware para verificar roles específicos
const roleMiddleware = (...allowedRoles) => {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        message: 'Usuario no autenticado',
        code: 'NOT_AUTHENTICATED'
      });
    }

    if (!allowedRoles.includes(req.user.rol)) {
      return res.status(403).json({
        success: false,
        message: 'No tienes permisos para acceder a este recurso',
        code: 'INSUFFICIENT_PERMISSIONS',
        requiredRoles: allowedRoles,
        userRole: req.user.rol
      });
    }

    next();
  };
};

// Middleware opcional - continúa aunque no haya token (para rutas públicas con info adicional si hay usuario)
const optionalAuthMiddleware = async (req, res, next) => {
  try {
    const authHeader = req.header('Authorization');
    
    if (!authHeader) {
      req.user = null;
      return next();
    }

    const token = authHeader.startsWith('Bearer ') 
      ? authHeader.slice(7) 
      : authHeader;

    if (!token) {
      req.user = null;
      return next();
    }

    const decoded = jwt.verify(
      token, 
      process.env.JWT_SECRET || 'mi_clave_secreta_super_segura_2024'
    );

    const usuario = await Usuario.findByPk(decoded.id, {
      attributes: { exclude: ['password'] }
    });

    if (usuario && usuario.estado) {
      req.user = {
        id: usuario.id,
        username: usuario.username,
        email: usuario.email,
        rol: usuario.rol,
        nombre: usuario.nombre,
        apellido: usuario.apellido
      };
    } else {
      req.user = null;
    }

    next();

  } catch (error) {
    // En caso de error, simplemente no autenticar
    req.user = null;
    next();
  }
};

// Middleware para verificar si el usuario es el propietario del recurso
const ownershipMiddleware = (resourceIdParam = 'id') => {
  return async (req, res, next) => {
    try {
      const resourceId = req.params[resourceIdParam];
      const userId = req.user.id;

      // Si es admin, puede acceder a cualquier recurso
      if (req.user.rol === 'admin') {
        return next();
      }

      // Verificar si el usuario es propietario del recurso
      // Esto dependerá de tu lógica de negocio específica
      if (parseInt(resourceId) !== userId) {
        return res.status(403).json({
          success: false,
          message: 'No tienes permisos para acceder a este recurso',
          code: 'NOT_OWNER'
        });
      }

      next();

    } catch (error) {
      console.error('Error en ownershipMiddleware:', error);
      res.status(500).json({
        success: false,
        message: 'Error interno del servidor'
      });
    }
  };
};

module.exports = {
  authMiddleware,
  roleMiddleware,
  optionalAuthMiddleware,
  ownershipMiddleware
};