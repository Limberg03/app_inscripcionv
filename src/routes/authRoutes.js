const express = require('express');
const router = express.Router();

// Importar controlador y middlewares
const authController = require('../controllers/authController');
const { 
  authMiddleware, 
  roleMiddleware, 
  optionalAuthMiddleware 
} = require('../middleware/authMiddleware');

// Importar validaciones
const {
  registerValidation,
  loginValidation,
  changePasswordValidation,
  refreshTokenValidation
} = require('../validators/authValidators');

// ==========================================
// RUTAS PÚBLICAS (No requieren autenticación)
// ==========================================

/**
 * @route   POST /api/auth/register
 * @desc    Registrar nuevo usuario
 * @access  Public
 * @body    { nombre, apellido, email, username, password, confirmPassword, rol? }
 */
router.post('/register', registerValidation, authController.register);

/**
 * @route   POST /api/auth/login
 * @desc    Iniciar sesión
 * @access  Public
 * @body    { login, password }
 */
router.post('/login', loginValidation, authController.login);

/**
 * @route   POST /api/auth/refresh-token
 * @desc    Renovar token de acceso usando refresh token
 * @access  Public
 * @body    { refreshToken }
 */
router.post('/refresh-token', refreshTokenValidation, authController.refreshToken);

// ==========================================
// RUTAS PRIVADAS (Requieren autenticación)
// ==========================================

/**
 * @route   GET /api/auth/profile
 * @desc    Obtener perfil del usuario autenticado
 * @access  Private
 * @header  Authorization: Bearer <token>
 */
router.get('/profile', authMiddleware, authController.profile);

/**
 * @route   POST /api/auth/logout
 * @desc    Cerrar sesión (opcional para logs)
 * @access  Private
 * @header  Authorization: Bearer <token>
 */
router.post('/logout', authMiddleware, authController.logout);

/**
 * @route   PUT /api/auth/change-password
 * @desc    Cambiar contraseña del usuario autenticado
 * @access  Private
 * @header  Authorization: Bearer <token>
 * @body    { currentPassword, newPassword, confirmNewPassword }
 */
router.put('/change-password', 
  authMiddleware, 
  changePasswordValidation, 
  authController.changePassword
);

// ==========================================
// RUTAS ADICIONALES ÚTILES
// ==========================================

/**
 * @route   GET /api/auth/verify-token
 * @desc    Verificar si el token es válido
 * @access  Private
 * @header  Authorization: Bearer <token>
 */
router.get('/verify-token', authMiddleware, (req, res) => {
  res.status(200).json({
    success: true,
    message: 'Token válido',
    data: {
      user: req.user,
      tokenValid: true
    }
  });
});

/**
 * @route   GET /api/auth/me
 * @desc    Alias para obtener perfil (más intuitivo)
 * @access  Private
 * @header  Authorization: Bearer <token>
 */
router.get('/me', authMiddleware, authController.profile);

// ==========================================
// RUTAS PARA ROLES ESPECÍFICOS (Ejemplos)
// ==========================================

/**
 * @route   GET /api/auth/admin-only
 * @desc    Ruta solo para administradores (ejemplo)
 * @access  Private (Admin only)
 * @header  Authorization: Bearer <token>
 */
router.get('/admin-only', 
  authMiddleware, 
  roleMiddleware('admin'), 
  (req, res) => {
    res.status(200).json({
      success: true,
      message: 'Acceso solo para administradores',
      data: {
        user: req.user,
        message: 'Solo los administradores pueden ver este contenido'
      }
    });
  }
);

/**
 * @route   GET /api/auth/teacher-student
 * @desc    Ruta para docentes y estudiantes (ejemplo)
 * @access  Private (Teacher/Student only)
 * @header  Authorization: Bearer <token>
 */
router.get('/teacher-student', 
  authMiddleware, 
  roleMiddleware('docente', 'estudiante'), 
  (req, res) => {
    res.status(200).json({
      success: true,
      message: 'Acceso para docentes y estudiantes',
      data: {
        user: req.user,
        message: 'Contenido académico disponible'
      }
    });
  }
);

// ==========================================
// INFORMACIÓN DE LA API AUTH
// ==========================================

/**
 * @route   GET /api/auth/
 * @desc    Información sobre las rutas de autenticación
 * @access  Public
 */
router.get('/', (req, res) => {
  res.status(200).json({
    success: true,
    message: 'API de Autenticación - Sistema Académico',
    version: '1.0.0',
    endpoints: {
      public: {
        register: {
          method: 'POST',
          path: '/api/auth/register',
          description: 'Registrar nuevo usuario',
          body: ['nombre', 'apellido', 'email', 'username', 'password', 'confirmPassword', 'rol?']
        },
        login: {
          method: 'POST',
          path: '/api/auth/login',
          description: 'Iniciar sesión',
          body: ['login (email o username)', 'password']
        },
        refreshToken: {
          method: 'POST',
          path: '/api/auth/refresh-token',
          description: 'Renovar token de acceso',
          body: ['refreshToken']
        }
      },
      private: {
        profile: {
          method: 'GET',
          path: '/api/auth/profile',
          description: 'Obtener perfil del usuario',
          headers: ['Authorization: Bearer <token>']
        },
        logout: {
          method: 'POST',
          path: '/api/auth/logout',
          description: 'Cerrar sesión',
          headers: ['Authorization: Bearer <token>']
        },
        changePassword: {
          method: 'PUT',
          path: '/api/auth/change-password',
          description: 'Cambiar contraseña',
          headers: ['Authorization: Bearer <token>'],
          body: ['currentPassword', 'newPassword', 'confirmNewPassword']
        },
        verifyToken: {
          method: 'GET',
          path: '/api/auth/verify-token',
          description: 'Verificar validez del token',
          headers: ['Authorization: Bearer <token>']
        }
      }
    },
    authentication: {
      type: 'JWT (JSON Web Token)',
      headerFormat: 'Authorization: Bearer <token>',
      tokenExpiry: '24 horas',
      refreshTokenExpiry: '7 días'
    },
    roles: ['admin', 'docente', 'estudiante', 'usuario'],
    security: {
      passwordHashing: 'bcrypt (salt rounds: 12)',
      tokenSigning: 'HMAC SHA256',
      validation: 'express-validator'
    }
  });
});

module.exports = router;