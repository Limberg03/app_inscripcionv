const { body, param, query } = require('express-validator');

// Validaciones para registro
const registerValidation = [
  body('nombre')
    .notEmpty()
    .withMessage('El nombre es requerido')
    .isLength({ min: 2, max: 100 })
    .withMessage('El nombre debe tener entre 2 y 100 caracteres'),

  body('apellido')
    .notEmpty()
    .withMessage('El apellido es requerido')
    .isLength({ min: 2, max: 100 })
    .withMessage('El apellido debe tener entre 2 y 100 caracteres'),

  body('email')
    .notEmpty()
    .withMessage('El email es requerido')
    .isEmail()
    .withMessage('Debe ser un email válido')
    .normalizeEmail(),

  body('username')
    .notEmpty()
    .withMessage('El nombre de usuario es requerido')
    .isLength({ min: 3, max: 50 })
    .withMessage('El username debe tener entre 3 y 50 caracteres'),

  body('password')
    .notEmpty()
    .withMessage('La contraseña es requerida')
    .isLength({ min: 6, max: 128 })
    .withMessage('La contraseña debe tener entre 6 y 128 caracteres'),

  

  body('rol')
    .optional()
    .isIn(['admin', 'docente', 'estudiante', 'usuario'])
    .withMessage('El rol debe ser: admin, docente, estudiante o usuario')
];

// Validaciones para login
const loginValidation = [
  body('login')
    .notEmpty()
    .withMessage('Email o username requerido')
    .isLength({ min: 3, max: 255 })
    .withMessage('Email o username debe tener entre 3 y 255 caracteres'),

  body('password')
    .notEmpty()
    .withMessage('La contraseña es requerida')
    .isLength({ min: 1, max: 128 })
    .withMessage('Contraseña inválida')
];

// Validaciones para cambio de contraseña
const changePasswordValidation = [
  body('currentPassword')
    .notEmpty()
    .withMessage('La contraseña actual es requerida'),

  body('newPassword')
    .notEmpty()
    .withMessage('La nueva contraseña es requerida')
    .isLength({ min: 6, max: 128 })
    .withMessage('La nueva contraseña debe tener entre 6 y 128 caracteres')
    .custom(async (value, { req }) => {
      if (value === req.body.currentPassword) {
        throw new Error('La nueva contraseña debe ser diferente a la actual');
      }
      return true;
    }),

  body('confirmNewPassword')
    .notEmpty()
    .withMessage('La confirmación de la nueva contraseña es requerida')
    .custom(async (value, { req }) => {
      if (value !== req.body.newPassword) {
        throw new Error('Las contraseñas nuevas no coinciden');
      }
      return true;
    })
];

// Validación para refresh token
const refreshTokenValidation = [
  body('refreshToken')
    .notEmpty()
    .withMessage('Refresh token requerido')
    .isJWT()
    .withMessage('Refresh token debe ser un JWT válido')
];

// Validación para parámetros de ID
const idParamValidation = [
  param('id')
    .isInt({ min: 1 })
    .withMessage('ID debe ser un número entero positivo')
];

// Validaciones para consultas con paginación
const paginationValidation = [
  query('page')
    .optional()
    .isInt({ min: 1 })
    .withMessage('La página debe ser un número entero positivo'),

  query('limit')
    .optional()
    .isInt({ min: 1, max: 100 })
    .withMessage('El límite debe ser un número entre 1 y 100'),

  query('sort')
    .optional()
    .isIn(['id', 'nombre', 'apellido', 'email', 'username', 'fecha_creacion', 'ultimo_acceso'])
    .withMessage('Campo de ordenación no válido'),

  query('order')
    .optional()
    .isIn(['ASC', 'DESC', 'asc', 'desc'])
    .withMessage('El orden debe ser ASC o DESC')
];

// Validación para filtros de búsqueda
const searchValidation = [
  query('search')
    .optional()
    .isLength({ min: 1, max: 100 })
    .withMessage('El término de búsqueda debe tener entre 1 y 100 caracteres')
    .trim()
    .escape(),

  query('rol')
    .optional()
    .isIn(['admin', 'docente', 'estudiante', 'usuario'])
    .withMessage('El rol debe ser: admin, docente, estudiante o usuario'),

  query('estado')
    .optional()
    .isBoolean()
    .withMessage('El estado debe ser true o false')
];

// Validación para actualización de perfil
const updateProfileValidation = [
  body('nombre')
    .optional()
    .isLength({ min: 2, max: 100 })
    .withMessage('El nombre debe tener entre 2 y 100 caracteres'),

  body('apellido')
    .optional()
    .isLength({ min: 2, max: 100 })
    .withMessage('El apellido debe tener entre 2 y 100 caracteres'),

  body('email')
    .optional()
    .isEmail()
    .withMessage('Debe ser un email válido')
    .normalizeEmail()
    .isLength({ max: 255 })
    .withMessage('El email no puede exceder 255 caracteres'),

  body('username')
    .optional()
    .isLength({ min: 3, max: 50 })
    .withMessage('El username debe tener entre 3 y 50 caracteres')
];

module.exports = {
  registerValidation,
  loginValidation,
  changePasswordValidation,
  refreshTokenValidation,
  idParamValidation,
  paginationValidation,
  searchValidation,
  updateProfileValidation
};