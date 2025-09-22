const { body, param } = require('express-validator');
const { Estudiante, Inscripcion, Carrera, PlanEstudio } = require('../models');
const { Op } = require('sequelize');

const validators = {
  // Validadores para Estudiante
  estudiante: {
    create: [
      body('nombre')
        .notEmpty()
        .withMessage('El número de estudiante es requerido')
        .isLength({ max: 20 })
        .withMessage('El número no puede exceder 20 caracteres')
        .custom(async (value) => {
          // Validación asíncrona - verificar unicidad
          const existingStudent = await Estudiante.findOne({
            where: { nombre: value }
          });
          if (existingStudent) {
            throw new Error('El número de estudiante ya está registrado');
          }
          return true;
        }),
      
      body('registro')
        .notEmpty()
        .withMessage('El registro (nombre) es requerido')
        .isLength({ max: 50 })
        .withMessage('El registro no puede exceder 50 caracteres')
        .custom(async (value) => {
          // Validación asíncrona - verificar unicidad del registro
          const existingStudent = await Estudiante.findOne({
            where: { registro: value }
          });
          if (existingStudent) {
            throw new Error('El registro ya está en uso');
          }
          return true;
        }),
      
      body('telefono')
        .optional()
        .isLength({ max: 20 })
        .withMessage('El teléfono no puede exceder 20 caracteres')
        .custom(async (value) => {
          // Validación asíncrona opcional - formato de teléfono
          if (value && value.trim()) {
            const phoneRegex = /^[+]?[\d\s\-\(\)]{7,20}$/;
            if (!phoneRegex.test(value)) {
              throw new Error('El formato del teléfono no es válido');
            }
          }
          return true;
        }),
      
      body('fechaNac')
        .optional()
        .isISO8601()
        .withMessage('La fecha de nacimiento debe tener formato válido (YYYY-MM-DD)')
        .custom(async (value) => {
          // Validación asíncrona - verificar edad mínima
          if (value) {
            const birthDate = new Date(value);
            const today = new Date();
            const age = today.getFullYear() - birthDate.getFullYear();
            
            if (age < 15) {
              throw new Error('El estudiante debe tener al menos 15 años');
            }
            if (age > 80) {
              throw new Error('Por favor verificar la fecha de nacimiento');
            }
          }
          return true;
        })
    ],

    update: [
      param('id')
        .isInt({ min: 1 })
        .withMessage('El ID debe ser un número entero positivo')
        .custom(async (value) => {
          // Verificar que el estudiante existe
          const student = await Estudiante.findByPk(value);
          if (!student) {
            throw new Error('El estudiante no existe');
          }
          return true;
        }),
      
      body('nombre')
        .notEmpty()
        .withMessage('El nombre de estudiante es requerido')
        .isLength({ max: 20 })
        .withMessage('El número no puede exceder 20 caracteres')
        .custom(async (value, { req }) => {
          // Validación asíncrona - verificar unicidad excluyendo el registro actual
          const existingStudent = await Estudiante.findOne({
            where: { 
              nombre: value,
              id: { [Op.ne]: req.params.id }
            }
          });
          if (existingStudent) {
            throw new Error('El número de estudiante ya está registrado');
          }
          return true;
        }),
      
      body('registro')
        .notEmpty()
        .withMessage('El registro (nombre) es requerido')
        .isLength({ max: 50 })
        .withMessage('El registro no puede exceder 50 caracteres')
        .custom(async (value, { req }) => {
          // Validación asíncrona - verificar unicidad del registro excluyendo el actual
          const existingStudent = await Estudiante.findOne({
            where: { 
              registro: value,
              id: { [Op.ne]: req.params.id }
            }
          });
          if (existingStudent) {
            throw new Error('El registro ya está en uso');
          }
          return true;
        }),
      
      body('telefono')
        .optional()
        .isLength({ max: 20 })
        .withMessage('El teléfono no puede exceder 20 caracteres')
        .custom(async (value) => {
          if (value && value.trim()) {
            const phoneRegex = /^[+]?[\d\s\-\(\)]{7,20}$/;
            if (!phoneRegex.test(value)) {
              throw new Error('El formato del teléfono no es válido');
            }
          }
          return true;
        }),
      
      body('fechaNac')
        .optional()
        .isISO8601()
        .withMessage('La fecha de nacimiento debe tener formato válido (YYYY-MM-DD)')
        .custom(async (value) => {
          if (value) {
            const birthDate = new Date(value);
            const today = new Date();
            const age = today.getFullYear() - birthDate.getFullYear();
            
            if (age < 15) {
              throw new Error('El estudiante debe tener al menos 15 años');
            }
            if (age > 80) {
              throw new Error('Por favor verificar la fecha de nacimiento');
            }
          }
          return true;
        })
    ]
  },

  // Validadores para Inscripción
  inscripcion: {
    create: [
      body('gestion')
        .isInt({ min: 2000, max: 2100 })
        .withMessage('La gestión debe ser un año válido entre 2000 y 2100')
        .custom(async (value) => {
          // Validar que la gestión no sea muy futura
          const currentYear = new Date().getFullYear();
          if (value > currentYear + 2) {
            throw new Error('La gestión no puede ser más de 2 años en el futuro');
          }
          return true;
        }),
      
      body('estudianteId')
        .isInt({ min: 1 })
        .withMessage('El ID del estudiante debe ser un número entero positivo')
        .custom(async (value) => {
          // Verificar que el estudiante existe y está activo
          const student = await Estudiante.findByPk(value);
          if (!student) {
            throw new Error('El estudiante no existe');
          }
          if (!student.estado) {
            throw new Error('El estudiante debe estar activo para inscribirse');
          }
          return true;
        }),
      
      body('carreraId')
        .isInt({ min: 1 })
        .withMessage('El ID de la carrera debe ser un número entero positivo')
        .custom(async (value) => {
          // Verificar que la carrera existe y está activa
          const carrera = await Carrera.findByPk(value);
          if (!carrera) {
            throw new Error('La carrera no existe');
          }
          if (!carrera.estado) {
            throw new Error('La carrera debe estar activa para inscribirse');
          }
          return true;
        }),
      
      body('fecha')
        .optional()
        .isISO8601()
        .withMessage('La fecha debe tener formato válido (YYYY-MM-DD)')
        .custom(async (value, { req }) => {
          // Verificar que no existe inscripción duplicada
          if (req.body.estudianteId && req.body.gestion) {
            const existingInscription = await Inscripcion.findOne({
              where: {
                estudianteId: req.body.estudianteId,
                gestion: req.body.gestion,
                carreraId: req.body.carreraId
              }
            });
            if (existingInscription) {
              throw new Error('El estudiante ya está inscrito en esta carrera para esta gestión');
            }
          }
          return true;
        })
    ],

    update: [
      param('id')
        .isInt({ min: 1 })
        .withMessage('El ID debe ser un número entero positivo')
        .custom(async (value) => {
          const inscription = await Inscripcion.findByPk(value);
          if (!inscription) {
            throw new Error('La inscripción no existe');
          }
          return true;
        }),
      
      body('gestion')
        .optional()
        .isInt({ min: 2000, max: 2100 })
        .withMessage('La gestión debe ser un año válido entre 2000 y 2100')
        .custom(async (value) => {
          if (value) {
            const currentYear = new Date().getFullYear();
            if (value > currentYear + 2) {
              throw new Error('La gestión no puede ser más de 2 años en el futuro');
            }
          }
          return true;
        }),
      
      body('estudianteId')
        .optional()
        .isInt({ min: 1 })
        .withMessage('El ID del estudiante debe ser un número entero positivo')
        .custom(async (value) => {
          if (value) {
            const student = await Estudiante.findByPk(value);
            if (!student) {
              throw new Error('El estudiante no existe');
            }
            if (!student.estado) {
              throw new Error('El estudiante debe estar activo');
            }
          }
          return true;
        }),
      
      body('fecha')
        .optional()
        .isISO8601()
        .withMessage('La fecha debe tener formato válido (YYYY-MM-DD)')
    ]
  },

  carrera: {
    create: [
      body('nombre')
        .notEmpty()
        .withMessage('El nombre es requerido')
        .isLength({ min: 3, max: 100 })
        .withMessage('El nombre debe tener entre 3 y 100 caracteres')
        .custom(async (value) => {
          // Verificar unicidad del nombre de carrera
          const existingCarrera = await Carrera.findOne({
            where: { nombre: value }
          });
          if (existingCarrera) {
            throw new Error('Ya existe una carrera con este nombre');
          }
          return true;
        }),
      
      body('codigo')
        .notEmpty()
        .withMessage('El código es requerido')
        .isLength({ min: 2, max: 20 })
        .withMessage('El código debe tener entre 2 y 20 caracteres')
        .custom(async (value) => {
          // Verificar unicidad del código
          const existingCarrera = await Carrera.findOne({
            where: { codigo: value }
          });
          if (existingCarrera) {
            throw new Error('Ya existe una carrera con este código');
          }
          // Validar formato del código (solo letras y números)
          const codeRegex = /^[A-Z0-9]+$/i;
          if (!codeRegex.test(value)) {
            throw new Error('El código solo puede contener letras y números');
          }
          return true;
        }),
      
      body('modalidad')
        .notEmpty()
        .withMessage('La modalidad es requerida')
        .isIn(['presencial', 'virtual', 'semipresencial'])
        .withMessage('La modalidad debe ser: presencial, virtual o semipresencial'),
      
      body('estado')
        .optional()
        .isBoolean()
        .withMessage('El estado debe ser un valor booleano'),
      
      body('planEstudioId')
        .notEmpty()
        .withMessage('El ID del plan de estudio es requerido')
        .isInt({ min: 1 })
        .withMessage('El ID del plan de estudio debe ser un número entero positivo')
        // .custom(async (value) => {
        //   // Verificar que el plan de estudio existe
        //   const planEstudio = await PlanEstudio.findByPk(value);
        //   if (!planEstudio) {
        //     throw new Error('El plan de estudio no existe');
        //   }
        //   if (!planEstudio.estado) {
        //     throw new Error('El plan de estudio debe estar activo');
        //   }
        //   return true;
        // })
    ],

    update: [
      param('id')
        .isInt({ min: 1 })
        .withMessage('El ID debe ser un número entero positivo')
        .custom(async (value) => {
          const carrera = await Carrera.findByPk(value);
          if (!carrera) {
            throw new Error('La carrera no existe');
          }
          return true;
        }),
      
      body('nombre')
        .notEmpty()
        .withMessage('El nombre es requerido')
        .isLength({ min: 3, max: 100 })
        .withMessage('El nombre debe tener entre 3 y 100 caracteres')
        .custom(async (value, { req }) => {
          const existingCarrera = await Carrera.findOne({
            where: { 
              nombre: value,
              id: { [Op.ne]: req.params.id }
            }
          });
          if (existingCarrera) {
            throw new Error('Ya existe una carrera con este nombre');
          }
          return true;
        }),
      
      body('codigo')
        .notEmpty()
        .withMessage('El código es requerido')
        .isLength({ min: 2, max: 20 })
        .withMessage('El código debe tener entre 2 y 20 caracteres')
        .custom(async (value, { req }) => {
          const existingCarrera = await Carrera.findOne({
            where: { 
              codigo: value,
              id: { [Op.ne]: req.params.id }
            }
          });
          if (existingCarrera) {
            throw new Error('Ya existe una carrera con este código');
          }
          const codeRegex = /^[A-Z0-9]+$/i;
          if (!codeRegex.test(value)) {
            throw new Error('El código solo puede contener letras y números');
          }
          return true;
        }),
      
      body('modalidad')
        .notEmpty()
        .withMessage('La modalidad es requerida')
        .isIn(['presencial', 'virtual', 'semipresencial'])
        .withMessage('La modalidad debe ser: presencial, virtual o semipresencial'),
      
      body('estado')
        .optional()
        .isBoolean()
        .withMessage('El estado debe ser un valor booleano'),
      
      body('planEstudioId')
        .notEmpty()
        .withMessage('El ID del plan de estudio es requerido')
        .isInt({ min: 1 })
        .withMessage('El ID del plan de estudio debe ser un número entero positivo')
        .custom(async (value) => {
          const planEstudio = await PlanEstudio.findByPk(value);
          if (!planEstudio) {
            throw new Error('El plan de estudio no existe');
          }
          if (!planEstudio.estado) {
            throw new Error('El plan de estudio debe estar activo');
          }
          return true;
        })
    ],

    patch: [
      param('id')
        .isInt({ min: 1 })
        .withMessage('El ID debe ser un número entero positivo')
        .custom(async (value) => {
          const carrera = await Carrera.findByPk(value);
          if (!carrera) {
            throw new Error('La carrera no existe');
          }
          return true;
        }),
      
      body('nombre')
        .optional()
        .isLength({ min: 3, max: 100 })
        .withMessage('El nombre debe tener entre 3 y 100 caracteres')
        .custom(async (value, { req }) => {
          if (value) {
            const existingCarrera = await Carrera.findOne({
              where: { 
                nombre: value,
                id: { [Op.ne]: req.params.id }
              }
            });
            if (existingCarrera) {
              throw new Error('Ya existe una carrera con este nombre');
            }
          }
          return true;
        }),
      
      body('codigo')
        .optional()
        .isLength({ min: 2, max: 20 })
        .withMessage('El código debe tener entre 2 y 20 caracteres')
        .custom(async (value, { req }) => {
          if (value) {
            const existingCarrera = await Carrera.findOne({
              where: { 
                codigo: value,
                id: { [Op.ne]: req.params.id }
              }
            });
            if (existingCarrera) {
              throw new Error('Ya existe una carrera con este código');
            }
            const codeRegex = /^[A-Z0-9]+$/i;
            if (!codeRegex.test(value)) {
              throw new Error('El código solo puede contener letras y números');
            }
          }
          return true;
        }),
      
      body('modalidad')
        .optional()
        .isIn(['presencial', 'virtual', 'semipresencial'])
        .withMessage('La modalidad debe ser: presencial, virtual o semipresencial'),
      
      body('estado')
        .optional()
        .isBoolean()
        .withMessage('El estado debe ser un valor booleano'),
      
      body('planEstudioId')
        .optional()
        .isInt({ min: 1 })
        .withMessage('El ID del plan de estudio debe ser un número entero positivo')
        .custom(async (value) => {
          if (value) {
            const planEstudio = await PlanEstudio.findByPk(value);
            if (!planEstudio) {
              throw new Error('El plan de estudio no existe');
            }
            if (!planEstudio.estado) {
              throw new Error('El plan de estudio debe estar activo');
            }
          }
          return true;
        })
    ]
  },

  // Validadores de parámetros comunes
  common: {
    idParam: [
      param('id')
        .isInt({ min: 1 })
        .withMessage('El ID debe ser un número entero positivo')
    ],

    // Validador asíncrono para verificar existencia de cualquier modelo
    existsInModel: (Model, fieldName = 'id') => {
      return param(fieldName)
        .custom(async (value) => {
          const record = await Model.findByPk(value);
          if (!record) {
            throw new Error(`El registro no existe en ${Model.name}`);
          }
          return true;
        });
    },

    // Validador para verificar unicidad
    uniqueInModel: (Model, field, excludeId = null) => {
      return body(field)
        .custom(async (value, { req }) => {
          const where = { [field]: value };
          
          // Excluir el ID actual en caso de actualización
          if (excludeId && req.params && req.params.id) {
            where.id = { [Op.ne]: req.params.id };
          }
          
          const existingRecord = await Model.findOne({ where });
          if (existingRecord) {
            throw new Error(`El ${field} ya está en uso`);
          }
          return true;
        });
    }
  }
};

module.exports = validators;