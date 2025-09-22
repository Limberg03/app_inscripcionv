const express = require('express');
const router = express.Router();
const materiaController = require('../controllers/materiaController');

// Obtener todas las materias
router.get('/', materiaController.getAll);

// RUTAS ESPECÍFICAS PRIMERO (antes de /:id)
// Obtener materias por plan de estudio
router.get('/plan-estudio/:planEstudioId', materiaController.getByPlanEstudio);

// Obtener materias por nivel
router.get('/nivel/:nivelId', materiaController.getByNivel);

// RUTAS CON PARÁMETROS GENERALES AL FINAL
// Obtener materia por ID
router.get('/:id', materiaController.getById);

// Crear nueva materia
router.post('/', materiaController.create);

// Actualizar materia
router.put('/:id', materiaController.update);

// Actualización parcial
router.patch('/:id', materiaController.patch);

// Eliminar materia
router.delete('/:id', materiaController.delete);

module.exports = router;