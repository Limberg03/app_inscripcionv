const express = require('express');
const router = express.Router();
const materiaControllerSync = require('../controllers_sync/materiaControllerSync');

// Obtener todas las materias
router.get('/', materiaControllerSync.getAll);

// RUTAS ESPECÍFICAS PRIMERO (antes de /:id)
// Obtener materias por plan de estudio
router.get('/plan-estudio/:planEstudioId', materiaControllerSync.getByPlanEstudio);

// Obtener materias por nivel
router.get('/nivel/:nivelId', materiaControllerSync.getByNivel);

// RUTAS CON PARÁMETROS GENERALES AL FINAL
// Obtener materia por ID
router.get('/:id', materiaControllerSync.getById);

// Crear nueva materia
router.post('/', materiaControllerSync.create);

// Actualizar materia
router.put('/:id', materiaControllerSync.update);

// Actualización parcial
router.patch('/:id', materiaControllerSync.patch);

// Eliminar materia
router.delete('/:id', materiaControllerSync.delete);

module.exports = router;