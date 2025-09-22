const express = require('express');
const router = express.Router();
const carreraController = require('../controllers/carreraController');
const validators = require('../validators');

// Obtener todas las carreras
router.get('/', carreraController.getAll);

// Obtener carrera por ID
router.get('/:id', validators.common.idParam, carreraController.getById);

// Crear nueva carrera
router.post('/', validators.carrera.create, carreraController.create);

// Actualizar carrera completa
router.put('/:id', validators.carrera.update, carreraController.update);

// Actualización parcial
router.patch('/:id', validators.carrera.patch, carreraController.patch);

// Eliminar carrera
router.delete('/:id', validators.common.idParam, carreraController.delete);

module.exports = router;