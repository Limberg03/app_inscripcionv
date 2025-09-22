const express = require('express');
const router = express.Router();
const carreraControllerSync = require('../controllers_sync/carreraControllerSync');
const validators = require('../validators');

// Obtener todas las carreras
router.get('/', carreraControllerSync.getAll);

// Obtener carrera por ID
router.get('/:id', validators.common.idParam, carreraControllerSync.getById);

// Crear nueva carrera
router.post('/', validators.carrera.create, carreraControllerSync.create);

// Actualizar carrera completa
router.put('/:id', validators.carrera.update, carreraControllerSync.update);

// Actualización parcial
router.patch('/:id', validators.carrera.patch, carreraControllerSync.patch);

// Eliminar carrera
router.delete('/:id', validators.common.idParam, carreraControllerSync.delete);

module.exports = router;