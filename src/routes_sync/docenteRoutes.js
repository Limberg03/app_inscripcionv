const express = require('express');
const router = express.Router();
const docenteControllerSync = require('../controllers_sync/docenteControllerSync');
//const validators = require('../validators');

// Obtener todos los docentes
router.get('/', docenteControllerSync.getAll);

// Obtener docente por ID
router.get('/:id', docenteControllerSync.getById);

// Crear nuevo docente
router.post('/', docenteControllerSync.create);

// Actualizar docente
router.put('/:id', docenteControllerSync.update);

// Actualización parcial
router.patch('/:id',  docenteControllerSync.patch);

// Eliminar docente
router.delete('/:id', docenteControllerSync.delete);

module.exports = router;