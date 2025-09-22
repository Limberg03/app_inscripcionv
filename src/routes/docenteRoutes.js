const express = require('express');
const router = express.Router();
const docenteController = require('../controllers/docenteController');
//const validators = require('../validators');

// Obtener todos los docentes
router.get('/', docenteController.getAll);

// Obtener docente por ID
router.get('/:id', docenteController.getById);

// Crear nuevo docente
router.post('/', docenteController.create);

// Actualizar docente
router.put('/:id', docenteController.update);

// Actualización parcial
router.patch('/:id',  docenteController.patch);

// Eliminar docente
router.delete('/:id', docenteController.delete);

module.exports = router;