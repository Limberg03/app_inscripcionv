const express = require('express');
const router = express.Router();
const nivelControllerSync = require('../controllers_sync/nivelControllerSync');

// Obtener todos los niveles
router.get('/', nivelControllerSync.getAll);

// Obtener nivel por ID
router.get('/:id',  nivelControllerSync.getById);

// Crear nuevo nivel
router.post('/',  nivelControllerSync.create);

// Actualizar nivel
router.put('/:id', nivelControllerSync.update);

// Actualización parcial
router.patch('/:id',  nivelControllerSync.patch);

// Eliminar nivel
router.delete('/:id',  nivelControllerSync.delete);

module.exports = router;