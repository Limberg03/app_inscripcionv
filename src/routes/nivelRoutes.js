const express = require('express');
const router = express.Router();
const nivelController = require('../controllers/nivelController');

// Obtener todos los niveles
router.get('/', nivelController.getAll);

// Obtener nivel por ID
router.get('/:id',  nivelController.getById);

// Crear nuevo nivel
router.post('/',  nivelController.create);

// Actualizar nivel
router.put('/:id', nivelController.update);

// Actualización parcial
router.patch('/:id',  nivelController.patch);

// Eliminar nivel
router.delete('/:id',  nivelController.delete);

module.exports = router;