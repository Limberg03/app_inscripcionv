const express = require('express');
const router = express.Router();
const aulaController = require('../controllers/aulaController');
const validators = require('../validators');

// Obtener todas las aulas
router.get('/', aulaController.getAll);

// Obtener aulas disponibles
router.get('/disponibles', aulaController.getAvailable);

// Obtener aula por ID
router.get('/:id',  aulaController.getById);

// Crear nueva aula
router.post('/',  aulaController.create);

// Actualizar aula
router.put('/:id', aulaController.update);

// Actualización parcial
router.patch('/:id', aulaController.patch);

// Eliminar aula
router.delete('/:id', aulaController.delete);

module.exports = router;