const express = require('express');
const router = express.Router();
const aulaControllerSync = require('../controllers_sync/aulaControllerSync');
const validators = require('../validators');

// Obtener todas las aulas
router.get('/', aulaControllerSync.getAll);

// Obtener aulas disponibles
router.get('/disponibles', aulaControllerSync.getAvailable);

// Obtener aula por ID
router.get('/:id',  aulaControllerSync.getById);

// Crear nueva aula
router.post('/',  aulaControllerSync.create);

// Actualizar aula
router.put('/:id', aulaControllerSync.update);

// Actualización parcial
router.patch('/:id', aulaControllerSync.patch);

// Eliminar aula
router.delete('/:id', aulaControllerSync.delete);

module.exports = router;