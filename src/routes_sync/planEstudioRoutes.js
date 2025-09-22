const express = require('express');
const router = express.Router();
const planEstudioControllerSync = require('../controllers_sync/planEstudioControllerSync');

// Obtener todos los planes de estudio
router.get('/', planEstudioControllerSync.getAll);

// Obtener plan de estudio por ID
router.get('/:id',  planEstudioControllerSync.getById);

// Crear nuevo plan de estudio
router.post('/',  planEstudioControllerSync.create);

// Actualizar plan de estudio
router.put('/:id',  planEstudioControllerSync.update);

// Actualización parcial
router.patch('/:id', planEstudioControllerSync.patch);

module.exports = router;