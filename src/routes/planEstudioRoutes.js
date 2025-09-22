const express = require('express');
const router = express.Router();
const planEstudioController = require('../controllers/planEstudioController');

// Obtener todos los planes de estudio
router.get('/', planEstudioController.getAll);

// Obtener plan de estudio por ID
router.get('/:id',  planEstudioController.getById);

// Crear nuevo plan de estudio
router.post('/',  planEstudioController.create);

// Actualizar plan de estudio
router.put('/:id',  planEstudioController.update);

// Actualización parcial
router.patch('/:id', planEstudioController.patch);

module.exports = router;