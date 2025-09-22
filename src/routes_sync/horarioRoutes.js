const express = require('express');
const router = express.Router();
const horarioControllerSync = require('../controllers_sync/horarioControllerSync');

// Obtener todos los horarios
router.get('/', horarioControllerSync.getAll);

// Obtener horario por ID
router.get('/:id',  horarioControllerSync.getById);

// Crear nuevo horario
router.post('/',  horarioControllerSync.create);

// Actualizar horario
router.put('/:id',  horarioControllerSync.update);

// Eliminar horario
router.delete('/:id', horarioControllerSync.delete);


module.exports = router;