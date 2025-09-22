const express = require('express');
const router = express.Router();
const horarioController = require('../controllers/horarioController');

// Obtener todos los horarios
router.get('/', horarioController.getAll);

// Obtener horario por ID
router.get('/:id',  horarioController.getById);

// Crear nuevo horario
router.post('/',  horarioController.create);

// Actualizar horario
router.put('/:id',  horarioController.update);

// Eliminar horario
router.delete('/:id', horarioController.delete);


module.exports = router;