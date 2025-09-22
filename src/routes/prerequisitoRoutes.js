const express = require('express');
const router = express.Router();
const prerequisitoController = require('../controllers/prerequisitoController');

// Obtener todos los prerequisitos
router.get('/', prerequisitoController.getAll);

// Obtener prerequisito por ID
router.get('/:id',  prerequisitoController.getById);

// Crear nuevo prerequisito
router.post('/', prerequisitoController.create);

// Actualizar prerequisito
router.put('/:id', prerequisitoController.update);

// Eliminar prerequisito
router.delete('/:id',  prerequisitoController.delete);

// Obtener prerequisitos por materia
router.get('/materia/:materiaId',  prerequisitoController.getByMateria);

module.exports = router;