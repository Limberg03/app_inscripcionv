const express = require('express');
const router = express.Router();
const prerequisitoControllerSync = require('../controllers_sync/prerequisitoControllerSync');

// Obtener todos los prerequisitos
router.get('/', prerequisitoControllerSync.getAll);

// Obtener prerequisito por ID
router.get('/:id',  prerequisitoControllerSync.getById);

// Crear nuevo prerequisito
router.post('/', prerequisitoControllerSync.create);

// Actualizar prerequisito
router.put('/:id', prerequisitoControllerSync.update);

// Eliminar prerequisito
router.delete('/:id',  prerequisitoControllerSync.delete);

// Obtener prerequisitos por materia
router.get('/materia/:materiaId',  prerequisitoControllerSync.getByMateria);

module.exports = router;