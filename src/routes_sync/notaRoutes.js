const express = require('express');
const router = express.Router();
const notaControllerSync = require('../controllers_sync/notaControllerSync');

// Obtener todas las notas
router.get('/', notaControllerSync.getAll);

// Obtener nota por ID
router.get('/:id',  notaControllerSync.getById);

// Crear nueva nota
router.post('/', notaControllerSync.create);

// Actualizar nota
router.put('/:id',  notaControllerSync.update);

// Actualización parcial
router.patch('/:id',  notaControllerSync.patch);

// Eliminar nota
router.delete('/:id',  notaControllerSync.delete);

// Obtener notas por grupo de materia
router.get('/grupo-materia/:grupoMateriaId',  notaControllerSync.getByGrupoMateria);


module.exports = router;