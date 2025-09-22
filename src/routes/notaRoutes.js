const express = require('express');
const router = express.Router();
const notaController = require('../controllers/notaController');

// Obtener todas las notas
router.get('/', notaController.getAll);

// Obtener nota por ID
router.get('/:id',  notaController.getById);

// Crear nueva nota
router.post('/', notaController.create);

// Actualizar nota
router.put('/:id',  notaController.update);

// Actualización parcial
router.patch('/:id',  notaController.patch);

// Eliminar nota
router.delete('/:id',  notaController.delete);

// Obtener notas por grupo de materia
router.get('/grupo-materia/:grupoMateriaId',  notaController.getByGrupoMateria);


module.exports = router;