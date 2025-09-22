const express = require('express');
const router = express.Router();
const grupoMateriaController = require('../controllers/grupoMateriaController');

// Obtener todos los grupos de materia
router.get('/', grupoMateriaController.getAll);

// Obtener grupo de materia por ID
router.get('/:id',grupoMateriaController.getById);

// Crear nuevo grupo de materia
router.post('/',  grupoMateriaController.create);

// Actualizar grupo de materia
router.put('/:id',  grupoMateriaController.update);

// Actualización parcial
router.patch('/:id', grupoMateriaController.patch);

// Eliminar grupo de materia
router.delete('/:id',  grupoMateriaController.delete);

// Obtener grupos por materia
router.get('/materia/:materiaId',  grupoMateriaController.getByMateria);

// Obtener grupos por docente
router.get('/docente/:docenteId',  grupoMateriaController.getByDocente);

module.exports = router;