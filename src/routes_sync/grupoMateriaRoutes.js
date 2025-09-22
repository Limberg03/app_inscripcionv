const express = require('express');
const router = express.Router();
const grupoMateriaControllerSync = require('../controllers_sync/grupoMateriaControllerSync');

// Obtener todos los grupos de materia
router.get('/', grupoMateriaControllerSync.getAll);

// Obtener grupo de materia por ID
router.get('/:id',grupoMateriaControllerSync.getById);

// Crear nuevo grupo de materia
router.post('/',  grupoMateriaControllerSync.create);

// Actualizar grupo de materia
router.put('/:id',  grupoMateriaControllerSync.update);

// Actualización parcial
router.patch('/:id', grupoMateriaControllerSync.patch);

// Eliminar grupo de materia
router.delete('/:id',  grupoMateriaControllerSync.delete);

// Obtener grupos por materia
router.get('/materia/:materiaId',  grupoMateriaControllerSync.getByMateria);

// Obtener grupos por docente
router.get('/docente/:docenteId',  grupoMateriaControllerSync.getByDocente);

module.exports = router;