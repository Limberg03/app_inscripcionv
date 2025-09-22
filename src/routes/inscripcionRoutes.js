const express = require('express');
const router = express.Router();
const inscripcionController = require('../controllers/inscripcionController');
const validators = require('../validators');

router.get('/', inscripcionController.getAll);
router.get('/:id', validators.common.idParam, inscripcionController.getById);
router.post('/', validators.inscripcion.create, inscripcionController.create);
router.put('/:id', validators.inscripcion.update, inscripcionController.update);
router.delete('/:id', validators.common.idParam, inscripcionController.delete);
router.get('/estudiante/:estudianteId', validators.common.idParam, inscripcionController.getByEstudiante);

module.exports = router;