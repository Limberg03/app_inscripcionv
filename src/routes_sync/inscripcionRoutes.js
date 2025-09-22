const express = require('express');
const router = express.Router();
const inscripcionControllerSync = require('../controllers_sync/inscripcionControllerSync');
const validators = require('../validators');

router.get('/', inscripcionControllerSync.getAll);
router.get('/:id', validators.common.idParam, inscripcionControllerSync.getById);
router.post('/', validators.inscripcion.create, inscripcionControllerSync.create);
router.put('/:id', validators.inscripcion.update, inscripcionControllerSync.update);
router.delete('/:id', validators.common.idParam, inscripcionControllerSync.delete);
router.get('/estudiante/:estudianteId', validators.common.idParam, inscripcionControllerSync.getByEstudiante);

module.exports = router;