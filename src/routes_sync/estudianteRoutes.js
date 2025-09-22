const express = require('express');
const router = express.Router();
const estudianteControllerSync = require('../controllers_sync/estudianteControllerSync');
const validators = require('../validators');

router.get('/', estudianteControllerSync.getAll);
router.get('/:id', validators.common.idParam, estudianteControllerSync.getById);
router.post('/', validators.estudiante.create, estudianteControllerSync.create);
router.put('/:id', validators.estudiante.update, estudianteControllerSync.update);
router.delete('/:id', validators.common.idParam, estudianteControllerSync.delete);

module.exports = router;