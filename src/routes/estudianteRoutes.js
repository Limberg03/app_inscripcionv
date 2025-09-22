const express = require('express');
const router = express.Router();
const estudianteController = require('../controllers/estudianteController');
const validators = require('../validators');

router.get('/', estudianteController.getAll);
router.get('/:id', validators.common.idParam, estudianteController.getById);
router.post('/', validators.estudiante.create, estudianteController.create);
router.put('/:id', validators.estudiante.update, estudianteController.update);
router.delete('/:id', validators.common.idParam, estudianteController.delete);

module.exports = router;