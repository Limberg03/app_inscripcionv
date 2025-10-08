const express = require('express');
const router = express.Router();
const inscripcionController = require('../controllers/inscripcionController');
const validators = require('../validators');
const { body } = require('express-validator'); 

router.get('/', inscripcionController.getAll);
router.get('/:id', validators.common.idParam, inscripcionController.getById);
router.post('/', validators.inscripcion.create, inscripcionController.create);
router.put('/:id', validators.inscripcion.update, inscripcionController.update);
router.delete('/:id', validators.common.idParam, inscripcionController.delete);
router.get('/estudiante/:estudianteId', validators.common.idParam, inscripcionController.getByEstudiante);

router.post('/request-seat',
  [
    body('estudianteId').isInt({ min: 1 }).withMessage('Valid estudianteId required'),
    body('grupoMateriaId').isInt({ min: 1 }).withMessage('Valid grupoMateriaId required'),
    body('gestion').optional().isInt({ min: 2000, max: 2100 })
  ],
  inscripcionController.requestSeat
  
);

// router.get('/request-seat',
// [

// ],

// );



module.exports = router;