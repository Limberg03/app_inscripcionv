
const { body, param, query } = require('express-validator');

const queueValidators = {
  enqueueTask: [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    body('type').isString().isLength({ min: 1 }).withMessage('Task type is required'),
    body('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('operation').isString().isIn(['create', 'update', 'delete', 'bulkcreate', 'bulkupdate', 'bulkdelete', 'custom'])
      .withMessage('Operation must be one of: create, update, delete, bulkcreate, bulkupdate, bulkdelete, custom'),
    body('data').exists().withMessage('Task data is required')
  ],

  saveRecord: [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('data').exists().withMessage('Record data is required')
  ],

  updateRecord: [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    param('id').isInt({ min: 1 }).withMessage('Valid ID is required'),
    body('data').exists().withMessage('Update data is required')
  ],

  bulkSave: [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('records').isArray({ min: 1 }).withMessage('Records array is required and must not be empty')
  ],

  createWorker: [
  param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
  body('threadCount').default(1).isInt({ min: 1, max: 10 }).withMessage('Thread count must be between 1 and 10')
],
  taskId: [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('taskId').isString().isLength({ min: 1 }).withMessage('Task ID is required')
  ],

  workerId: [
    param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')
  ],

  idParam: [
    param('id').isInt({ min: 1 }).withMessage('Valid ID is required')
  ]
};

module.exports = queueValidators;