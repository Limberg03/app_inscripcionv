const express = require('express');
const { body, param, query, validationResult } = require('express-validator');
const queueController = require('../controllers/queueController');
const router = express.Router();

// Import the updated QueueService (now uses Redis)
const QueueService = require('../services/QueueService');
const CallbackService = require('../services/CallbackService');

// Initialize services
const queueService = new QueueService();
const callbackService = new CallbackService();

// validacion de err, ej.: visualiza porque dio error
const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      success: false,
      message: 'Validation errors',
      errors: errors.array()
    });
  }
  next();
};

// Not crash
const ensureInitialized = async (req, res, next) => {  
  try {
    if (!queueService.initialized) {
      await queueService.initialize();
    }
    next();
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to initialize queue service',
      error: error.message
    });
  }
};

// ================ HEALTH AND INFO ENDPOINTS ================

// Queue system health check
router.get('/health', ensureInitialized, async (req, res) => {
  try {
    const [redisInfo, allStats] = await Promise.all([
      queueService.getRedisInfo(),
      queueService.getAllQueuesStats()
    ]);

    res.json({
      success: true,
      status: 'healthy',
      redis: redisInfo.redis,
      queues: allStats.stats,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Redis information
router.get('/redis/info', ensureInitialized, async (req, res) => {
  try {
    const result = await queueService.getRedisInfo();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to get Redis information',
      error: error.message
    });
  }
});

// Dashboard (could serve an HTML page or return JSON)
router.get('/dashboard', ensureInitialized, async (req, res) => {
  try {
    const [allStats, redisInfo, workers] = await Promise.all([
      queueService.getAllQueuesStats(),
      queueService.getRedisInfo(),
      queueService.getAllWorkers()
    ]);

    res.json({
      success: true,
      dashboard: {
        queues: allStats.stats,
        redis: redisInfo.redis,
        workers: workers.workers,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to load dashboard data',
      error: error.message
    });
  }
});

// ================ QUEUE MANAGEMENT ================

// Create queue
router.post('/:queueName', 
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.createQueue(queueName, req.body);
      res.status(201).json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to create queue',
        error: error.message
      });
    }
  }
);

// Delete queue
router.delete('/:queueName',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.deleteQueue(queueName);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to delete queue',
        error: error.message
      });
    }
  }
); 

// Get queue stats
router.get('/:queueName/stats',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.getQueueStats(queueName);
      res.json(result);
    } catch (error) {
      res.status(404).json({
        success: false,
        message: 'Queue not found or failed to get stats',
        error: error.message
      });
    }
  }
);

// Get all queues stats
router.get('/stats/all', ensureInitialized, async (req, res) => {
  try {
    const result = await queueService.getAllQueuesStats();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to get all queue stats',
      error: error.message
    });
  }
});

// Get queue tasks
router.get('/:queueName/tasks',
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    query('status').optional().isIn(['pending', 'processing', 'completed', 'failed', 'error']),
    query('limit').optional().isInt({ min: 1, max: 100 }).withMessage('Limit must be between 1 and 100'),
    query('offset').optional().isInt({ min: 0 }).withMessage('Offset must be non-negative')
  ],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const { status, limit, offset } = req.query;
      const result = await queueService.getQueueTasks(queueName, {
        status,
        limit: parseInt(limit) || 50,
        offset: parseInt(offset) || 0
      });
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to get queue tasks',
        error: error.message
      });
    }
  }
);

// Clear queue (Redis-specific feature)
router.delete('/:queueName/clear',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.clearQueue(queueName);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to clear queue',
        error: error.message
      });
    }
  }
);

router.delete('/:queueName/clear/:status',
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('status').isIn(['pending', 'processing', 'completed', 'failed', 'error'])
  ],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName, status } = req.params;
      const result = await queueService.clearQueue(queueName, status);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to clear queue',
        error: error.message
      });
    }
  }
);

// ================ WORKER MANAGEMENT ================

// Create worker
router.post('/:queueName/workers',
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    body('threadCount').default(1).isInt({ min: 1, max: 100 }).withMessage('Thread count must be between 1 and 100'),
    body('autoStart').default(false).isBoolean().withMessage('autoStart must be boolean'),
    
    body('batchSize').default(2).isInt({ min: 1, max: 200 }).withMessage('batchSize must be between 1 and 200') // NUEVO
  ],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const { threadCount, autoStart, batchSize } = req.body; // NUEVO: batchSize
      
      const result = await queueService.createWorker(queueName, threadCount, {
        autoStart,
        batchSize, // NUEVO
        onTaskCompleted: async (data) => {
          await callbackService.executeCallbacks('task:completed', data);
        },
        onTaskFailed: async (data) => {
          await callbackService.executeCallbacks('task:failed', data);
        },
        onTaskError: async (data) => {
          await callbackService.executeCallbacks('task:error', data);
        },
        onWorkerError: async (error, workerId) => {
          await callbackService.executeCallbacks('worker:error', { error, workerId });
        }
      });
      
      res.status(201).json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to create worker',
        error: error.message
      });
    }
  }
);

// Get all workers
router.get('/workers', ensureInitialized, async (req, res) => {
  try {
    const result = await queueService.getAllWorkers();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to get workers',
      error: error.message
    });
  }
});

// Start worker
router.post('/workers/:workerId/start',
  [param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { workerId } = req.params;
      const result = await queueService.startWorker(workerId);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to start worker',
        error: error.message
      });
    }
  }
);

// Pause worker
router.post('/workers/:workerId/pause',
  [param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { workerId } = req.params;
      const result = await queueService.pauseWorker(workerId);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to pause worker',
        error: error.message
      });
    }
  }
);

// Resume worker
router.post('/workers/:workerId/resume',
  [param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { workerId } = req.params;
      const result = await queueService.resumeWorker(workerId);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to resume worker',
        error: error.message
      });
    }
  }
);

// Stop worker
router.delete('/workers/:workerId',
  [param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { workerId } = req.params;
      const result = await queueService.deleteWorker(workerId); // ✅ CAMBIADO
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to delete worker', // ✅ CAMBIADO
        error: error.message
      });
    }
  }
);

// Get worker stats
router.get('/workers/:workerId/stats',
  [param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { workerId } = req.params;
      const result = await queueService.getWorkerStats(workerId);
      res.json(result);
    } catch (error) {
      res.status(404).json({
        success: false,
        message: 'Worker not found or failed to get stats',
        error: error.message
      });
    }
  }
);

// Pause all workers for a queue
router.post('/:queueName/workers/pause',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.pauseQueueWorkers(queueName);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to pause queue workers',
        error: error.message
      });
    }
  }
);

// Resume all workers for a queue
router.post('/:queueName/workers/resume',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.resumeQueueWorkers(queueName);
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to resume queue workers',
        error: error.message
      });
    }
  }
);

router.delete('/workers',
  ensureInitialized,
  async (req, res) => {
    try {
      const { graceful = 'true' } = req.query;
      const result = await queueService.deleteAllWorkers(graceful === 'true');
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to delete all workers',
        error: error.message
      });
    }
  }
);

// Delete workers of specific queue
router.delete('/:queueName/workers',
  [param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const { graceful = 'true' } = req.query;
      const result = await queueService.deleteQueueWorkers(queueName, graceful === 'true');
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to delete queue workers',
        error: error.message
      });
    }
  }
);

// Clean all system (queues + workers)
router.delete('/system/clean-all',
  ensureInitialized,
  async (req, res) => {
    try {
      const result = await queueService.deleteAllQueuesAndWorkers();
      res.json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to clean system',
        error: error.message
      });
    }
  }
);

// ================ TASK OPERATIONS ================

// Enqueue generic task
router.post('/:queueName/tasks',
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    body('type').isString().isLength({ min: 1 }).withMessage('Task type is required'),
    body('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('operation').isString().isIn(['create', 'update', 'delete', 'bulkcreate', 'bulkupdate', 'bulkdelete', 'custom'])
      .withMessage('Operation must be one of: create, update, delete, bulkcreate, bulkupdate, bulkdelete, custom'),
    body('data').exists().withMessage('Task data is required')
  ],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName } = req.params;
      const result = await queueService.enqueueTask(queueName, req.body);
      res.status(201).json(result);
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to enqueue task',
        error: error.message
      });
    }
  }
);



// RUTA UNIVERSAL PARA CUALQUIER MODELO

router.post('/:model/save', 
  [
    param('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('data').exists().withMessage('Data is required')
  ],
  queueController.universalSaveAutoBalance
);

// Update con auto-balance
router.put('/:model/:id', 
  [
    param('model').isString().isLength({ min: 1 }),
    param('id').isInt({ min: 1 }),
    body('data').exists()
  ],
  queueController.universalUpdateAutoBalance
);

// Delete con auto-balance
router.delete('/:model/:id',
  [
    param('model').isString().isLength({ min: 1 }),
    param('id').isInt({ min: 1 })
  ],
  queueController.universalDeleteAutoBalance
);

//-------------------

router.post('/:queueName/:model/save', 
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('model').isString().isLength({ min: 1 }).withMessage('Model name is required'),
    body('data').exists().withMessage('Data is required')
  ],
  queueController.universalSave
);

// RUTA UNIVERSAL PARA ACTUALIZAR
router.put('/:queueName/:model/:id', 
  [
    param('queueName').isString().isLength({ min: 1 }),
    param('model').isString().isLength({ min: 1 }),  
    param('id').isInt({ min: 1 }),
    body('data').exists()
  ],
  queueController.universalUpdate
);

// RUTA UNIVERSAL PARA ELIMINAR
router.delete('/:queueName/:model/:id',
  [
    param('queueName').isString().isLength({ min: 1 }),
    param('model').isString().isLength({ min: 1 }),
    param('id').isInt({ min: 1 })
  ],
  queueController.universalDelete
);

// RUTA UNIVERSAL PARA BULK OPERATIONS
router.post('/:queueName/:model/bulk',
  [
    param('queueName').isString().isLength({ min: 1 }),
    param('model').isString().isLength({ min: 1 }),
    body('operation').isIn(['create', 'update', 'delete']),
    body('data').exists()
  ],
  queueController.universalBulk
);

// Get task status
router.get('/:queueName/tasks/:taskId',
  [
    param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
    param('taskId').isString().isLength({ min: 1 }).withMessage('Task ID is required')
  ],
  handleValidationErrors,
  ensureInitialized,
  async (req, res) => {
    try {
      const { queueName, taskId } = req.params;
      const result = await queueService.getTaskStatus(queueName, taskId);
      res.json(result);
    } catch (error) {
      res.status(404).json({
        success: false,
        message: 'Task not found',
        error: error.message
      });
    }
  }
);

// ================ CALLBACK MANAGEMENT ================

// Register webhook callback
router.post('/callbacks/:eventType/:callbackId',
  [
    param('eventType').isIn(['task:completed', 'task:failed', 'task:error', 'worker:error']),
    param('callbackId').isString().isLength({ min: 1 }),
    body('webhookUrl').isURL().withMessage('Valid webhook URL is required'),
    body('method').default('POST').isIn(['POST', 'PUT', 'PATCH']),
    body('headers').optional().isObject()
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      const { webhookUrl, method, headers } = req.body;
      
      await callbackService.registerCallback(eventType, callbackId, {
        type: 'webhook',
        url: webhookUrl,
        method,
        headers
      });
      
      res.status(201).json({
        success: true,
        message: 'Webhook callback registered successfully',
        eventType,
        callbackId
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to register callback',
        error: error.message
      });
    }
  }
);

// Register custom callback
router.post('/callbacks/:eventType/:callbackId/custom',
  [
    param('eventType').isIn(['task:completed', 'task:failed', 'task:error', 'worker:error']),
    param('callbackId').isString().isLength({ min: 1 }),
    body('code').isString().isLength({ min: 1 }).withMessage('Callback code is required')
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      const { code } = req.body;
      
      await callbackService.registerCallback(eventType, callbackId, {
        type: 'custom',
        code
      });
      
      res.status(201).json({
        success: true,
        message: 'Custom callback registered successfully',
        eventType,
        callbackId
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to register custom callback',
        error: error.message
      });
    }
  }
);

// Remove callback
router.delete('/callbacks/:eventType/:callbackId',
  [
    param('eventType').isIn(['task:completed', 'task:failed', 'task:error', 'worker:error']),
    param('callbackId').isString().isLength({ min: 1 })
  ],
  handleValidationErrors,
  async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      await callbackService.removeCallback(eventType, callbackId);
      
      res.json({
        success: true,
        message: 'Callback removed successfully',
        eventType,
        callbackId
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Failed to remove callback',
        error: error.message
      });
    }
  }
);

// List callbacks
router.get('/callbacks', async (req, res) => {
  try {
    const callbacks = callbackService.listCallbacks();
    res.json({
      success: true,
      callbacks
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to list callbacks',
      error: error.message
    });
  }
});

// Get callback history
router.get('/callbacks/history', async (req, res) => {
  try {
    const history = callbackService.getExecutionHistory();
    res.json({
      success: true,
      history
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to get callback history',
      error: error.message
    });
  }
});

// Get callback stats
router.get('/callbacks/stats', async (req, res) => {
  try {
    const stats = callbackService.getStats();
    res.json({
      success: true,
      stats
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Failed to get callback stats',
      error: error.message
    });
  }
});

module.exports = router;