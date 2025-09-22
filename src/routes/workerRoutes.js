// // routes/workerRoutes.js
// const express = require('express');
// const router = express.Router();
// const workerController = require('../controllers/workerController');
// const { body, param } = require('express-validator');

// // Validadores
// const workerValidators = {
//   createWorker: [
//     param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required'),
//     body('threadCount').optional().isInt({ min: 1, max: 10 }).withMessage('Thread count must be between 1 and 10'),
//     body('autoStart').optional().isBoolean().withMessage('autoStart must be boolean'),
//     body('options').optional().isObject().withMessage('options must be an object')
//   ],

//   workerId: [
//     param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required')
//   ],

//   queueName: [
//     param('queueName').isString().isLength({ min: 1 }).withMessage('Queue name is required')
//   ],

//   pauseWorker: [
//     param('workerId').isString().isLength({ min: 1 }).withMessage('Worker ID is required'),
//     body('graceful').optional().isBoolean().withMessage('graceful must be boolean')
//   ]
// };

// // ======= RUTAS PARA WORKERS INDIVIDUALES =======

// // Crear worker (con control de autoStart)
// router.post('/:queueName/workers', 
//   workerValidators.createWorker,
//   workerController.createWorker
// );

// // Iniciar worker específico
// router.post('/workers/:workerId/start', 
//   workerValidators.workerId,
//   workerController.startWorker
// );

// // Pausar/Detener worker específico
// router.post('/workers/:workerId/pause', 
//   workerValidators.pauseWorker,
//   workerController.pauseWorker
// );

// // Reanudar worker específico
// router.post('/workers/:workerId/resume', 
//   workerValidators.workerId,
//   workerController.resumeWorker
// );

// // Obtener estado de worker específico
// router.get('/workers/:workerId/status', 
//   workerValidators.workerId,
//   workerController.getWorkerStatus
// );

// // Eliminar worker (método original)
// router.delete('/workers/:workerId', 
//   workerValidators.workerId,
//   workerController.stopWorker
// );

// // ======= RUTAS PARA CONTROL DE COLA COMPLETA =======

// // Pausar todos los workers de una cola
// router.post('/:queueName/workers/pause', 
//   workerValidators.queueName,
//   workerController.pauseQueueWorkers
// );

// // Reanudar todos los workers de una cola
// router.post('/:queueName/workers/resume', 
//   workerValidators.queueName,
//   workerController.resumeQueueWorkers
// );

// // Obtener estado de todos los workers
// router.get('/workers/status', workerController.getWorkerStatus);

// // ======= RUTAS DE INFORMACIÓN =======

// // Health check específico para workers
// router.get('/workers/health', async (req, res) => {
//   try {
//     const QueueService = require('../services/QueueService');
//     const queueService = new QueueService();
//     await queueService.initialize();
    
//     const workers = [];
//     for (const [id, worker] of queueService.queueManager.workers.entries()) {
//       workers.push(worker.getStats());
//     }
    
//     const totalWorkers = workers.length;
//     const runningWorkers = workers.filter(w => w.isRunning).length;
//     const pausedWorkers = workers.filter(w => w.isPaused).length;
    
//     res.status(200).json({
//       success: true,
//       message: 'Workers system is healthy',
//       summary: {
//         total: totalWorkers,
//         running: runningWorkers,
//         paused: pausedWorkers,
//         stopped: totalWorkers - runningWorkers
//       },
//       workers: workers.slice(0, 10), // Solo primeros 10 para evitar response muy grande
//       timestamp: new Date()
//     });
//   } catch (error) {
//     res.status(500).json({
//       success: false,
//       message: 'Workers health check failed',
//       error: error.message
//     });
//   }
// });

// module.exports = router;