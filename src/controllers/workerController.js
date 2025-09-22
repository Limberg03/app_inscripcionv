// controllers/workerController.js
const QueueService = require("../services/QueueService");
const { validationResult } = require("express-validator");

let queueService = null;

const getQueueService = () => {
  if (!queueService) {
    queueService = new QueueService();
  }
  return queueService;
};

const workerController = {
  // ✅ 1. CREAR WORKER CON CONTROL COMPLETO
  createWorker: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { queueName } = req.params;
      const { threadCount = 1, autoStart = true, options = {} } = req.body;

      const service = getQueueService();
      
      // Crear el worker pero NO iniciarlo automáticamente si autoStart = false
      const result = await service.createWorker(queueName, threadCount, {
        ...options,
        autoStart
      });

      console.log(`✅ Worker creado: ${result.workerId}, autoStart: ${autoStart}`);

      res.status(201).json({
        ...result,
        autoStart,
        status: autoStart ? 'running' : 'stopped'
      });
    } catch (error) {
      console.error("Error in createWorker:", error);
      res.status(500).json({
        success: false,
        message: "Error creating worker",
        error: error.message,
      });
    }
  },

  // ✅ 2. INICIAR WORKER ESPECÍFICO
  startWorker: async (req, res) => {
    try {
      const { workerId } = req.params;

      const service = getQueueService();
      await service.initialize();

      const worker = service.queueManager.workers.get(workerId);
      if (!worker) {
        return res.status(404).json({
          success: false,
          message: `Worker '${workerId}' not found`
        });
      }

      if (worker.isRunning) {
        return res.status(400).json({
          success: false,
          message: `Worker '${workerId}' is already running`
        });
      }

      await worker.start();

      console.log(`✅ Worker ${workerId} iniciado`);

      res.status(200).json({
        success: true,
        workerId,
        message: 'Worker started successfully',
        status: 'running',
        threadCount: worker.threadCount
      });
    } catch (error) {
      console.error("Error starting worker:", error);
      res.status(500).json({
        success: false,
        message: "Error starting worker",
        error: error.message,
      });
    }
  },

  // ✅ 3. PAUSAR/DETENER WORKER ESPECÍFICO
  pauseWorker: async (req, res) => {
    try {
      const { workerId } = req.params;
      const { graceful = true } = req.body;

      const service = getQueueService();
      await service.initialize();

      const worker = service.queueManager.workers.get(workerId);
      if (!worker) {
        return res.status(404).json({
          success: false,
          message: `Worker '${workerId}' not found`
        });
      }

      if (!worker.isRunning) {
        return res.status(400).json({
          success: false,
          message: `Worker '${workerId}' is already stopped`
        });
      }

      // Pausar el worker
      if (graceful) {
        worker.isPaused = true;
        console.log(`⏸️ Worker ${workerId} pausado (graceful)`);
      } else {
        await worker.stop();
        console.log(`⏹️ Worker ${workerId} detenido (force stop)`);
      }

      res.status(200).json({
        success: true,
        workerId,
        message: graceful ? 'Worker paused successfully' : 'Worker stopped successfully',
        status: graceful ? 'paused' : 'stopped',
        graceful
      });
    } catch (error) {
      console.error("Error pausing worker:", error);
      res.status(500).json({
        success: false,
        message: "Error pausing worker",
        error: error.message,
      });
    }
  },

  // ✅ 4. REANUDAR WORKER PAUSADO
  resumeWorker: async (req, res) => {
    try {
      const { workerId } = req.params;

      const service = getQueueService();
      await service.initialize();

      const worker = service.queueManager.workers.get(workerId);
      if (!worker) {
        return res.status(404).json({
          success: false,
          message: `Worker '${workerId}' not found`
        });
      }

      if (!worker.isPaused) {
        return res.status(400).json({
          success: false,
          message: `Worker '${workerId}' is not paused`
        });
      }

      worker.isPaused = false;
      console.log(`▶️ Worker ${workerId} reanudado`);

      res.status(200).json({
        success: true,
        workerId,
        message: 'Worker resumed successfully',
        status: 'running'
      });
    } catch (error) {
      console.error("Error resuming worker:", error);
      res.status(500).json({
        success: false,
        message: "Error resuming worker",
        error: error.message,
      });
    }
  },

  // ✅ 5. OBTENER ESTADO DE WORKERS
  getWorkerStatus: async (req, res) => {
    try {
      const { workerId } = req.params;

      const service = getQueueService();
      await service.initialize();

      if (workerId) {
        // Estado de un worker específico
        const worker = service.queueManager.workers.get(workerId);
        if (!worker) {
          return res.status(404).json({
            success: false,
            message: `Worker '${workerId}' not found`
          });
        }

        const status = {
          id: worker.id,
          queueName: worker.queue.name,
          isRunning: worker.isRunning,
          isPaused: worker.isPaused || false,
          threadCount: worker.threadCount,
          processingTasks: worker.processingTasks.size,
          status: worker.isPaused ? 'paused' : (worker.isRunning ? 'running' : 'stopped')
        };

        res.status(200).json({
          success: true,
          worker: status
        });
      } else {
        // Estado de todos los workers
        const workers = [];
        for (const [id, worker] of service.queueManager.workers.entries()) {
          workers.push({
            id: worker.id,
            queueName: worker.queue.name,
            isRunning: worker.isRunning,
            isPaused: worker.isPaused || false,
            threadCount: worker.threadCount,
            processingTasks: worker.processingTasks.size,
            status: worker.isPaused ? 'paused' : (worker.isRunning ? 'running' : 'stopped')
          });
        }

        res.status(200).json({
          success: true,
          workers,
          totalWorkers: workers.length
        });
      }
    } catch (error) {
      console.error("Error getting worker status:", error);
      res.status(500).json({
        success: false,
        message: "Error getting worker status",
        error: error.message,
      });
    }
  },

  // ✅ 6. PAUSAR TODOS LOS WORKERS DE UNA COLA
  pauseQueueWorkers: async (req, res) => {
    try {
      const { queueName } = req.params;
      const { graceful = true } = req.body;

      const service = getQueueService();
      await service.initialize();

      const workersForQueue = Array.from(service.queueManager.workers.entries())
        .filter(([_, worker]) => worker.queue.name === queueName);

      if (workersForQueue.length === 0) {
        return res.status(404).json({
          success: false,
          message: `No workers found for queue '${queueName}'`
        });
      }

      const results = [];

      for (const [workerId, worker] of workersForQueue) {
        if (worker.isRunning) {
          if (graceful) {
            worker.isPaused = true;
          } else {
            await worker.stop();
          }
          results.push({
            workerId,
            action: graceful ? 'paused' : 'stopped'
          });
        }
      }

      console.log(`⏸️ ${results.length} workers ${graceful ? 'pausados' : 'detenidos'} para cola '${queueName}'`);

      res.status(200).json({
        success: true,
        queueName,
        message: `Workers ${graceful ? 'paused' : 'stopped'} successfully`,
        workers: results,
        totalAffected: results.length
      });
    } catch (error) {
      console.error("Error pausing queue workers:", error);
      res.status(500).json({
        success: false,
        message: "Error pausing queue workers",
        error: error.message,
      });
    }
  },

  // ✅ 7. REANUDAR TODOS LOS WORKERS DE UNA COLA
  resumeQueueWorkers: async (req, res) => {
    try {
      const { queueName } = req.params;

      const service = getQueueService();
      await service.initialize();

      const workersForQueue = Array.from(service.queueManager.workers.entries())
        .filter(([_, worker]) => worker.queue.name === queueName);

      if (workersForQueue.length === 0) {
        return res.status(404).json({
          success: false,
          message: `No workers found for queue '${queueName}'`
        });
      }

      const results = [];

      for (const [workerId, worker] of workersForQueue) {
        if (worker.isPaused) {
          worker.isPaused = false;
          results.push({
            workerId,
            action: 'resumed'
          });
        } else if (!worker.isRunning) {
          await worker.start();
          results.push({
            workerId,
            action: 'started'
          });
        }
      }

      console.log(`▶️ ${results.length} workers reanudados para cola '${queueName}'`);

      res.status(200).json({
        success: true,
        queueName,
        message: 'Workers resumed successfully',
        workers: results,
        totalAffected: results.length
      });
    } catch (error) {
      console.error("Error resuming queue workers:", error);
      res.status(500).json({
        success: false,
        message: "Error resuming queue workers",
        error: error.message,
      });
    }
  },
  stopWorker: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { workerId } = req.params;

      const service = getQueueService();
      await service.initialize();

      const worker = service.queueManager.workers.get(workerId);
      if (!worker) {
        return res.status(404).json({
          success: false,
          message: `Worker '${workerId}' not found`
        });
      }

      // Detener el worker completamente
      if (worker.isRunning) {
        await worker.stop();
      }

      // Remover el worker del manager
      service.queueManager.workers.delete(workerId);

      console.log(`🗑️ Worker ${workerId} detenido y eliminado`);

      res.status(200).json({
        success: true,
        workerId,
        message: 'Worker stopped and removed successfully',
        status: 'deleted'
      });
    } catch (error) {
      console.error("Error stopping worker:", error);
      res.status(500).json({
        success: false,
        message: "Error stopping worker",
        error: error.message,
      });
    }
  }
};

module.exports = workerController;