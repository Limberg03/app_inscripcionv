const QueueService = require("../services/QueueService");
const { validationResult } = require("express-validator");

let queueService = null;

const getQueueService = () => {
  if (!queueService) {
    queueService = new QueueService();
  }
  return queueService;
};

const queueController = {
  enqueueTask: async (req, res) => {
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
      const taskData = req.body;

      const service = getQueueService();
      const result = await service.enqueueTask(queueName, taskData);

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in enqueueTask:", error);
      res.status(500).json({
        success: false,
        message: "Error enqueuing task",
        error: error.message,
      });
    }
  },

  universalSave: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { queueName, model } = req.params;
      const { data, options = {} } = req.body;

      console.log("🔄 Universal Save - Parámetros recibidos:");
      console.log("  queueName:", queueName);
      console.log("  model:", model);
      console.log("  data:", JSON.stringify(data, null, 2));

      const service = getQueueService();

      // Crear tarea con datos completos y estructura correcta
      const result = await service.enqueueTask(queueName, {
        type: "database",
        model: model,
        operation: "create",
        data: data,
        options: options,
      });

      console.log("✅ Tarea encolada exitosamente:", result.taskId);

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in universalSave:", error);
      res.status(500).json({
        success: false,
        message: "Error saving record",
        error: error.message,
      });
    }
  },

  // MÉTODO UNIVERSAL PARA ACTUALIZAR
  universalUpdate: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { queueName, model, id } = req.params;
      const { data, options = {} } = req.body;

      console.log("🔄 Universal Update - Parámetros recibidos:");
      console.log("  queueName:", queueName);
      console.log("  model:", model);
      console.log("  id:", id);
      console.log("  data:", JSON.stringify(data, null, 2));

      const service = getQueueService();

      const result = await service.enqueueTask(queueName, {
        type: "database",
        model: model,
        operation: "update",
        data: { id: parseInt(id), updateData: data },
        options: options,
      });

      console.log("✅ Tarea de actualización encolada:", result.taskId);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in universalUpdate:", error);
      res.status(500).json({
        success: false,
        message: "Error updating record",
        error: error.message,
      });
    }
  },

  // MÉTODO UNIVERSAL PARA ELIMINAR
  universalDelete: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { queueName, model, id } = req.params;
      const { options = {} } = req.body;

      console.log("🔄 Universal Delete - Parámetros recibidos:");
      console.log("  queueName:", queueName);
      console.log("  model:", model);
      console.log("  id:", id);

      const service = getQueueService();

      const result = await service.enqueueTask(queueName, {
        type: "database",
        model: model,
        operation: "delete",
        data: { id: parseInt(id) },
        options: options,
      });

      console.log("✅ Tarea de eliminación encolada:", result.taskId);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in universalDelete:", error);
      res.status(500).json({
        success: false,
        message: "Error deleting record",
        error: error.message,
      });
    }
  },

  // MÉTODO UNIVERSAL PARA OPERACIONES MASIVAS
  universalBulk: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { queueName, model } = req.params;
      const { operation, data, options = {} } = req.body;

      console.log("🔄 Universal Bulk - Parámetros recibidos:");
      console.log("  queueName:", queueName);
      console.log("  model:", model);
      console.log("  operation:", operation);
      console.log(
        "  data count:",
        Array.isArray(data) ? data.length : "not array"
      );

      const service = getQueueService();

      let taskData = {
        type: "database",
        model: model,
        operation: `bulk${operation}`,
        options: options,
      };

      // Preparar datos según la operación
      switch (operation) {
        case "create":
          taskData.data = { records: data };
          break;
        case "update":
          taskData.data = { where: data.where, updateData: data.updateData };
          break;
        case "delete":
          taskData.data = { where: data.where };
          break;
        default:
          return res.status(400).json({
            success: false,
            message: `Operación bulk '${operation}' no soportada`,
          });
      }

      const result = await service.enqueueTask(queueName, taskData);

      console.log("✅ Tarea bulk encolada:", result.taskId);

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in universalBulk:", error);
      res.status(500).json({
        success: false,
        message: "Error processing bulk operation",
        error: error.message,
      });
    }
  },

universalUpdateAutoBalance: async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: "Validation errors",
        errors: errors.array(),
      });
    }

    const { model, id } = req.params;
    const { data, options = {} } = req.body;

    console.log("🔄 Universal Update (Auto-Balance):");
    console.log("  model:", model);
    console.log("  id:", id);
    console.log("  data:", JSON.stringify(data, null, 2));

    const service = getQueueService();
    const result = await service.updateRecordAutoBalance(
      model, 
      parseInt(id), 
      data, 
      options
    );

    console.log(`✅ Update encolado con balanceo en: ${result.queueName}`);

    res.status(200).json(result);
  } catch (error) {
    console.error("Error in universalUpdateAutoBalance:", error);
    res.status(500).json({
      success: false,
      message: "Error updating record with auto-balance",
      error: error.message,
    });
  }
},

// ✅ DELETE con auto-balance
universalDeleteAutoBalance: async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: "Validation errors",
        errors: errors.array(),
      });
    }

    const { model, id } = req.params;
    const { options = {} } = req.body;

    console.log("🔄 Universal Delete (Auto-Balance):");
    console.log("  model:", model);
    console.log("  id:", id);

    const service = getQueueService();
    const result = await service.deleteRecordAutoBalance(
      model, 
      parseInt(id), 
      options
    );

    console.log(`✅ Delete encolado con balanceo en: ${result.queueName}`);

    res.status(200).json(result);
  } catch (error) {
    console.error("Error in universalDeleteAutoBalance:", error);
    res.status(500).json({
      success: false,
      message: "Error deleting record with auto-balance",
      error: error.message,
    });
  }
},


// ✅ NUEVO: Universal save CON balanceo automático
  universalSaveAutoBalance: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { model } = req.params;
      const { data, options = {} } = req.body;

      console.log("🔄 Universal Save (Auto-Balance) - Parámetros:");
      console.log("  model:", model);
      console.log("  data:", JSON.stringify(data, null, 2));

      const service = getQueueService();
      const result = await service.saveRecordAutoBalance(model, data, options);

      console.log(`✅ Tarea encolada con balanceo en: ${result.queueName}`);

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in universalSaveAutoBalance:", error);
      res.status(500).json({
        success: false,
        message: "Error saving record with auto-balance",
        error: error.message,
      });
    }
  },


  createQueue: async (req, res) => {
    try {
      const { queueName } = req.params;
      const options = req.body || {};

      const service = getQueueService();
      const result = await service.createQueue(queueName, options);

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in createQueue:", error);
      res.status(500).json({
        success: false,
        message: "Error creating queue",
        error: error.message,
      });
    }
  },

  deleteQueue: async (req, res) => {
    try {
      const { queueName } = req.params;

      const service = getQueueService();
      const result = await service.deleteQueue(queueName);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in deleteQueue:", error);
      res.status(500).json({
        success: false,
        message: "Error deleting queue",
        error: error.message,
      });
    }
  },

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
      const { threadCount = 1, options = {} } = req.body;

      console.log("Creating worker:", {
        queueName,
        threadCount,
        body: req.body,
      });

      const service = getQueueService();
      const result = await service.createWorker(
        queueName,
        threadCount,
        options
      );

      res.status(201).json(result);
    } catch (error) {
      console.error("Error in createWorker:", error);
      res.status(500).json({
        success: false,
        message: "Error creating worker",
        error: error.message,
      });
    }
  },

  stopWorker: async (req, res) => {
    try {
      const { workerId } = req.params;

      const service = getQueueService();
      const result = await service.stopWorker(workerId);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in stopWorker:", error);
      res.status(500).json({
        success: false,
        message: "Error stopping worker",
        error: error.message,
      });
    }
  },


  deleteWorkerPermanently: async (req, res) => {
    try {
      const { workerId } = req.params;
      
      const service = getQueueService();
      const result = await service.deleteWorkerPermanently(workerId);
      
      res.status(200).json(result);
    } catch (error) {
      console.error('Error deleting worker permanently:', error);
      res.status(500).json({
        success: false,
        message: 'Error deleting worker',
        error: error.message
      });
    }
  },

  getPersistedWorkers: async (req, res) => {
    try {
      const service = getQueueService();
      await service.initialize();
      
      const workersConfig = await service.queueManager.redis.hgetall(
        service.queueManager.workersConfigKey
      );
      
      const workers = Object.entries(workersConfig || {}).map(([id, config]) => ({
        id,
        ...JSON.parse(config)
      }));
      
      res.status(200).json({
        success: true,
        workers,
        total: workers.length
      });
    } catch (error) {
      console.error('Error getting persisted workers:', error);
      res.status(500).json({
        success: false,
        error: error.message
      });
    }
  },

  getTaskStatus: async (req, res) => {
    try {
      const { queueName, taskId } = req.params;

      const service = getQueueService();
      const result = await service.getTaskStatus(queueName, taskId);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in getTaskStatus:", error);
      if (error.message.includes("not found")) {
        return res.status(404).json({
          success: false,
          message: error.message,
        });
      }

      res.status(500).json({
        success: false,
        message: "Error getting task status",
        error: error.message,
      });
    }
  },

  // MÉTODO CORREGIDO: getQueueStats
  getQueueStats: async (req, res) => {
    try {
      const { queueName } = req.params;

      console.log(`Getting stats for queue: ${queueName}`);

      const service = getQueueService();

      // Verificar si la cola existe primero
      await service.initialize();
      const queue = await service.queueManager.getQueue(queueName);

      if (!queue) {
        console.log(`Queue '${queueName}' not found`);
        return res.status(404).json({
          success: false,
          message: `Queue '${queueName}' not found. Available queues: ${Array.from(
            service.queueManager.queues.keys()
          ).join(", ")}`,
        });
      }

      const result = await service.getQueueStats(queueName);
      console.log(`Stats retrieved for queue '${queueName}':`, result);

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in getQueueStats:", error);

      // Manejo específico de errores
      if (error.message.includes("not found")) {
        return res.status(404).json({
          success: false,
          message: error.message,
        });
      }

      res.status(500).json({
        success: false,
        message: "Error getting queue stats",
        error: error.message,
        stack: process.env.NODE_ENV === "development" ? error.stack : undefined,
      });
    }
  },

  getAllQueuesStats: async (req, res) => {
    try {
      const service = getQueueService();
      const result = await service.getAllQueuesStats();

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in getAllQueuesStats:", error);
      res.status(500).json({
        success: false,
        message: "Error getting all queues stats",
        error: error.message,
      });
    }
  },

  getQueueTasks: async (req, res) => {
    try {
      const { queueName } = req.params;
      const { status, limit = 50, offset = 0 } = req.query;

      console.log(`Getting tasks for queue: ${queueName}`);

      const service = getQueueService();

      // Verificar si la cola existe primero
      await service.initialize();
      const queue = await service.queueManager.getQueue(queueName);

      if (!queue) {
        console.log(`Queue '${queueName}' not found`);
        return res.status(404).json({
          success: false,
          message: `Queue '${queueName}' not found`,
        });
      }

      const result = await service.getQueueTasks(queueName, {
        status,
        limit: parseInt(limit),
        offset: parseInt(offset),
      });

      res.status(200).json(result);
    } catch (error) {
      console.error("Error in getQueueTasks:", error);
      if (error.message.includes("not found")) {
        return res.status(404).json({
          success: false,
          message: error.message,
        });
      }

      res.status(500).json({
        success: false,
        message: "Error getting queue tasks",
        error: error.message,
      });
    }
  },

  healthCheck: async (req, res) => {
    try {
      const service = getQueueService();
      const stats = await service.getAllQueuesStats();

      res.status(200).json({
        success: true,
        message: "Queue system is healthy",
        timestamp: new Date(),
        queues: Object.keys(stats.stats),
        totalQueues: Object.keys(stats.stats).length,
      });
    } catch (error) {
      console.error("Error in healthCheck:", error);
      res.status(500).json({
        success: false,
        message: "Queue system health check failed",
        error: error.message,
      });
    }
  },
  // 🔧 MÉTODO DE DEBUG - AGREGAR ESTE NUEVO MÉTODO
  debugQueueStats: async (req, res) => {
    const { queueName } = req.params;
    const debug = [];

    try {
      debug.push(`🔍 Starting debug for queue: ${queueName}`);

      const service = getQueueService();
      debug.push(`✅ QueueService obtained`);

      // Paso 1: Verificar inicialización
      debug.push(`🔄 Checking initialization...`);
      if (!service.initialized) {
        debug.push(`🔄 Initializing service...`);
        await service.initialize();
        debug.push(`✅ Service initialized`);
      } else {
        debug.push(`✅ Service already initialized`);
      }

      // Paso 2: Verificar QueueManager
      debug.push(`🔍 Checking QueueManager...`);
      if (!service.queueManager) {
        debug.push(`❌ No QueueManager found`);
        return res
          .status(500)
          .json({ success: false, debug, error: "No QueueManager" });
      }
      debug.push(`✅ QueueManager exists`);

      // Paso 3: Listar colas disponibles
      debug.push(`🔍 Getting available queues...`);
      const availableQueues = Array.from(service.queueManager.queues.keys());
      debug.push(`✅ Available queues: ${availableQueues.join(", ")}`);

      // Paso 4: Verificar si la cola específica existe
      debug.push(`🔍 Checking if queue '${queueName}' exists...`);
      const queue = await service.queueManager.getQueue(queueName);

      if (!queue) {
        debug.push(`❌ Queue '${queueName}' not found`);
        return res.status(404).json({
          success: false,
          debug,
          error: `Queue '${queueName}' not found`,
          availableQueues,
        });
      }
      debug.push(`✅ Queue '${queueName}' found`);

      // Paso 5: Verificar estructura de la cola
      debug.push(`🔍 Checking queue structure...`);
      debug.push(`  - Name: ${queue.name || "undefined"}`);
      debug.push(
        `  - Tasks array: ${
          queue.tasks ? `${queue.tasks.length} items` : "undefined"
        }`
      );
      debug.push(
        `  - TaskHistory: ${
          queue.taskHistory ? `${queue.taskHistory.size} items` : "undefined"
        }`
      );

      // Paso 6: Obtener estadísticas manualmente
      debug.push(`🔄 Getting stats manually...`);
      const manualStats = {
        name: queue.name,
        total: queue.taskHistory ? queue.taskHistory.size : 0,
        pending: queue.tasks ? queue.tasks.length : 0,
        processing: 0,
        completed: 0,
        failed: 0,
        error: 0,
      };

      // Paso 7: Procesar historial si existe
      if (queue.taskHistory && queue.taskHistory.size > 0) {
        debug.push(`🔄 Processing task history...`);
        let processed = 0;
        for (const [taskId, task] of queue.taskHistory.entries()) {
          if (task && task.status) {
            if (manualStats.hasOwnProperty(task.status)) {
              manualStats[task.status]++;
            }
          }
          processed++;
          if (processed > 100) break; // Limitar para evitar timeout
        }
        debug.push(`✅ Processed ${processed} tasks from history`);
      }

      debug.push(`✅ Manual stats calculated: ${JSON.stringify(manualStats)}`);

      // Paso 8: Intentar método original con timeout
      debug.push(`🔄 Trying original getStats method with timeout...`);

      const originalStatsPromise = queue.getStats();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("Timeout after 3 seconds")), 3000);
      });

      let originalStats = null;
      try {
        originalStats = await Promise.race([
          originalStatsPromise,
          timeoutPromise,
        ]);
        debug.push(
          `✅ Original method worked: ${JSON.stringify(originalStats)}`
        );
      } catch (error) {
        debug.push(`❌ Original method failed: ${error.message}`);
      }

      // Respuesta final
      res.status(200).json({
        success: true,
        queueName,
        debug,
        manualStats,
        originalStats,
        availableQueues,
      });
    } catch (error) {
      debug.push(`❌ Fatal error: ${error.message}`);
      debug.push(`❌ Stack: ${error.stack}`);

      res.status(500).json({
        success: false,
        queueName,
        debug,
        error: error.message,
        stack: error.stack,
      });
    }
  },

  debugLevel1: async (req, res) => {
    console.log("🔍 DEBUG LEVEL 1 - Starting...");

    try {
      console.log("🔍 Step 1: Creating response object...");
      const response = {
        success: true,
        message: "Debug Level 1 - Basic connectivity test",
        timestamp: new Date(),
        step: "Initial response created",
      };

      console.log("🔍 Step 2: Sending response...");
      res.status(200).json(response);
      console.log("✅ DEBUG LEVEL 1 - Completed successfully");
    } catch (error) {
      console.error("❌ DEBUG LEVEL 1 - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        step: "Failed in basic response",
      });
    }
  },

  // 🔧 DEBUG NIVEL 2: Probar obtener el servicio
  debugLevel2: async (req, res) => {
    console.log("🔍 DEBUG LEVEL 2 - Starting...");

    try {
      console.log("🔍 Step 1: Getting queue service...");
      const service = getQueueService();
      console.log("✅ Step 1: Service obtained:", !!service);

      console.log("🔍 Step 2: Checking service properties...");
      const serviceInfo = {
        hasQueueManager: !!service.queueManager,
        isInitialized: service.initialized,
        serviceType: typeof service,
      };
      console.log("✅ Step 2: Service info:", serviceInfo);

      res.status(200).json({
        success: true,
        message: "Debug Level 2 - Service access test",
        serviceInfo,
        timestamp: new Date(),
      });

      console.log("✅ DEBUG LEVEL 2 - Completed successfully");
    } catch (error) {
      console.error("❌ DEBUG LEVEL 2 - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        step: "Failed accessing service",
      });
    }
  },

  // 🔧 DEBUG NIVEL 3: Probar inicialización con timeout
  debugLevel3: async (req, res) => {
    console.log("🔍 DEBUG LEVEL 3 - Starting...");

    try {
      console.log("🔍 Step 1: Getting service...");
      const service = getQueueService();

      console.log("🔍 Step 2: Checking if already initialized...");
      if (service.initialized) {
        console.log("✅ Service already initialized");
        return res.status(200).json({
          success: true,
          message: "Service was already initialized",
          timestamp: new Date(),
        });
      }

      console.log("🔍 Step 3: Initializing with timeout...");

      const initPromise = service.initialize();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(
          () => reject(new Error("Initialization timeout after 5 seconds")),
          5000
        );
      });

      await Promise.race([initPromise, timeoutPromise]);

      console.log("✅ Step 3: Service initialized successfully");

      res.status(200).json({
        success: true,
        message: "Debug Level 3 - Initialization test completed",
        timestamp: new Date(),
      });

      console.log("✅ DEBUG LEVEL 3 - Completed successfully");
    } catch (error) {
      console.error("❌ DEBUG LEVEL 3 - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        step: "Failed during initialization",
      });
    }
  },

  // 🔧 DEBUG NIVEL 4: Probar acceso a queue manager
  debugLevel4: async (req, res) => {
    console.log("🔍 DEBUG LEVEL 4 - Starting...");

    try {
      const service = getQueueService();

      if (!service.initialized) {
        console.log("🔄 Initializing service...");
        await service.initialize();
      }

      console.log("🔍 Checking queue manager...");
      const queueManager = service.queueManager;

      if (!queueManager) {
        throw new Error("QueueManager is null or undefined");
      }

      console.log("🔍 Getting queue list...");
      const queueList = Array.from(queueManager.queues.keys());

      console.log("✅ Queue list obtained:", queueList);

      res.status(200).json({
        success: true,
        message: "Debug Level 4 - Queue Manager access test",
        queueList,
        queueCount: queueList.length,
        timestamp: new Date(),
      });
    } catch (error) {
      console.error("❌ DEBUG LEVEL 4 - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        step: "Failed accessing queue manager",
      });
    }
  },

  // 🔧 DEBUG NIVEL 5: Probar crear una cola simple
  debugLevel5: async (req, res) => {
    console.log("🔍 DEBUG LEVEL 5 - Starting...");
    const testQueueName = "debug_test_queue";

    try {
      const service = getQueueService();

      if (!service.initialized) {
        await service.initialize();
      }

      console.log("🔍 Creating test queue...");
      await service.createQueue(testQueueName);

      console.log("🔍 Verifying queue exists...");
      const queue = await service.queueManager.getQueue(testQueueName);

      if (!queue) {
        throw new Error("Queue was not created properly");
      }

      console.log("🔍 Getting basic queue info...");
      const queueInfo = {
        name: queue.name,
        tasksLength: queue.tasks ? queue.tasks.length : "undefined",
        taskHistorySize: queue.taskHistory
          ? queue.taskHistory.size
          : "undefined",
      };

      res.status(200).json({
        success: true,
        message: "Debug Level 5 - Queue creation test",
        testQueueName,
        queueInfo,
        timestamp: new Date(),
      });
    } catch (error) {
      console.error("❌ DEBUG LEVEL 5 - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        step: "Failed creating test queue",
      });
    }
  },
  debugSimpleStats: async (req, res) => {
    const { queueName } = req.params;
    console.log(`🔍 SIMPLE DEBUG - Queue: ${queueName}`);

    // Usar setTimeout para evitar que se cuelgue completamente
    const debugTimeout = setTimeout(() => {
      console.error("❌ SIMPLE DEBUG - Timeout reached!");
      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          error: "Debug timeout - process hung",
          queueName,
          step: "timeout",
        });
      }
    }, 10000); // 10 segundos timeout

    try {
      console.log("🔍 Step 1: Basic response test...");

      // Test 1: Respuesta básica
      if (queueName === "test-basic") {
        clearTimeout(debugTimeout);
        return res.status(200).json({
          success: true,
          message: "Basic response test works",
          queueName,
        });
      }

      console.log("🔍 Step 2: Getting service...");
      const service = getQueueService();
      console.log("✅ Step 2: Service obtained");

      console.log("🔍 Step 3: Checking initialization...");
      console.log("Service initialized?", service.initialized);

      if (!service.initialized) {
        console.log("🔄 Step 4: Initializing...");

        // Timeout específico para inicialización
        const initPromise = service.initialize();
        const initTimeout = new Promise((_, reject) => {
          setTimeout(() => reject(new Error("Init timeout")), 3000);
        });

        await Promise.race([initPromise, initTimeout]);
        console.log("✅ Step 4: Initialized");
      }

      console.log("🔍 Step 5: Getting available queues...");
      const availableQueues = Array.from(service.queueManager.queues.keys());
      console.log("✅ Step 5: Available queues:", availableQueues);

      clearTimeout(debugTimeout);

      res.status(200).json({
        success: true,
        message: "Simple debug completed",
        queueName,
        availableQueues,
        queueExists: availableQueues.includes(queueName),
        timestamp: new Date(),
      });
    } catch (error) {
      clearTimeout(debugTimeout);
      console.error("❌ SIMPLE DEBUG - Error:", error);

      if (!res.headersSent) {
        res.status(500).json({
          success: false,
          error: error.message,
          queueName,
          stack: error.stack,
        });
      }
    }
  },
  debugEmergency: async (req, res) => {
    console.log("🆘 EMERGENCY DEBUG - Starting...");

    try {
      console.log("🔍 Step 1: Getting service without initialization...");
      const service = getQueueService();

      console.log("🔍 Step 2: Checking persist directory...");
      const persistDir = service.queueManager.persistDir;
      console.log("Persist directory:", persistDir);

      console.log("🔍 Step 3: Checking if directory exists...");
      const fs = require("fs").promises;

      try {
        const stats = await fs.stat(persistDir);
        console.log("Directory exists:", stats.isDirectory());

        console.log("🔍 Step 4: Listing directory contents...");
        const files = await fs.readdir(persistDir);
        console.log("Files in directory:", files);

        // Verificar cada archivo .json
        const queueFiles = files.filter((f) => f.endsWith(".json"));
        const fileInfos = [];

        for (const file of queueFiles) {
          const filePath = require("path").join(persistDir, file);
          console.log(`🔍 Checking file: ${file}`);

          try {
            const fileStats = await fs.stat(filePath);
            const fileData = await fs.readFile(filePath, "utf8");

            fileInfos.push({
              name: file,
              size: fileStats.size,
              dataLength: fileData.length,
              canParse: true,
            });

            // Intentar parsear el JSON
            JSON.parse(fileData);
          } catch (parseError) {
            console.error(`❌ Error with file ${file}:`, parseError.message);
            fileInfos.push({
              name: file,
              error: parseError.message,
              canParse: false,
            });
          }
        }

        res.status(200).json({
          success: true,
          message: "Emergency debug completed",
          persistDir,
          directoryExists: true,
          filesFound: files,
          queueFiles,
          fileInfos,
          timestamp: new Date(),
        });
      } catch (dirError) {
        console.log("Directory does not exist:", dirError.message);

        res.status(200).json({
          success: true,
          message: "Emergency debug completed",
          persistDir,
          directoryExists: false,
          error: dirError.message,
          timestamp: new Date(),
        });
      }
    } catch (error) {
      console.error("❌ EMERGENCY DEBUG - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        stack: error.stack,
      });
    }
  },

  debugFileContent: async (req, res) => {
    console.log("🔍 FILE CONTENT DEBUG - Starting...");

    try {
      const service = getQueueService();
      const persistDir = service.queueManager.persistDir;
      const filePath = require("path").join(persistDir, "test_queue.json");

      console.log("🔍 Reading file content...");
      const fs = require("fs").promises;
      const fileContent = await fs.readFile(filePath, "utf8");

      console.log("🔍 Parsing JSON content...");
      const parsed = JSON.parse(fileContent);

      console.log("🔍 Analyzing structure...");
      const analysis = {
        hasName: !!parsed.name,
        tasksCount: parsed.tasks ? parsed.tasks.length : 0,
        taskHistoryCount: parsed.taskHistory ? parsed.taskHistory.length : 0,
        structure: {
          name: parsed.name,
          tasksIsArray: Array.isArray(parsed.tasks),
          taskHistoryIsArray: Array.isArray(parsed.taskHistory),
        },
      };

      // Analizar las tareas
      let taskAnalysis = [];
      if (parsed.tasks && parsed.tasks.length > 0) {
        taskAnalysis = parsed.tasks.slice(0, 3).map((task) => ({
          id: task.id,
          status: task.status,
          hasAllRequiredFields: !!(
            task.id &&
            task.type &&
            task.model &&
            task.operation
          ),
        }));
      }

      // Analizar el historial (primeros 3)
      let historyAnalysis = [];
      if (parsed.taskHistory && parsed.taskHistory.length > 0) {
        historyAnalysis = parsed.taskHistory.slice(0, 3).map(([id, task]) => ({
          id,
          taskStatus: task?.status,
          taskHasId: !!task?.id,
          hasAllRequiredFields: !!(
            task?.id &&
            task?.type &&
            task?.model &&
            task?.operation
          ),
        }));
      }

      res.status(200).json({
        success: true,
        message: "File content analysis completed",
        filePath,
        fileSize: fileContent.length,
        analysis,
        sampleTasks: taskAnalysis,
        sampleHistory: historyAnalysis,
        // Solo mostrar el contenido completo si es pequeño
        rawContent: fileContent.length < 2000 ? parsed : "Too large to display",
        timestamp: new Date(),
      });
    } catch (error) {
      console.error("❌ FILE CONTENT DEBUG - Error:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        stack: error.stack,
      });
    }
  },
  debugInitStepByStep: async (req, res) => {
    console.log("🔍 INIT STEP BY STEP - Starting...");
    const logs = [];

    const addLog = (message) => {
      console.log(message);
      logs.push(`${new Date().toISOString()}: ${message}`);
    };

    try {
      addLog("Step 1: Getting service...");
      const service = getQueueService();

      addLog("Step 2: Getting QueueManager...");
      const queueManager = service.queueManager;

      addLog("Step 3: Checking if already initialized...");
      if (queueManager.initialized) {
        return res.status(200).json({
          success: true,
          message: "Already initialized",
          logs,
        });
      }

      addLog("Step 4: Creating persist directory manually...");
      const fs = require("fs").promises;
      await fs.mkdir(queueManager.persistDir, { recursive: true });

      addLog("Step 5: Reading directory contents...");
      const files = await fs.readdir(queueManager.persistDir);
      addLog(`Found files: ${files.join(", ")}`);

      addLog("Step 6: Filtering queue files...");
      const queueFiles = files.filter((file) => file.endsWith(".json"));
      addLog(`Queue files: ${queueFiles.join(", ")}`);

      addLog("Step 7: Processing each queue file...");

      for (let i = 0; i < queueFiles.length; i++) {
        const file = queueFiles[i];
        const queueName = file.replace(".json", "");

        addLog(`Step 7.${i + 1}: Processing queue '${queueName}'...`);

        // Verificar si la cola ya existe en memoria
        if (queueManager.queues.has(queueName)) {
          addLog(`  Queue '${queueName}' already exists in memory`);
          continue;
        }

        addLog(`  Creating queue object for '${queueName}'...`);

        // Crear queue manualmente SIN llamar a createQueue para evitar recursión
        const { Queue } = require("../queue/QueueManager");
        const queue = new Queue(queueName, {
          persistDir: queueManager.persistDir,
          maxRetries: queueManager.maxRetries,
          retryDelay: queueManager.retryDelay,
        });

        addLog(`  Initializing queue '${queueName}'...`);

        // Timeout para la inicialización de cada cola
        const initPromise = queue.initialize();
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(
            () => reject(new Error(`Queue ${queueName} init timeout`)),
            3000
          );
        });

        await Promise.race([initPromise, timeoutPromise]);

        addLog(`  Adding queue '${queueName}' to manager...`);
        queueManager.queues.set(queueName, queue);

        addLog(`  ✅ Queue '${queueName}' loaded successfully`);
      }

      addLog("Step 8: Setting initialized flag...");
      queueManager.initialized = true;

      addLog("✅ Manual initialization completed successfully!");

      res.status(200).json({
        success: true,
        message: "Manual initialization completed",
        logs,
        loadedQueues: Array.from(queueManager.queues.keys()),
        timestamp: new Date(),
      });
    } catch (error) {
      addLog(`❌ Error: ${error.message}`);
      console.error("❌ INIT STEP BY STEP - Error:", error);

      res.status(500).json({
        success: false,
        error: error.message,
        logs,
        stack: error.stack,
      });
    }
  },
  testFixedInit: async (req, res) => {
    console.log("🧪 TESTING FIXED INITIALIZATION");

    try {
      // Reiniciar el servicio para probar desde cero
      console.log("🔄 Getting fresh service...");

      // Forzar nueva instancia (opcional, para test limpio)
      const QueueService = require("../services/QueueService");
      const testService = new QueueService();

      console.log("🔄 Testing normal initialization...");

      // Probar inicialización con timeout
      const initPromise = testService.initialize();
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(
          () => reject(new Error("Init timeout after 10 seconds")),
          10000
        );
      });

      await Promise.race([initPromise, timeoutPromise]);
      console.log("✅ Initialization completed successfully!");

      // Probar obtener estadísticas
      console.log("🔄 Testing getQueueStats...");
      const stats = await testService.getQueueStats("test_queue");
      console.log("✅ Stats obtained:", stats);

      res.status(200).json({
        success: true,
        message: "Fixed initialization test completed successfully!",
        stats,
        timestamp: new Date(),
      });
    } catch (error) {
      console.error("❌ TEST FAILED:", error);
      res.status(500).json({
        success: false,
        error: error.message,
        message: "Fixed initialization test failed",
        stack: error.stack,
      });
    }
  },
};

// Manejo de cierre graceful
process.on("SIGTERM", async () => {
  if (queueService) {
    console.log("🔄 Shutting down queue service...");
    try {
      await queueService.shutdown();
    } catch (error) {
      console.error("Error during graceful shutdown:", error);
    }
  }
});

process.on("SIGINT", async () => {
  if (queueService) {
    console.log("🔄 Shutting down queue service...");
    try {
      await queueService.shutdown();
    } catch (error) {
      console.error("Error during graceful shutdown:", error);
    }
  }
});

module.exports = queueController;
