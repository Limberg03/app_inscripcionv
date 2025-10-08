const { QueueManager } = require('../queue/QueueManager');

class QueueService {
  constructor() {
    this.queueManager = new QueueManager({
      redisHost: process.env.REDIS_HOST || 'localhost',
      redisPort: process.env.REDIS_PORT || 6379,
      redisPassword: process.env.REDIS_PASSWORD,
      redisDb: process.env.REDIS_DB || 0,
      maxRetries: parseInt(process.env.QUEUE_MAX_RETRIES) || 3,
      retryDelay: parseInt(process.env.QUEUE_RETRY_DELAY) || 1000
    });
    this.initialized = false;
  }

  async initialize() {
    if (this.initialized) return;
    
    await this.queueManager.initialize();
    this.initialized = true;
    
    console.log('✅ QueueService with Redis initialized');
  }

  async enqueueTask(queueName, taskData) {
    if (!this.initialized) await this.initialize();
    
    this.validateTaskData(taskData);
    
    const queue = await this.queueManager.createQueue(queueName);
    const taskId = await queue.enqueue(taskData);
    
    return {
      success: true,
      taskId,
      queueName,
      message: 'Task enqueued successfully'
    };
  }

  async saveRecord(queueName, model, data, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'create',
      data,
      options
    });
  }

  async updateRecord(queueName, model, id, data, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'update',
      data: { id, updateData: data },
      options
    });
  }

  async deleteRecord(queueName, model, id, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'delete',
      data: { id },
      options
    });
  }

  async bulkSave(queueName, model, records, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'bulkcreate',
      data: { records },
      options
    });
  }

  async bulkUpdate(queueName, model, where, updateData, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'bulkupdate',
      data: { where, updateData },
      options
    });
  }

  async bulkDelete(queueName, model, where, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'bulkdelete',
      data: { where },
      options
    });
  }


 async findLeastLoadedQueue() {
  if (!this.initialized) await this.initialize();
  
  // Cargar colas desde Redis (esto ahora limpia huérfanas)
  await this.queueManager.loadPersistedQueues();
  
  const queues = Array.from(this.queueManager.queues.keys());
  
  if (queues.length === 0) {
    throw new Error('No queues available. Create at least one queue first.');
  }

  let leastLoadedQueue = null;
  let minLoad = Infinity;

  console.log(`🔍 Buscando cola con menos carga entre ${queues.length} colas...`);

  for (const queueName of queues) {
    try {
      const queue = this.queueManager.queues.get(queueName);
      if (!queue) {
        console.log(`⚠️ Cola '${queueName}' no encontrada, saltando...`);
        continue;
      }

      // Verificar que la cola existe en Redis
      const statsExist = await queue.redis.exists(queue.keys.stats);
      if (!statsExist) {
        console.log(`⚠️ Cola '${queueName}' no tiene stats en Redis, eliminando de memoria...`);
        this.queueManager.queues.delete(queueName);
        continue;
      }

      const pendingCount = await queue.redis.llen(queue.keys.pending);
      const processingCount = await queue.redis.llen(queue.keys.processing);

      const queueWorkers = Array.from(this.queueManager.workers.entries())
        .filter(([_, worker]) => worker.queue.name === queueName);
      
      const hasActiveWorkers = queueWorkers.some(([_, worker]) => 
        worker.isRunning && !worker.isPaused
      );

      let effectiveLoad;
      
      if (!hasActiveWorkers) {
        effectiveLoad = pendingCount + processingCount;
        console.log(`  📊 Cola '${queueName}' (SIN WORKERS ACTIVOS):`);
        console.log(`     └─ Pendientes: ${pendingCount} → Carga efectiva: ${effectiveLoad}`);
      } else {
        effectiveLoad = pendingCount + processingCount;
        console.log(`  📊 Cola '${queueName}' (CON WORKERS ACTIVOS):`);
        console.log(`     ├─ Pendientes: ${pendingCount}`);
        console.log(`     ├─ Procesando: ${processingCount}`);
        console.log(`     └─ Carga efectiva: ${effectiveLoad}`);
      }

      if (effectiveLoad < minLoad) {
        minLoad = effectiveLoad;
        leastLoadedQueue = queueName;
      }

    } catch (error) {
      console.error(`❌ Error verificando cola '${queueName}':`, error.message);
      continue;
    }
  }

  if (!leastLoadedQueue) {
    throw new Error('No se encontraron colas disponibles para balanceo');
  }

  console.log(`✅ Cola seleccionada: '${leastLoadedQueue}' con carga efectiva: ${minLoad}`);
  return leastLoadedQueue;
}
  // ✅ NUEVO: Enqueue con balanceo automático
  async enqueueTaskAutoBalance(taskData) {
    if (!this.initialized) await this.initialize();
    
    this.validateTaskData(taskData);
    
    const queueName = await this.findLeastLoadedQueue();
    const queue = await this.queueManager.getQueue(queueName);
    const taskId = await queue.enqueue(taskData);
    
    return {
      success: true,
      taskId,
      queueName,
      autoBalanced: true,
      message: `Task enqueued to least loaded queue: ${queueName}`
    };
  }

  // ✅ NUEVO: Save con balanceo automático
  async saveRecordAutoBalance(model, data, options = {}) {
    return await this.enqueueTaskAutoBalance({
      type: 'database',
      model,
      operation: 'create',
      data,
      options
    });
  }

  async updateRecordAutoBalance(model, id, data, options = {}) {
    return await this.enqueueTaskAutoBalance({
      type: 'database',
      model,
      operation: 'update',
      data: { id, updateData: data },
      options
    });
  }

  async deleteRecordAutoBalance(model, id, options = {}) {
    return await this.enqueueTaskAutoBalance({
      type: 'database',
      model,
      operation: 'delete',
      data: { id },
      options
    });
  }

  async customOperation(queueName, model, method, params, options = {}) {
    return await this.enqueueTask(queueName, {
      type: 'database',
      model,
      operation: 'custom',
      data: { method, params },
      options
    });
  }

  async createQueue(queueName, options = {}) {
    if (!this.initialized) await this.initialize();
    
    const queue = await this.queueManager.createQueue(queueName, options);
    return {
      success: true,
      queueName,
      message: 'Queue created successfully'
    };
  }

 // 7. ELIMINAR COLA (con limpieza completa de workers)
async deleteQueue(queueName) {
  if (!this.initialized) await this.initialize();
  
  console.log(`🔄 QueueService: Eliminando cola '${queueName}'...`);
  
  // Eliminar todos los workers de esta cola
  const workersResult = await this.deleteQueueWorkers(queueName, false);
  
  // Eliminar la cola (esto llamará a QueueManager.deleteQueue)
  await this.queueManager.deleteQueue(queueName);
  
  // VERIFICACIÓN: Asegurar que no existe en Redis
  const remainingKeys = await this.queueManager.redis.keys(`queue:${queueName}:*`);
  const workersInRedis = await this.queueManager.redis.hkeys(
    this.queueManager.workersConfigKey
  );
  const workersOfDeletedQueue = workersInRedis.filter(wId => wId.includes(queueName));
  
  if (remainingKeys.length > 0 || workersOfDeletedQueue.length > 0) {
    console.error(`❌ ERROR: Cola '${queueName}' no se eliminó completamente:`);
    console.error(`   - Keys en Redis: ${remainingKeys.length}`);
    console.error(`   - Workers en Redis: ${workersOfDeletedQueue.length}`);
    
    // Forzar limpieza
    if (remainingKeys.length > 0) {
      await this.queueManager.redis.del(...remainingKeys);
    }
    for (const wId of workersOfDeletedQueue) {
      await this.queueManager.deleteWorkerConfig(wId);
    }
    
    console.log(`✅ Limpieza forzada completada`);
  }
  
  console.log(`✅ Cola '${queueName}' eliminada completamente con ${workersResult.deletedCount} workers`);
  
  return {
    success: true,
    queueName,
    deletedWorkers: workersResult.deletedCount,
    cleanedKeys: remainingKeys.length,
    message: `Queue '${queueName}' deleted permanently with ${workersResult.deletedCount} workers`
  };
}

  
async deleteAllQueuesAndWorkers() {
  if (!this.initialized) await this.initialize();
  
  console.log('Limpiando sistema completo...');
  
  // Primero eliminar todos los workers
  const workersResult = await this.deleteAllWorkers(false);
  
  // Luego eliminar todas las colas
  const queueNames = Array.from(this.queueManager.queues.keys());
  const deletedQueues = [];
  
  for (const queueName of queueNames) {
    try {
      await this.queueManager.deleteQueue(queueName);
      deletedQueues.push(queueName);
      console.log(`  Cola '${queueName}' eliminada`);
    } catch (error) {
      console.error(`  Error eliminando cola '${queueName}':`, error.message);
    }
  }
  
  // Verificar limpieza en Redis
  const remainingKeys = await this.queueManager.redis.keys('queue:*');
  const remainingWorkers = await this.queueManager.redis.hkeys(
    this.queueManager.workersConfigKey
  );
  
  console.log(`Sistema limpiado: ${deletedQueues.length} colas, ${workersResult.deletedCount} workers`);
  console.log(`Residuos en Redis: ${remainingKeys.length} keys, ${remainingWorkers.length} workers`);
  
  return {
    success: true,
    deletedQueues: deletedQueues.length,
    deletedWorkers: workersResult.deletedCount,
    queues: deletedQueues,
    remainingInRedis: {
      keys: remainingKeys.length,
      workers: remainingWorkers.length
    },
    message: `System cleaned: ${deletedQueues.length} queues and ${workersResult.deletedCount} workers deleted`
  };
}

  // MODIFICAR ESTE MÉTODO (línea ~130)
async createWorker(queueName, threadCount = 1, options = {}) {
  if (!this.initialized) await this.initialize();
  
  // NUEVO: Soporte para batchSize
  const workerOptions = {
    ...options,
    batchSize: options.batchSize || 3 // Por defecto 3 tareas por lote
  };
  
  const { workerId, worker } = await this.queueManager.createWorker(queueName, threadCount, workerOptions);
 worker.batchSize = workerOptions.batchSize;

  return {
    success: true,
    workerId,
    queueName,
    threadCount,
    batchSize: workerOptions.batchSize, // Incluir en respuesta
    message: 'Worker created successfully',
    worker: worker
  };
}

async deleteWorker(workerId) {
  if (!this.initialized) await this.initialize();
  
  console.log(`🔍 [DEBUG] Iniciando eliminación de worker: ${workerId}`);
  
  const worker = this.queueManager.workers.get(workerId);
  if (!worker) {
    throw new Error(`Worker '${workerId}' not found`);
  }

  console.log(`🔍 [DEBUG] Worker encontrado en memoria: ${workerId}`);
  console.log(`🔍 [DEBUG] Estado del worker: running=${worker.isRunning}, paused=${worker.isPaused}`);
  
  // Detener worker
  if (worker.isRunning) {
    console.log(`🔍 [DEBUG] Deteniendo worker...`);
    await worker.stop(true);
    console.log(`🔍 [DEBUG] Worker detenido`);
  }
  
  // Eliminar de memoria
  console.log(`🔍 [DEBUG] Eliminando de memoria...`);
  this.queueManager.workers.delete(workerId);
  console.log(`🔍 [DEBUG] Workers en memoria después de eliminar: ${this.queueManager.workers.size}`);
  
  // Eliminar de Redis
  console.log(`🔍 [DEBUG] Eliminando de Redis...`);
  await this.queueManager.deleteWorkerConfig(workerId);
  
  // Verificar eliminación en Redis
  const checkRedis = await this.queueManager.redis.hget(
    this.queueManager.workersConfigKey,
    workerId
  );
  console.log(`🔍 [DEBUG] Worker en Redis después de eliminar: ${checkRedis === null ? 'ELIMINADO ✅' : 'AÚN EXISTE ❌'}`);
  
  // Contar workers en Redis
  const allWorkersInRedis = await this.queueManager.redis.hkeys(
    this.queueManager.workersConfigKey
  );
  console.log(`🔍 [DEBUG] Total workers en Redis: ${allWorkersInRedis.length}`);
  
  return {
    success: true,
    workerId,
    queueName: worker.queue.name,
    message: 'Worker deleted permanently from system'
  };
}


async deleteAllWorkers(graceful = true) {
  if (!this.initialized) await this.initialize();
  
  const workerIds = Array.from(this.queueManager.workers.keys());
  
  if (workerIds.length === 0) {
    return {
      success: true,
      deletedCount: 0,
      message: 'No workers to delete'
    };
  }

  console.log(`Eliminando ${workerIds.length} workers...`);
  
  const results = [];
  const errors = [];
  
  for (const workerId of workerIds) {
    try {
      const worker = this.queueManager.workers.get(workerId);
      
      // Detener worker si está corriendo
      if (worker && worker.isRunning) {
        await worker.stop(graceful);
      }
      
      // Eliminar de memoria
      this.queueManager.workers.delete(workerId);
      
      // Eliminar de Redis
      await this.queueManager.deleteWorkerConfig(workerId);
      
      results.push({
        workerId,
        queueName: worker?.queue?.name,
        deleted: true
      });
      
      console.log(`  Worker '${workerId}' eliminado`);
      
    } catch (error) {
      console.error(`  Error eliminando worker '${workerId}':`, error.message);
      errors.push({
        workerId,
        error: error.message
      });
    }
  }
  
  // Verificar que Redis quedó limpio
  const remainingInRedis = await this.queueManager.redis.hkeys(
    this.queueManager.workersConfigKey
  );
  
  console.log(`Todos los workers eliminados (${results.length} exitosos, ${errors.length} errores)`);
  console.log(`Workers restantes en Redis: ${remainingInRedis.length}`);
  
  return {
    success: true,
    deletedCount: results.length,
    failedCount: errors.length,
    deletedWorkers: results,
    errors: errors.length > 0 ? errors : undefined,
    remainingInRedis: remainingInRedis.length,
    message: `Deleted ${results.length} workers successfully`
  };
}


async deleteQueueWorkers(queueName, graceful = true) {
  if (!this.initialized) await this.initialize();
  
  const workersToDelete = [];
  
  for (const [workerId, worker] of this.queueManager.workers.entries()) {
    if (worker.queue.name === queueName) {
      workersToDelete.push(workerId);
    }
  }
  
  if (workersToDelete.length === 0) {
    return {
      success: true,
      queueName,
      deletedCount: 0,
      message: `No workers found for queue '${queueName}'`
    };
  }
  
  console.log(`Eliminando ${workersToDelete.length} workers de cola '${queueName}'...`);
  
  const results = [];
  const errors = [];
  
  for (const workerId of workersToDelete) {
    try {
      const worker = this.queueManager.workers.get(workerId);
      
      if (worker && worker.isRunning) {
        await worker.stop(graceful);
      }
      
      this.queueManager.workers.delete(workerId);
      await this.queueManager.deleteWorkerConfig(workerId);
      
      results.push(workerId);
      console.log(`  Worker '${workerId}' eliminado`);
      
    } catch (error) {
      console.error(`  Error eliminando worker '${workerId}':`, error.message);
      errors.push({ workerId, error: error.message });
    }
  }
  
  return {
    success: true,
    queueName,
    deletedCount: results.length,
    failedCount: errors.length,
    deletedWorkers: results,
    errors: errors.length > 0 ? errors : undefined,
    message: `Deleted ${results.length} workers from queue '${queueName}'`
  };
}






async deleteWorkerPermanently(workerId) {
    if (!this.initialized) await this.initialize();
    
    const worker = this.queueManager.workers.get(workerId);
    if (worker) {
      await worker.stop();
    }
    
    this.queueManager.workers.delete(workerId);
    await this.queueManager.deleteWorkerConfig(workerId);
    
    return {
      success: true,
      workerId,
      message: 'Worker permanently deleted from system'
    };
  }

 async updateWorkerConfig(workerId, updates) {
  if (!this.initialized) await this.initialize();
  
  const worker = this.queueManager.workers.get(workerId);
  if (!worker) {
    throw new Error(`Worker '${workerId}' not found`);
  }

  const currentConfig = await this.queueManager.redis.hget(
    this.queueManager.workersConfigKey,
    workerId
  );
  
  if (!currentConfig) {
    console.warn(`No config found for worker '${workerId}', creating new`);
  }
  
  const config = currentConfig ? JSON.parse(currentConfig) : {};
  
  const updatedConfig = {
    ...config,
    ...updates,
    updatedAt: new Date().toISOString()
  };
  
  await this.queueManager.saveWorkerConfig(workerId, updatedConfig);
  
  console.log(`Config de worker '${workerId}' actualizada:`, updates);
  
  return {
    success: true,
    workerId,
    updates,
    message: 'Worker configuration updated and persisted'
  };
}

  async getWorker(workerId) {
    if (!this.initialized) await this.initialize();
    
    const worker = this.queueManager.workers.get(workerId);
    return worker;
  }

  async pauseWorker(workerId) {
  if (!this.initialized) await this.initialize();
  
  const worker = this.queueManager.workers.get(workerId);
  if (!worker) {
    throw new Error(`Worker '${workerId}' not found`);
  }

  worker.pause();
  
  // ✅ ACTUALIZAR EN REDIS
  await this.updateWorkerConfig(workerId, { 
    isPaused: true,
    isRunning: worker.isRunning 
  });
  
  console.log(`⏸️ Worker '${workerId}' pausado y persistido`);
  
  return {
    success: true,
    workerId,
    message: 'Worker paused and persisted successfully'
  };
}

async resumeWorker(workerId) {
  if (!this.initialized) await this.initialize();
  
  const worker = this.queueManager.workers.get(workerId);
  if (!worker) {
    throw new Error(`Worker '${workerId}' not found`);
  }

  worker.resume();
  
  // ✅ ACTUALIZAR EN REDIS
  await this.updateWorkerConfig(workerId, { 
    isPaused: false,
    isRunning: worker.isRunning 
  });
  
  console.log(`▶️ Worker '${workerId}' reanudado y persistido`);
  
  return {
    success: true,
    workerId,
    message: 'Worker resumed and persisted successfully'
  };
}

 async startWorker(workerId) {
  if (!this.initialized) await this.initialize();
  
  const worker = this.queueManager.workers.get(workerId);
  if (!worker) {
    throw new Error(`Worker '${workerId}' not found`);
  }

  await worker.start();
  
  // ✅ ACTUALIZAR EN REDIS
  await this.updateWorkerConfig(workerId, { 
    isRunning: true,
    isPaused: false 
  });
  
  console.log(`▶️ Worker '${workerId}' iniciado y persistido`);
  
  return {
    success: true,
    workerId,
    message: 'Worker started and persisted successfully'
  };
}

  async getWorkerStats(workerId) {
    if (!this.initialized) await this.initialize();
    
    const worker = this.queueManager.workers.get(workerId);
    if (!worker) {
      throw new Error(`Worker '${workerId}' not found`);
    }

    return {
      success: true,
      stats: worker.getStats()
    };
  }

async getAllWorkers() {
  if (!this.initialized) await this.initialize();
  
  const workers = [];
  for (const [workerId, worker] of this.queueManager.workers.entries()) {
    const stats = worker.getStats();
    
    // Obtener config de Redis para verificar sincronización
    const configStr = await this.queueManager.redis.hget(
      this.queueManager.workersConfigKey,
      workerId
    );
    const config = configStr ? JSON.parse(configStr) : {};
    
    workers.push({
      id: workerId,
      batchSize: worker.batchSize || worker.options?.batchSize || 1,
      persistedState: { // Estado guardado en Redis
        isRunning: config.isRunning,
        isPaused: config.isPaused
      },
      ...stats
    });
  }

  return {
    success: true,
    workers
  };
}

  async pauseQueueWorkers(queueName) {
    if (!this.initialized) await this.initialize();
    
    const pausedWorkers = [];
    for (const [workerId, worker] of this.queueManager.workers.entries()) {
      if (worker.queue.name === queueName) {
        worker.pause();
        pausedWorkers.push(workerId);
      }
    }

    return {
      success: true,
      queueName,
      pausedWorkers,
      message: `Paused ${pausedWorkers.length} workers for queue '${queueName}'`
    };
  }

  async resumeQueueWorkers(queueName) {
    if (!this.initialized) await this.initialize();
    
    const resumedWorkers = [];
    for (const [workerId, worker] of this.queueManager.workers.entries()) {
      if (worker.queue.name === queueName) {
        worker.resume();
        resumedWorkers.push(workerId);
      }
    }

    return {
      success: true,
      queueName,
      resumedWorkers,
      message: `Resumed ${resumedWorkers.length} workers for queue '${queueName}'`
    };
  }

  async getTaskStatus(queueName, taskId) {
    if (!this.initialized) await this.initialize();
    
    const queue = await this.queueManager.getQueue(queueName);
    if (!queue) {
      throw new Error(`Queue '${queueName}' not found`);
    }

    const task = await queue.getTask(taskId);
    if (!task) {
      throw new Error(`Task '${taskId}' not found in queue '${queueName}'`);
    }

    return {
      success: true,
      task: {
        id: task.id,
        status: task.status,
        model: task.model,
        operation: task.operation,
        createdAt: task.createdAt,
        startedAt: task.startedAt,
        completedAt: task.completedAt,
        result: task.result,
        error: task.error,
        retryCount: task.retryCount
      }
    };
  }

  async getQueueStats(queueName) {
    if (!this.initialized) await this.initialize();
    
    const stats = await this.queueManager.getQueueStats(queueName);
    if (!stats) {
      throw new Error(`Queue '${queueName}' not found`);
    }

    return {
      success: true,
      stats
    };
  }

  async getAllQueuesStats() {
    if (!this.initialized) await this.initialize();
    
    const stats = await this.queueManager.getAllQueuesStats();
    return {
      success: true,
      stats
    };
  }

  async getQueueTasks(queueName, options = {}) {
    if (!this.initialized) await this.initialize();
    
    const queue = await this.queueManager.getQueue(queueName);
    if (!queue) {
      throw new Error(`Queue '${queueName}' not found`);
    }

    const { status, limit = 50, offset = 0 } = options;
    
    const result = await queue.getQueueTasks({ status, limit, offset });

    return {
      success: true,
      tasks: result.tasks,
      pagination: {
        limit,
        offset,
        returned: result.tasks.length,
        total: result.total
      }
    };
  }

  // Redis specific methods
  async clearQueue(queueName, status = null) {
    if (!this.initialized) await this.initialize();
    
    const queue = await this.queueManager.getQueue(queueName);
    if (!queue) {
      throw new Error(`Queue '${queueName}' not found`);
    }

    let clearedCount = 0;

    if (status) {
      // Clear specific status
      let key;
      switch(status) {
        case 'pending':
          key = queue.keys.pending;
          break;
        case 'processing':
          key = queue.keys.processing;
          break;
        case 'completed':
          key = queue.keys.completed;
          break;
        case 'failed':
        case 'error':
          key = queue.keys.failed;
          break;
        default:
          throw new Error(`Invalid status: ${status}`);
      }
      
      clearedCount = await queue.redis.llen(key);
      await queue.redis.del(key);
      
      // Update stats
      await queue.redis.hset(queue.keys.stats, status, 0);
      
    } else {
      // Clear all
      const allKeys = Object.values(queue.keys);
      const pipeline = queue.redis.pipeline();
      
      for (const key of allKeys) {
        if (key !== queue.keys.stats) {
          pipeline.del(key);
        }
      }
      
      // Reset stats
      pipeline.hmset(queue.keys.stats, {
        total: 0,
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0,
        error: 0
      });
      
      await pipeline.exec();
      clearedCount = 'all';
    }

    return {
      success: true,
      queueName,
      status: status || 'all',
      clearedCount,
      message: `Queue ${status || 'completely'} cleared successfully`
    };
  }

  async getRedisInfo() {
    if (!this.initialized) await this.initialize();
    
    const info = await this.queueManager.redis.info();
    const memory = await this.queueManager.redis.info('memory');
    
    return {
      success: true,
      redis: {
        status: 'connected',
        info: info.split('\n').reduce((acc, line) => {
          if (line.includes(':')) {
            const [key, value] = line.split(':');
            acc[key] = value;
          }
          return acc;
        }, {}),
        memory: memory.split('\n').reduce((acc, line) => {
          if (line.includes(':')) {
            const [key, value] = line.split(':');
            acc[key] = value;
          }
          return acc;
        }, {})
      }
    };
  }

  validateTaskData(taskData) {
    const { type, model, operation, data } = taskData;
    
    if (!type) throw new Error('Task type is required');
    if (!model) throw new Error('Model name is required');
    if (!operation) throw new Error('Operation is required');
    if (!data) throw new Error('Task data is required');

    const validOperations = ['create', 'update', 'delete', 'bulkcreate', 'bulkupdate', 'bulkdelete', 'custom', 'requestseat'];
    if (!validOperations.includes(operation.toLowerCase())) {
      throw new Error(`Invalid operation: ${operation}. Valid operations: ${validOperations.join(', ')}`);
    }
  }

  async shutdown() {
    if (!this.initialized) return;
    
    await this.queueManager.shutdown();
    this.initialized = false;
    console.log('✅ QueueService with Redis shutdown complete');
  }

  // Event listeners (Redis pub/sub could be implemented here)
  onTaskCompleted(queueName, callback) {
    // For Redis implementation, we'd typically use Redis pub/sub
    // For now, keeping the EventEmitter approach
    this.queueManager.on(`${queueName}:task:completed`, callback);
  }

  onTaskFailed(queueName, callback) {
    this.queueManager.on(`${queueName}:task:failed`, callback);
  }

  onTaskError(queueName, callback) {
    this.queueManager.on(`${queueName}:task:error`, callback);
  }
}

module.exports = QueueService;