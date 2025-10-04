const Redis = require('ioredis');
const EventEmitter = require('events');
const { Worker } = require('worker_threads');
const path = require('path');

class Task {
  constructor(data) {
    this.id = data.id;
    this.type = data.type;
    this.model = data.model;
    this.operation = data.operation;
    this.data = data.data;
    this.options = data.options || {};
    this.queueName = data.queueName;
    this.status = data.status || "pending";
     this.threadId = data.threadId;
    this.createdAt = data.createdAt || new Date();
    this.startedAt = data.startedAt;
    this.completedAt = data.completedAt;
    this.result = data.result;
    this.error = data.error;
    this.retryCount = data.retryCount || 0;
  }

  serialize() {
    return JSON.stringify({
      id: this.id,
      type: this.type,
      model: this.model,
      operation: this.operation,
      data: this.data,
      options: this.options,
      queueName: this.queueName,
      status: this.status,
      threadId: this.threadId, 
      createdAt: this.createdAt?.toISOString(),
      startedAt: this.startedAt?.toISOString(),
      completedAt: this.completedAt?.toISOString(),
      result: this.result,
      error: this.error,
      retryCount: this.retryCount,
    });
  }

  static deserialize(data) {
    const parsed = typeof data === 'string' ? JSON.parse(data) : data;
    return new Task({
      ...parsed,
      createdAt: parsed.createdAt ? new Date(parsed.createdAt) : new Date(),
      startedAt: parsed.startedAt ? new Date(parsed.startedAt) : null,
      completedAt: parsed.completedAt ? new Date(parsed.completedAt) : null,
    });
  }
}

class RedisQueue extends EventEmitter {
  constructor(name, options = {}) {
    super();
    this.name = name;
    this.redis = options.redis;
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000;
    
    // Redis keys
    this.keys = {
      pending: `queue:${name}:pending`,
      processing: `queue:${name}:processing`,
      completed: `queue:${name}:completed`,
      failed: `queue:${name}:failed`,
      tasks: `queue:${name}:tasks`,
      stats: `queue:${name}:stats`
    };
  }

  async initialize() {
    
    try {
      const statsExist = await this.redis.exists(this.keys.stats);
      if (!statsExist) {
        await this.redis.hmset(this.keys.stats, {
          total: 0,
          pending: 0,
          processing: 0,
          completed: 0,
          failed: 0,
          error: 0
        });
      }
      
    } catch (error) {
      console.error(`❌ [RedisQueue] Error initializing queue ${this.name}:`, error);
      throw error;
    }
  }

  async enqueue(taskData) {
    const task = new Task({
      id: this.generateTaskId(),
      type: taskData.type,
      model: taskData.model,
      operation: taskData.operation,
      data: taskData.data,
      options: taskData.options || {},
      queueName: this.name,
      createdAt: new Date(),
      status: "pending",
    });

    const pipeline = this.redis.pipeline();
    
    pipeline.lpush(this.keys.pending, task.serialize());
    
    pipeline.hset(this.keys.tasks, task.id, task.serialize());
    
    pipeline.hincrby(this.keys.stats, 'total', 1);
    pipeline.hincrby(this.keys.stats, 'pending', 1);
    
    await pipeline.exec();

    this.emit("task:enqueued", task);
    
    return task.id;
  }

async dequeue(batchSize = 1) {
  if (batchSize === 1) {
    const taskData = await this.redis.rpoplpush(this.keys.pending, this.keys.processing);
    if (!taskData) return null;
    
    const task = Task.deserialize(taskData);
    task.status = "processing";
    task.startedAt = new Date();
    
    const pipeline = this.redis.pipeline();
    pipeline.hset(this.keys.tasks, task.id, task.serialize());
    pipeline.hincrby(this.keys.stats, 'pending', -1);
    pipeline.hincrby(this.keys.stats, 'processing', 1);
    await pipeline.exec();
    
    return task;
  }

  // Procesamiento por lotes
  const tasks = [];
  const pipeline = this.redis.pipeline();
  
  const luaScript = `
    local batchSize = tonumber(ARGV[1])
    local pendingKey = KEYS[1]
    local processingKey = KEYS[2]
    local results = {}
    
    for i = 1, batchSize do
      local task = redis.call('rpoplpush', pendingKey, processingKey)
      if task then
        table.insert(results, task)
      else
        break
      end
    end
    
    return results
  `;
  
  const taskDataArray = await this.redis.eval(
    luaScript, 
    2, 
    this.keys.pending, 
    this.keys.processing, 
    batchSize
  );

  if (!taskDataArray || taskDataArray.length === 0) return [];

  for (const taskData of taskDataArray) {
    const task = Task.deserialize(taskData);
    task.status = "processing";
    task.startedAt = new Date();
    
    pipeline.hset(this.keys.tasks, task.id, task.serialize());
    tasks.push(task);
  }
  
  pipeline.hincrby(this.keys.stats, 'pending', -taskDataArray.length);
  pipeline.hincrby(this.keys.stats, 'processing', taskDataArray.length);
  await pipeline.exec();

  return tasks;
}

  async getTask(taskId) {
    const taskData = await this.redis.hget(this.keys.tasks, taskId);
    return taskData ? Task.deserialize(taskData) : null;
  }

  async updateTaskStatus(taskId, status, result = null, error = null, threadId =null) {
    const taskData = await this.redis.hget(this.keys.tasks, taskId);
    if (!taskData) return false;

    const task = Task.deserialize(taskData);
    const oldStatus = task.status;
    
    task.status = status;
    task.completedAt = new Date();
    if (threadId) task.threadId = threadId;

    if (result) task.result = result;
    if (error) {
      task.error = error;
      task.retryCount = (task.retryCount || 0) + 1;
    }

    const pipeline = this.redis.pipeline();
    
    pipeline.hset(this.keys.tasks, taskId, task.serialize());
    
    if (oldStatus === 'processing') {
      pipeline.lrem(this.keys.processing, 1, Task.deserialize(taskData).serialize());
      pipeline.hincrby(this.keys.stats, 'processing', -1);
    }
    
    if (status === 'completed') {
      pipeline.lpush(this.keys.completed, task.serialize());
      pipeline.hincrby(this.keys.stats, 'completed', 1);
    } else if (status === 'failed' || status === 'error') {
      pipeline.lpush(this.keys.failed, task.serialize());
      pipeline.hincrby(this.keys.stats, status, 1);
    }
    
    await pipeline.exec();

    this.emit("task:updated", task);
    return true;
  }

  async requeueTask(taskId) {
    const taskData = await this.redis.hget(this.keys.tasks, taskId);
    if (!taskData) return false;

    const task = Task.deserialize(taskData);

    if (task.retryCount >= this.maxRetries) {
      await this.updateTaskStatus(taskId, 'failed');
      return false;
    }

    task.status = 'pending';
    task.retryCount = (task.retryCount || 0) + 1;
    
    const pipeline = this.redis.pipeline();
    
    pipeline.lrem(this.keys.processing, 1, taskData);
    
    pipeline.lpush(this.keys.pending, task.serialize());
    
    pipeline.hset(this.keys.tasks, taskId, task.serialize());
    
    pipeline.hincrby(this.keys.stats, 'processing', -1);
    pipeline.hincrby(this.keys.stats, 'pending', 1);
    
    await pipeline.exec();

    this.emit("task:requeued", task);
    return true;
  }

  async getStats() {
    
    try {
      const stats = await this.redis.hgetall(this.keys.stats);
      
      const numericStats = {};
      for (const [key, value] of Object.entries(stats)) {
        numericStats[key] = parseInt(value) || 0;
      }
      
      return {
        name: this.name,
        ...numericStats
      };
    } catch (error) {
   //   console.error(`❌ [RedisQueue] Error generating stats:`, error);
      throw error;
    }
  }

  async getQueueTasks(options = {}) {
    const { status, limit = 50, offset = 0 } = options;
    const tasks = [];
    
    let key;
    switch(status) {
      case 'pending':
        key = this.keys.pending;
        break;
      case 'processing':
        key = this.keys.processing;
        break;
      case 'completed':
        key = this.keys.completed;
        break;
      case 'failed':
      case 'error':
        key = this.keys.failed;
        break;
      default:
        // Get all tasks from tasks hash
        const allTasks = await this.redis.hgetall(this.keys.tasks);
        const taskArray = Object.values(allTasks)
          .map(data => Task.deserialize(data))
          .slice(offset, offset + limit);
        
        return {
          tasks: taskArray.map(task => ({
            id: task.id,
            status: task.status,
            model: task.model,
            operation: task.operation,
            threadId: task.threadId,
            createdAt: task.createdAt,
            startedAt: task.startedAt,
            completedAt: task.completedAt,
            error: task.error,
            retryCount: task.retryCount
          })),
          total: Object.keys(allTasks).length
        };
    }
    
    if (key) {
      const taskDataArray = await this.redis.lrange(key, offset, offset + limit - 1);
      const tasks = taskDataArray.map(data => {
        const task = Task.deserialize(data);
        return {
          id: task.id,
          status: task.status,
          model: task.model,
          operation: task.operation,
            threadId: task.threadId,
          createdAt: task.createdAt,
          startedAt: task.startedAt,
          completedAt: task.completedAt,
          error: task.error,
          retryCount: task.retryCount
        };
      });
      
      const total = await this.redis.llen(key);
      return { tasks, total };
    }
    
    return { tasks: [], total: 0 };
  }

  generateTaskId() {
    return `${this.name}_${Date.now()}_${Math.random()
      .toString(36)
      .substr(2, 9)}`;
  }

 async destroy() {
  console.log(`\n🗑️ ========== DESTROYING QUEUE '${this.name}' ==========`);
  
  try {
    // Listar TODAS las claves antes de eliminar
    const keysBeforePattern = `queue:${this.name}:*`;
    const keysBefore = await this.redis.keys(keysBeforePattern);
    console.log(`📋 Keys encontradas ANTES de eliminar (${keysBefore.length}):`, keysBefore);
    
    // Método 1: Eliminar usando las claves definidas
    const definedKeys = [
      this.keys.pending,
      this.keys.processing,
      this.keys.completed,
      this.keys.failed,
      this.keys.tasks,
      this.keys.stats
    ];
    
    console.log(`📝 Keys definidas a eliminar (${definedKeys.length}):`, definedKeys);
    
    // Eliminar una por una para ver cuál falla
    for (const key of definedKeys) {
      try {
        const exists = await this.redis.exists(key);
        if (exists) {
          const deleted = await this.redis.del(key);
          console.log(`   ${deleted ? '✅' : '❌'} Eliminando: ${key} (existed: ${exists})`);
        } else {
          console.log(`   ⏭️  Key no existe: ${key}`);
        }
      } catch (keyError) {
        console.error(`   ❌ ERROR eliminando ${key}:`, keyError.message);
      }
    }
    
    // Método 2: Buscar y eliminar CUALQUIER key relacionada con esta cola
    const keysAfterDefined = await this.redis.keys(keysBeforePattern);
    
    if (keysAfterDefined.length > 0) {
      console.log(`⚠️  Quedan ${keysAfterDefined.length} keys después de eliminar las definidas:`, keysAfterDefined);
      console.log(`🔧 Eliminando keys restantes...`);
      
      if (keysAfterDefined.length > 0) {
        const result = await this.redis.del(...keysAfterDefined);
        console.log(`   Eliminadas ${result} keys adicionales`);
      }
    }
    
    // VERIFICACIÓN FINAL
    const keysFinal = await this.redis.keys(keysBeforePattern);
    
    if (keysFinal.length === 0) {
      console.log(`✅ ÉXITO: Todas las keys de '${this.name}' eliminadas de Redis`);
    } else {
      console.error(`❌ FALLO: Aún quedan ${keysFinal.length} keys en Redis:`, keysFinal);
      
      // Último intento: SCAN y DELETE
      console.log(`🔧 Último intento con SCAN...`);
      let cursor = '0';
      let deletedByScan = 0;
      
      do {
        const [newCursor, keys] = await this.redis.scan(
          cursor, 
          'MATCH', 
          keysBeforePattern, 
          'COUNT', 
          100
        );
        cursor = newCursor;
        
        if (keys.length > 0) {
          const deleted = await this.redis.del(...keys);
          deletedByScan += deleted;
          console.log(`   Eliminadas ${deleted} keys por SCAN`);
        }
      } while (cursor !== '0');
      
      console.log(`   Total eliminadas por SCAN: ${deletedByScan}`);
    }
    
    // Limpiar listeners
    this.removeAllListeners();
    
    console.log(`========== QUEUE '${this.name}' DESTRUCTION COMPLETE ==========\n`);
    
  } catch (error) {
    console.error(`❌ ERROR FATAL en destroy() de cola '${this.name}':`, error);
    throw error;
  }
} 
}

// Enhanced Queue Worker with Redis support
class QueueWorker extends EventEmitter {
  constructor(id, queue, threadCount = 1, options = {}) {
    super();
    this.id = id;
    this.queue = queue;
    this.threadCount = threadCount;
    this.workers = [];
    this.isRunning = false;
    this.isPaused = false;
    this.processingTasks = new Set();
    this.options = options;
    
    // Callbacks
    this.callbacks = {
      onTaskCompleted: options.onTaskCompleted || null,
      onTaskFailed: options.onTaskFailed || null,
      onTaskError: options.onTaskError || null,
      onWorkerError: options.onWorkerError || null
    };
    
    // Statistics
    this.stats = {
      totalProcessed: 0,
      totalCompleted: 0,
      totalFailed: 0,
      totalErrors: 0,
      lastActivity: null,
      startedAt: null
    };
    
  }

  async start() {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    this.isPaused = false;
    this.stats.startedAt = new Date();

    // Create worker threads
    for (let i = 0; i < this.threadCount; i++) {
      const worker = new Worker(path.join(__dirname, 'worker-thread.js'));

      worker.on('message', async (message) => {
        await this.handleWorkerMessage(message);
      });

      worker.on('error', (error) => {
        console.error(`❌ Worker thread error:`, error);
        this.emit('worker:error', { workerId: this.id, error });
        
        if (this.callbacks.onWorkerError) {
          try {
            this.callbacks.onWorkerError(error, this.id);
          } catch (callbackError) {
            console.error('Error in worker error callback:', callbackError);
          }
        }
      });

      this.workers.push(worker);
    }

    this.processLoop();
    this.emit('worker:started', { workerId: this.id });
  }

  async stop(graceful = true) {
    if (!this.isRunning) {
      return;
    }


    this.isRunning = false;
    this.isPaused = false;

    if (graceful) {
      while (this.processingTasks.size > 0) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    await Promise.all(this.workers.map((worker) => worker.terminate()));
    this.workers = [];

    this.emit('worker:stopped', { workerId: this.id, graceful });
  }

  pause() {
    if (!this.isRunning) {
      throw new Error(`Worker ${this.id} is not running`);
    }
    
    this.isPaused = true;
    this.emit('worker:paused', { workerId: this.id });
  }

  resume() {
    if (!this.isRunning) {
      throw new Error(`Worker ${this.id} is not running`);
    }
    
    if (!this.isPaused) {
      return;
    }
    
    this.isPaused = false;
    this.emit('worker:resumed', { workerId: this.id });
  }

  async processLoop() {
    while (this.isRunning) {
      try {
        if (this.isPaused) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
          continue;
        }

        if (this.processingTasks.size < this.threadCount) {
          const task = await this.queue.dequeue();

          if (task) {
            await this.processTask(task);
          } else {
            await new Promise((resolve) => setTimeout(resolve, 1000));
          }
        } else {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      } catch (error) {
        console.error('Error in process loop:', error);
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  async processTask(task) {
    const availableWorker = this.workers.find(
      (w) => !this.processingTasks.has(w.threadId)
    );
    
    if (!availableWorker) {
      // Return task to queue using Redis
      await this.queue.redis.rpoplpush(this.queue.keys.processing, this.queue.keys.pending);
      return;
    }

    this.processingTasks.add(availableWorker.threadId);
    this.stats.totalProcessed++;
    this.stats.lastActivity = new Date();

    try {
      
      availableWorker.postMessage({
        type: 'process',
        task: task.serialize(),
      });
    } catch (error) {
      console.error(`❌ Error sending task to worker:`, error);
      this.processingTasks.delete(availableWorker.threadId);
      await this.queue.updateTaskStatus(task.id, 'error', null, error.message);
    }
  }

  async handleWorkerMessage(message) {
    const { type, taskId, result, error, threadId } = message;

    if (threadId) {
      this.processingTasks.delete(threadId);
    }

    this.stats.lastActivity = new Date();

    switch (type) {
      case 'task:completed':
        await this.queue.updateTaskStatus(taskId, 'completed', result, null, threadId);
        this.stats.totalCompleted++;
        
        this.emit('task:completed', { taskId, result, workerId: this.id });
        
        if (this.callbacks.onTaskCompleted) {
          try {
            await this.callbacks.onTaskCompleted({
              taskId,
              result,
              workerId: this.id,
              queueName: this.queue.name
            });
          } catch (callbackError) {
            console.error('Error in task completed callback:', callbackError);
          }
        }
        break;

      case 'task:failed':
        const shouldRetry = await this.queue.requeueTask(taskId);
        this.stats.totalFailed++;
        
        this.emit('task:failed', { taskId, error, retry: shouldRetry, workerId: this.id });
        
        if (this.callbacks.onTaskFailed) {
          try {
            await this.callbacks.onTaskFailed({
              taskId,
              error,
              retry: shouldRetry,
              workerId: this.id,
              queueName: this.queue.name
            });
          } catch (callbackError) {
            console.error('Error in task failed callback:', callbackError);
          }
        }
        break;

      case 'task:error':
        await this.queue.updateTaskStatus(taskId, 'error', null, error, threadId);
        this.stats.totalErrors++;
        
        this.emit('task:error', { taskId, error, workerId: this.id });
        
        if (this.callbacks.onTaskError) {
          try {
            await this.callbacks.onTaskError({
              taskId,
              error,
              workerId: this.id,
              queueName: this.queue.name
            });
          } catch (callbackError) {
            console.error('Error in task error callback:', callbackError);
          }
        }
        break;
    }
  }

  getStats() {
    return {
      id: this.id,
      queueName: this.queue.name,
      isRunning: this.isRunning,
      isPaused: this.isPaused,
      threadCount: this.threadCount,
      processingTasks: this.processingTasks.size,
      stats: {
        ...this.stats,
        uptime: this.stats.startedAt ? Date.now() - this.stats.startedAt.getTime() : 0
      }
    };
  }

  updateCallbacks(callbacks) {
    this.callbacks = {
      ...this.callbacks,
      ...callbacks
    };
  }
}

// Redis-based Queue Manager
class QueueManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.queues = new Map();
    this.workers = new Map();
    this.maxRetries = options.maxRetries || 3;
    this.retryDelay = options.retryDelay || 1000;
    this.initialized = false;
    
    this.redis = new Redis({
      host: options.redisHost || process.env.REDIS_HOST || 'localhost',
      port: options.redisPort || process.env.REDIS_PORT || 6379,
      password: options.redisPassword || process.env.REDIS_PASSWORD,
      db: options.redisDb || process.env.REDIS_DB || 0,
      retryDelayOnFailover: 100,
      enableReadyCheck: false,
      maxRetriesPerRequest: null,
    });

      // ✅ NUEVO: Clave para almacenar configuración de workers
    this.workersConfigKey = 'workers:config';
  }



  async saveWorkerConfig(workerId, config) {
  try {
    const finalConfig = {
      queueName: config.queueName,
      threadCount: config.threadCount,
      options: config.options || {},
      isRunning: config.isRunning !== undefined ? config.isRunning : true,
      isPaused: config.isPaused !== undefined ? config.isPaused : false,
      createdAt: config.createdAt || new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    await this.redis.hset(
      this.workersConfigKey,
      workerId,
      JSON.stringify(finalConfig)
    );
    
    console.log(`💾 Worker config saved: ${workerId}`, {
      isRunning: finalConfig.isRunning,
      isPaused: finalConfig.isPaused
    });
  } catch (error) {
    console.error('❌ Error saving worker config:', error);
    throw error;
  }
}

  // ✅ NUEVO: Método para eliminar configuración de worker
  async deleteWorkerConfig(workerId) {
    try {
      await this.redis.hdel(this.workersConfigKey, workerId);
    } catch (error) {
      console.error('Error deleting worker config:', error);
    }
  }

  async loadPersistedWorkers() {
  try {
    const workersConfig = await this.redis.hgetall(this.workersConfigKey);
    
    if (!workersConfig || Object.keys(workersConfig).length === 0) {
      console.log('📭 No persisted workers found');
      return;
    }

    console.log(`📦 Loading ${Object.keys(workersConfig).length} persisted workers...`);

    for (const [workerId, configStr] of Object.entries(workersConfig)) {
      try {
        const config = JSON.parse(configStr);
        
        // Verificar que la cola existe
        const queue = await this.getQueue(config.queueName);
        if (!queue) {
          console.log(`⚠️ Queue '${config.queueName}' not found for worker '${workerId}', deleting config...`);
          await this.deleteWorkerConfig(workerId);
          continue;
        }

        // Recrear el worker SIN iniciarlo automáticamente
        const worker = new QueueWorker(
          workerId, 
          queue, 
          config.threadCount, 
          config.options
        );
        
        this.workers.set(workerId, worker);

        // ✅ RESPETAR ESTADO PERSISTIDO
        if (config.isRunning === true) {
          await worker.start();
          
          // Si estaba pausado, aplicar pausa DESPUÉS de iniciar
          if (config.isPaused === true) {
            worker.pause();
            console.log(`✅ Worker '${workerId}' restored → RUNNING + PAUSED`);
          } else {
            console.log(`✅ Worker '${workerId}' restored → RUNNING`);
          }
        } else {
          console.log(`✅ Worker '${workerId}' restored → STOPPED`);
        }

      } catch (parseError) {
        console.error(`❌ Error restoring worker '${workerId}':`, parseError);
        await this.deleteWorkerConfig(workerId);
      }
    }
    
    console.log(`✅ ${this.workers.size} workers cargados desde Redis`);
  } catch (error) {
    console.error('❌ Error loading persisted workers:', error);
  }
}

  async initialize() {
    if (this.initialized) return;

    try {
      await this.redis.ping();
      await this.loadPersistedQueues();
      await this.loadPersistedWorkers(); // ✅ NUEVO: Cargar workers

      this.initialized = true;
    } catch (error) {
      console.error("❌ Error initializing RedisQueueManager:", error);
      throw error;
    }
  }

  async loadPersistedQueues() {
  try {
    const statsKeys = await this.redis.keys('queue:*:stats');
    
    if (statsKeys.length === 0) {
      console.log('📭 No persisted queues found');
      return;
    }
    
    const queueNames = statsKeys.map(key => {
      const match = key.match(/^queue:([^:]+):stats$/);
      return match ? match[1] : null;
    }).filter(Boolean);
    
    console.log(`📦 Found ${queueNames.length} persisted queues: ${queueNames.join(', ')}`);

    for (const queueName of queueNames) {
      if (this.queues.has(queueName)) {
        console.log(`   ℹ️  Cola '${queueName}' ya existe en memoria`);
        continue;
      }
      
      // SIMPLIFICADO: Solo verificar que exista la key stats
      const statsExist = await this.redis.exists(`queue:${queueName}:stats`);
      
      if (!statsExist) {
        console.log(`⚠️  Cola '${queueName}' sin stats en Redis, limpiando...`);
        const orphanKeys = await this.redis.keys(`queue:${queueName}:*`);
        if (orphanKeys.length > 0) {
          await this.redis.del(...orphanKeys);
          console.log(`   🗑️  Eliminadas ${orphanKeys.length} keys huérfanas`);
        }
        continue;
      }
      
      // Recrear cola válida
      const queue = new RedisQueue(queueName, {
        redis: this.redis,
        maxRetries: this.maxRetries,
        retryDelay: this.retryDelay,
      });

      await queue.initialize();
      this.queues.set(queueName, queue);
      
      console.log(`✅ Cola '${queueName}' cargada desde Redis`);
    }
    
    console.log(`✅ Total de colas cargadas: ${this.queues.size}`);
  } catch (error) {
    console.error("❌ Error loading persisted queues:", error);
    throw error;
  }
}

  async createQueue(queueName, options = {}) {
    if (!this.initialized) {
      await this.initialize();
    }

    if (this.queues.has(queueName)) {
      console.log(`📋 Queue '${queueName}' already exists`);
      return this.queues.get(queueName);
    }

    const queue = new RedisQueue(queueName, {
      ...options,
      redis: this.redis,
      maxRetries: this.maxRetries,
      retryDelay: this.retryDelay,
    });
//a
    await queue.initialize();
    this.queues.set(queueName, queue); 

    console.log(`✅ Queue '${queueName}' created`);
    return queue;
  }

  async getQueue(queueName) {
    return this.queues.get(queueName);
  }

 async deleteQueue(queueName) {
  console.log(`🗑️ Iniciando eliminación de cola '${queueName}'...`);
  
  const queue = this.queues.get(queueName);
  
  // PRIMERO: Detener y eliminar workers de esta cola
  const workersToDelete = [];
  for (const [workerId, worker] of this.workers.entries()) {
    if (worker.queue.name === queueName) {
      workersToDelete.push(workerId);
    }
  }
 
  console.log(`   Encontrados ${workersToDelete.length} workers para eliminar`);
  
  for (const workerId of workersToDelete) {
    const worker = this.workers.get(workerId);
    if (worker && worker.isRunning) {
      await worker.stop(false); // Stop sin graceful para ser más rápido
    }
    this.workers.delete(workerId);
    await this.deleteWorkerConfig(workerId);
    console.log(`   🗑️ Worker '${workerId}' eliminado`);
  }
  
  // SEGUNDO: Destruir la cola (esto limpia Redis)
  if (queue) {
    await queue.destroy();
  } else {
    console.log(`⚠️ Cola '${queueName}' no encontrada en memoria, limpiando Redis directamente...`);
    // Limpiar Redis aunque no exista en memoria
    const keysToDelete = await this.redis.keys(`queue:${queueName}:*`);
    if (keysToDelete.length > 0) {
      await this.redis.del(...keysToDelete);
      console.log(`   Eliminadas ${keysToDelete.length} keys huérfanas de Redis`);
    }
  }
  
  // TERCERO: Eliminar de memoria
  this.queues.delete(queueName);
  
  // VERIFICACIÓN FINAL
  const remainingKeys = await this.redis.keys(`queue:${queueName}:*`);
  const remainingWorkers = workersToDelete.filter(wId => this.workers.has(wId));
  
  console.log(`✅ Cola '${queueName}' eliminada:`);
  console.log(`   - Workers eliminados: ${workersToDelete.length}`);
  console.log(`   - Keys restantes en Redis: ${remainingKeys.length}`);
  console.log(`   - Workers restantes en memoria: ${remainingWorkers.length}`);
  
  if (remainingKeys.length > 0) {
    console.error(`❌ ERROR: Aún hay ${remainingKeys.length} keys en Redis:`, remainingKeys);
  }
}

async createWorker(queueName, threadCount = 1, options = {}) {
  const queue = await this.getQueue(queueName);
  if (!queue) {
    throw new Error(`Queue '${queueName}' not found`);
  }

  const workerId = `${queueName}_worker_${Date.now()}`;
  const worker = new QueueWorker(workerId, queue, threadCount, options);

  this.workers.set(workerId, worker);
  worker.batchSize = options.batchSize || 1;
  
  // ✅ Guardar estado inicial en Redis
  const initialState = {
    queueName,
    threadCount,
    options,
    isRunning: options.autoStart !== false, // true por defecto
    isPaused: false,
    createdAt: new Date().toISOString()
  };
  
  await this.saveWorkerConfig(workerId, initialState);
  
  // Auto-iniciar si corresponde
  if (options.autoStart !== false) {
    await worker.start();
  }

  console.log(`✅ Worker '${workerId}' created with initial state:`, {
    isRunning: initialState.isRunning,
    isPaused: initialState.isPaused
  });
  
  return { workerId, worker };
}


   async stopWorker(workerId) {
    const worker = this.workers.get(workerId);
    if (worker) {
      await worker.stop();
      this.workers.delete(workerId);
      
      // ✅ NUEVO: Eliminar configuración de Redis
      await this.deleteWorkerConfig(workerId);
      
      console.log(`✅ Worker '${workerId}' stopped and removed from persistence`);
    }
  }

  async getQueueStats(queueName) {

    if (!queueName || typeof queueName !== "string") {
      console.error(`❌ [RedisQueueManager] Invalid queueName:`, queueName);
      return null;
    }

    const queue = await this.getQueue(queueName);
    if (!queue) {
      return null;
    }

    try {
      const stats = await queue.getStats();
      return stats;
    } catch (error) {
      console.error(`❌ [RedisQueueManager] Error in queue.getStats():`, error);
      throw error;
    }
  }

  async getAllQueuesStats() {
    const stats = {};
    for (const [name, queue] of this.queues) {
      stats[name] = await queue.getStats();
    }
    return stats;
  }

 async shutdown() {
    // Detener workers PERO mantener su configuración
    const workerPromises = Array.from(this.workers.values()).map((worker) =>
      worker.stop()
    );
    await Promise.all(workerPromises);
    
    // ✅ MODIFICADO: NO eliminar configuración de workers en shutdown
    // Solo limpiar la memoria
    this.workers.clear();
    
    const queuePromises = Array.from(this.queues.values()).map((queue) =>
      queue.destroy()
    );
    await Promise.all(queuePromises);
    await this.redis.quit();

    this.queues.clear();
    console.log('✅ Shutdown complete - worker configs preserved');
  }
}

module.exports = {
  QueueManager,
  RedisQueue,
  Task,
  QueueWorker,
};
