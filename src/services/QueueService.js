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

  async deleteQueue(queueName) {
    if (!this.initialized) await this.initialize();
    
    await this.queueManager.deleteQueue(queueName);
    return {
      success: true,
      queueName,
      message: 'Queue deleted successfully'
    };
  }

  async createWorker(queueName, threadCount = 1, options = {}) {
    if (!this.initialized) await this.initialize();
    
    const { workerId, worker } = await this.queueManager.createWorker(queueName, threadCount, options);
    return {
      success: true,
      workerId,
      queueName,
      threadCount,
      message: 'Worker created successfully',
      worker: worker
    };
  }

  async stopWorker(workerId) {
    if (!this.initialized) await this.initialize();
    
    await this.queueManager.stopWorker(workerId);
    return {
      success: true,
      workerId,
      message: 'Worker stopped successfully'
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
    return {
      success: true,
      workerId,
      message: 'Worker paused successfully'
    };
  }

  async resumeWorker(workerId) {
    if (!this.initialized) await this.initialize();
    
    const worker = this.queueManager.workers.get(workerId);
    if (!worker) {
      throw new Error(`Worker '${workerId}' not found`);
    }

    worker.resume();
    return {
      success: true,
      workerId,
      message: 'Worker resumed successfully'
    };
  }

  async startWorker(workerId) {
    if (!this.initialized) await this.initialize();
    
    const worker = this.queueManager.workers.get(workerId);
    if (!worker) {
      throw new Error(`Worker '${workerId}' not found`);
    }

    await worker.start();
    return {
      success: true,
      workerId,
      message: 'Worker started successfully'
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
      workers.push({
        id: workerId,
        ...worker.getStats()
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

    const validOperations = ['create', 'update', 'delete', 'bulkcreate', 'bulkupdate', 'bulkdelete', 'custom'];
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