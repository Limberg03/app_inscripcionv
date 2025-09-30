// queue/QueueWorkerEnhanced.js
const { Worker } = require('worker_threads');
const { EventEmitter } = require('events');
const path = require('path');

class QueueWorker extends EventEmitter {
  constructor(id, queue, threadCount = 1, options = {}) {
    super();
    this.id = id;
    this.queue = queue;
    this.threadCount = threadCount;
    this.batchSize = options.batchSize || 1; 
    this.workers = [];
    this.isRunning = false;
    this.isPaused = false;  // ✅ NUEVA FUNCIONALIDAD
    this.processingTasks = new Set();
    this.options = options;
    
    // ✅ CALLBACK SYSTEM
    this.callbacks = {
      onTaskCompleted: options.onTaskCompleted || null,
      onTaskFailed: options.onTaskFailed || null,
      onTaskError: options.onTaskError || null,
      onWorkerError: options.onWorkerError || null
    };
    
    // Estadísticas mejoradas
    this.stats = {
      totalProcessed: 0,
      totalCompleted: 0,
      totalFailed: 0,
      totalErrors: 0,
      lastActivity: null,
      startedAt: null
    };
    
    console.log(`🏗️ QueueWorker ${this.id} created with ${this.threadCount} threads`);
  }

  async start() {
    if (this.isRunning) {
      console.log(`⚠️ Worker ${this.id} is already running`);
      return;
    }

    this.isRunning = true;
    this.isPaused = false;
    this.stats.startedAt = new Date();

    // Crear worker threads
    for (let i = 0; i < this.threadCount; i++) {
      const worker = new Worker(path.join(__dirname, 'worker-thread.js'));


        worker.workerId = `${this.id}_thread_${i}`;
      worker.on('message', async (message) => {
        await this.handleWorkerMessage(message);
      });

      worker.on('error', (error) => {
        console.error(`❌ Worker thread error:`, error);
        this.emit('worker:error', { workerId: this.id, error });
        
        // Ejecutar callback de error de worker
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

    // Iniciar el loop de procesamiento
    this.processLoop();

    console.log(`✅ QueueWorker ${this.id} started with ${this.threadCount} threads`);
    this.emit('worker:started', { workerId: this.id });
  }

  async stop(graceful = true) {
    if (!this.isRunning) {
      console.log(`⚠️ Worker ${this.id} is already stopped`);
      return;
    }

    console.log(`🔄 Stopping QueueWorker ${this.id}${graceful ? ' (graceful)' : ' (forced)'}...`);

    this.isRunning = false;
    this.isPaused = false;

    if (graceful) {
      // Esperar a que terminen las tareas actuales
      console.log(`⏳ Waiting for ${this.processingTasks.size} tasks to complete...`);
      while (this.processingTasks.size > 0) {
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    // Terminar todos los worker threads
    await Promise.all(this.workers.map((worker) => worker.terminate()));
    this.workers = [];

    console.log(`✅ QueueWorker ${this.id} stopped`);
    this.emit('worker:stopped', { workerId: this.id, graceful });
  }

  pause() {
    if (!this.isRunning) {
      throw new Error(`Worker ${this.id} is not running`);
    }
    
    this.isPaused = true;
    console.log(`⏸️ QueueWorker ${this.id} paused`);
    this.emit('worker:paused', { workerId: this.id });
  }

  resume() {
    if (!this.isRunning) {
      throw new Error(`Worker ${this.id} is not running`);
    }
    
    if (!this.isPaused) {
      console.log(`⚠️ Worker ${this.id} is not paused`);
      return;
    }
    
    this.isPaused = false;
    console.log(`▶️ QueueWorker ${this.id} resumed`);
    this.emit('worker:resumed', { workerId: this.id });
  }

async processLoop() {
  const BATCH_SIZE = this.options.batchSize || 5; // Configurable
  
  while (this.isRunning) {
    try {
      if (this.isPaused) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        continue;
      }

      const availableSlots = this.threadCount - this.processingTasks.size;
      
      if (availableSlots > 0) {
        // NUEVO: Obtener lote de tareas
        const batchSize = Math.min(availableSlots, BATCH_SIZE);
        const tasks = await this.queue.dequeue(batchSize);

        if (tasks && tasks.length > 0) {
          console.log(`📦 Processing batch of ${tasks.length} tasks`);
          
          // Procesar cada tarea del lote
          for (const task of tasks) {
            await this.processTask(task);
          }
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
    task.threadId = availableWorker.threadId;
    
    if (!availableWorker) {
      // No hay worker disponible, devolver tarea a la cola
      this.queue.tasks.unshift(task);
      return;
    }

    this.processingTasks.add(availableWorker.threadId);
    this.stats.totalProcessed++;
    this.stats.lastActivity = new Date();

    try {
      console.log(`🔧 Processing task ${task.id} with worker thread ${availableWorker.threadId}`);
      
      availableWorker.postMessage({
        type: 'process',
        task: task.serialize(),
         threadId: availableWorker.threadId 
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
        await this.queue.updateTaskStatus(taskId, 'completed', result,null, threadId);
        this.stats.totalCompleted++;
        
        console.log(`✅ Task ${taskId} completed successfully`);
        this.emit('task:completed', { taskId, result, workerId: this.id });
        
        // ✅ EJECUTAR CALLBACK
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
        
        console.log(`⚠️ Task ${taskId} failed, retry: ${shouldRetry}`);
        this.emit('task:failed', { taskId, error, retry: shouldRetry, workerId: this.id });
        
        // ✅ EJECUTAR CALLBACK
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
        
        if (!shouldRetry) {
          await this.queue.updateTaskStatus(taskId, 'failed', null, error);
        }
        break;

      case 'task:error':
        await this.queue.updateTaskStatus(taskId, 'error', null, error);
        this.stats.totalErrors++;
        
        console.log(`❌ Task ${taskId} error: ${error}`);
        this.emit('task:error', { taskId, error, workerId: this.id });
        
        // ✅ EJECUTAR CALLBACK
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

  // ✅ MÉTODO PARA OBTENER ESTADÍSTICAS DETALLADAS
  getStats() {
    return {
      id: this.id,
      queueName: this.queue.name,
      isRunning: this.isRunning,
      isPaused: this.isPaused,
      threadCount: this.threadCount,
       batchSize: this.batchSize,
      processingTasks: this.processingTasks.size,
      stats: {
        ...this.stats,
        uptime: this.stats.startedAt ? Date.now() - this.stats.startedAt.getTime() : 0
      }
    };
  }

  // ✅ MÉTODO PARA ACTUALIZAR CALLBACKS EN RUNTIME
  updateCallbacks(callbacks) {
    this.callbacks = {
      ...this.callbacks,
      ...callbacks
    };
    console.log(`🔧 Updated callbacks for worker ${this.id}`);
  }
}

module.exports = QueueWorker;