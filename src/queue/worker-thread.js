const { parentPort, threadId } = require('worker_threads');
const TaskProcessor = require('./TaskProcessor');

let processor = null;

parentPort.on('message', async (message) => {
  try {
    console.log(`[Worker ${threadId}] Mensaje recibido tipo:`, message.type);
    
    const { type, task } = message;

    if (type === 'process') {
  try {
    if (!processor) {
      console.log(`[Worker ${threadId}] Inicializando TaskProcessor...`);
      processor = new TaskProcessor();
      await processor.initialize();
      console.log(`[Worker ${threadId}] TaskProcessor inicializado`);
    }

    // NUEVO: Soporte para lotes
    const tasks = Array.isArray(task) ? task : [task];
    const results = [];
    
    console.log(`[Worker ${threadId}] Procesando lote de ${tasks.length} tareas`);

    for (const singleTask of tasks) {
      let taskObject;
      if (typeof singleTask === 'string') {
        taskObject = JSON.parse(singleTask);
      } else {
        taskObject = singleTask;
      }

      if (!taskObject?.model || !taskObject?.operation) {
        throw new Error(`Tarea incompleta - model: ${taskObject?.model}, operation: ${taskObject?.operation}`);
      }

      const result = await processor.processTask(taskObject);
      results.push({ taskId: taskObject.id, result });
      
      // Enviar cada resultado individualmente para mantener compatibilidad
      parentPort.postMessage({
        type: 'task:completed',
        taskId: taskObject.id,
        result,
        threadId
      });
    }

    console.log(`[Worker ${threadId}] ✅ Lote de ${tasks.length} tareas completado`);

  } catch (error) {
        console.error(`[Worker ${threadId}] ❌ Error procesando tarea:`, error.message);

        // Intentar obtener taskId incluso si hay error
        let taskId = 'unknown';
        try {
          const taskObj = typeof task === 'string' ? JSON.parse(task) : task;
          taskId = taskObj?.id || 'unknown';
        } catch (parseError) {
          console.error(`[Worker ${threadId}] Error obteniendo taskId:`, parseError.message);
        }

        const shouldRetry = error.retry !== false && (task?.retryCount || 0) < 3;

        parentPort.postMessage({
          type: shouldRetry ? 'task:failed' : 'task:error',
          taskId: taskId,
          error: error.message || String(error),
          threadId,
          shouldRetry
        });
      }
    } else {
      console.log(`[Worker ${threadId}] Tipo de mensaje desconocido:`, type);
    }

  } catch (error) {
    console.error(`[Worker ${threadId}] Error fatal en worker:`, error);
    
    parentPort.postMessage({
      type: 'worker:error',
      error: error.message || String(error),
      threadId
    });
  }
});

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error(`[Worker ${threadId}] Uncaught exception:`, error);
  
  parentPort.postMessage({
    type: 'worker:error',
    error: error.message || String(error),
    threadId
  });
  
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error(`[Worker ${threadId}] Unhandled rejection:`, reason);
  
  parentPort.postMessage({
    type: 'worker:error', 
    error: String(reason),
    threadId
  });
  
  process.exit(1);
});

console.log(`[Worker ${threadId}] Worker thread inicializado y listo`);