// services/CallbackService.js
const { EventEmitter } = require('events');

class CallbackService extends EventEmitter {
  constructor() {
    super();
    this.callbacks = new Map();
    this.callbackHistory = [];
    this.maxHistorySize = 1000;
  }

  // Registrar callback para tipos específicos de eventos
  registerCallback(eventType, callbackId, callback) {
    if (!this.callbacks.has(eventType)) {
      this.callbacks.set(eventType, new Map());
    }
    
    this.callbacks.get(eventType).set(callbackId, callback);
    console.log(`Callback registrado: ${eventType} -> ${callbackId}`);
  }

  // Remover callback
  removeCallback(eventType, callbackId) {
    if (this.callbacks.has(eventType)) {
      this.callbacks.get(eventType).delete(callbackId);
      console.log(`Callback removido: ${eventType} -> ${callbackId}`);
    }
  }

  // Ejecutar todos los callbacks para un evento específico
  async executeCallbacks(eventType, data) {
    const startTime = Date.now();
    let executedCallbacks = 0;
    let errors = [];

    try {
      if (this.callbacks.has(eventType)) {
        const eventCallbacks = this.callbacks.get(eventType);
        
        for (const [callbackId, callback] of eventCallbacks) {
          try {
            console.log(`Ejecutando callback ${callbackId} para evento ${eventType}`);
            
            if (typeof callback === 'function') {
              await callback(data);
            } else {
              console.error(`Callback ${callbackId} no es una función`);
            }
            
            executedCallbacks++;
          } catch (error) {
            console.error(`Error en callback ${callbackId}:`, error);
            errors.push({
              callbackId,
              error: error.message,
              stack: error.stack
            });
          }
        }
      }

      // Guardar en historial
      this.addToHistory({
        eventType,
        data,
        executedCallbacks,
        errors,
        timestamp: new Date(),
        duration: Date.now() - startTime
      });

      // Emitir evento para notificación
      this.emit('callbacks:executed', {
        eventType,
        executedCallbacks,
        errors: errors.length,
        duration: Date.now() - startTime
      });

    } catch (error) {
      console.error(`Error ejecutando callbacks para ${eventType}:`, error);
      this.addToHistory({
        eventType,
        data,
        executedCallbacks: 0,
        errors: [{ error: error.message, stack: error.stack }],
        timestamp: new Date(),
        duration: Date.now() - startTime
      });
    }

    return {
      executed: executedCallbacks,
      errors: errors.length,
      success: errors.length === 0
    };
  }

  // Callback por defecto para tareas completadas
  static defaultTaskCompletedCallback = async (data) => {
    const { taskId, result, workerId, queueName } = data;
    console.log(`✅ [CALLBACK] Tarea completada exitosamente:`);
    console.log(`   - Task ID: ${taskId}`);
    console.log(`   - Worker: ${workerId}`);
    console.log(`   - Queue: ${queueName}`);
    console.log(`   - Resultado: ${JSON.stringify(result, null, 2)}`);
    
    // Aquí puedes agregar lógica adicional como:
    // - Enviar notificación
    // - Actualizar base de datos
    // - Disparar otros procesos
  };

  // Callback por defecto para tareas fallidas
  static defaultTaskFailedCallback = async (data) => {
    const { taskId, error, retry, workerId, queueName } = data;
    console.log(`⚠️ [CALLBACK] Tarea falló:`);
    console.log(`   - Task ID: ${taskId}`);
    console.log(`   - Worker: ${workerId}`);
    console.log(`   - Queue: ${queueName}`);
    console.log(`   - Error: ${error}`);
    console.log(`   - Se reintentará: ${retry ? 'Sí' : 'No'}`);
    
    if (!retry) {
      console.log(`❌ [CALLBACK] Tarea ${taskId} falló definitivamente después de reintentos`);
      // Lógica para tareas que fallaron definitivamente
    }
  };

  // Callback por defecto para errores de tareas
  static defaultTaskErrorCallback = async (data) => {
    const { taskId, error, workerId, queueName } = data;
    console.error(`❌ [CALLBACK] Error en tarea:`);
    console.error(`   - Task ID: ${taskId}`);
    console.error(`   - Worker: ${workerId}`);
    console.error(`   - Queue: ${queueName}`);
    console.error(`   - Error: ${error}`);
    
    // Aquí puedes agregar:
    // - Log a archivo de errores
    // - Enviar alerta
    // - Notificar administrador
  };

  // Callback por defecto para errores de worker
  static defaultWorkerErrorCallback = async (error, workerId) => {
    console.error(`❌ [CALLBACK] Error en worker ${workerId}:`);
    console.error(`   - Error: ${error.message}`);
    console.error(`   - Stack: ${error.stack}`);
    
    // Lógica para manejar errores de worker:
    // - Reiniciar worker
    // - Notificar sistema de monitoreo
    // - Escalar problema
  };

  // Agregar al historial
  addToHistory(entry) {
    this.callbackHistory.unshift(entry);
    
    if (this.callbackHistory.length > this.maxHistorySize) {
      this.callbackHistory = this.callbackHistory.slice(0, this.maxHistorySize);
    }
  }

  // Obtener historial de callbacks
  getHistory(limit = 50, eventType = null) {
    let history = this.callbackHistory;
    
    if (eventType) {
      history = history.filter(entry => entry.eventType === eventType);
    }
    
    return history.slice(0, limit);
  }

  // Obtener estadísticas de callbacks
  getStats() {
    const stats = {
      totalCallbacks: 0,
      callbacksByType: {},
      recentExecutions: 0,
      recentErrors: 0
    };

    // Contar callbacks registrados
    for (const [eventType, callbacks] of this.callbacks) {
      stats.callbacksByType[eventType] = callbacks.size;
      stats.totalCallbacks += callbacks.size;
    }

    // Estadísticas de las últimas 24 horas
    const yesterday = Date.now() - (24 * 60 * 60 * 1000);
    const recentHistory = this.callbackHistory.filter(entry => 
      entry.timestamp.getTime() > yesterday
    );

    stats.recentExecutions = recentHistory.reduce((sum, entry) => 
      sum + entry.executedCallbacks, 0
    );

    stats.recentErrors = recentHistory.reduce((sum, entry) => 
      sum + entry.errors.length, 0
    );

    return stats;
  }

  // Configurar callbacks por defecto
  setupDefaultCallbacks() {
    this.registerCallback('task:completed', 'default', 
      CallbackService.defaultTaskCompletedCallback);
    
    this.registerCallback('task:failed', 'default', 
      CallbackService.defaultTaskFailedCallback);
    
    this.registerCallback('task:error', 'default', 
      CallbackService.defaultTaskErrorCallback);
    
    this.registerCallback('worker:error', 'default', 
      CallbackService.defaultWorkerErrorCallback);
    
    console.log('✅ Callbacks por defecto configurados');
  }

  // Limpiar callbacks
  clearAllCallbacks() {
    this.callbacks.clear();
    console.log('🧹 Todos los callbacks removidos');
  }

  // Limpiar historial
  clearHistory() {
    this.callbackHistory = [];
    console.log('🧹 Historial de callbacks limpiado');
  }


    listCallbacks() {
    const callbacks = {};

    for (const [eventType, eventCallbacks] of this.callbacks) {
      callbacks[eventType] = Array.from(eventCallbacks.keys());
    }

    return callbacks;
  }

  // Método adicional para obtener historial de ejecuciones
  getExecutionHistory(limit = 50, eventType = null) {
    return this.getHistory(limit, eventType);
  }
  
}

module.exports = CallbackService;