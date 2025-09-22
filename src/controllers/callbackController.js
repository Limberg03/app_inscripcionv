// controllers/callbackController.js - VERSIÓN CORREGIDA PARA SINGLETONS
const CallbackService = require('../services/CallbackService');
const QueueService = require('../services/QueueService');

// Usar singletons globales en lugar de crear nuevas instancias
let callbackService = null;
let queueService = null;

const getServices = () => {
  // Obtener las instancias desde los módulos exportados en app.js
  const { callbackService: globalCallback, queueService: globalQueue } = require('../app');
  
  if (globalCallback && typeof globalCallback === 'function') {
    callbackService = globalCallback();
  }
  if (globalQueue && typeof globalQueue === 'function') {
    queueService = globalQueue();
  }

  // Fallback: crear instancias si no hay globales
  if (!callbackService) {
    callbackService = new CallbackService();
    callbackService.setupDefaultCallbacks();
  }
  if (!queueService) {
    queueService = new QueueService();
  }
  
  return { callbackService, queueService };
};

const callbackController = {
  // Registrar callback personalizado - VERSIÓN CORREGIDA
  registerCallback: async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      const { webhookUrl, method = 'POST', headers = {} } = req.body;

      const { callbackService } = getServices();

      console.log(`🔔 Registrando webhook callback: ${eventType} -> ${callbackId}`);
      console.log(`   URL: ${webhookUrl}`);
      console.log(`   Método: ${method}`);

      // CORRECCIÓN: Crear callback que hace HTTP request con manejo de errores mejorado
      const httpCallback = async (data) => {
        try {
          console.log(`🔔 Ejecutando webhook callback: ${callbackId}`);
          console.log(`   URL: ${webhookUrl}`);
          console.log(`   Método: ${method}`);
          console.log(`   Datos:`, JSON.stringify(data, null, 2));

          // Importar fetch dinámicamente
          const fetch = (await import('node-fetch')).default;
          
          const payload = {
            eventType,
            callbackId,
            data,
            timestamp: new Date().toISOString()
          };

          console.log(`📤 Enviando payload:`, JSON.stringify(payload, null, 2));

          const response = await fetch(webhookUrl, {
            method,
            headers: {
              'Content-Type': 'application/json',
              'User-Agent': 'Academic-Queue-System/1.0',
              ...headers
            },
            body: JSON.stringify(payload),
            timeout: 10000 // 10 seconds timeout
          });

          console.log(`📥 Respuesta del webhook: ${response.status} ${response.statusText}`);
          
          if (!response.ok) {
            throw new Error(`Webhook failed: ${response.status} ${response.statusText}`);
          }

          const responseData = await response.text();
          console.log(`✅ Webhook ejecutado exitosamente`);
          console.log(`   Respuesta:`, responseData);

          return {
            success: true,
            status: response.status,
            response: responseData
          };
        } catch (error) {
          console.error(`❌ Error en webhook callback ${callbackId}:`, error.message);
          throw error;
        }
      };

      // CORRECCIÓN: Registrar el callback correctamente
      callbackService.registerCallback(eventType, callbackId, httpCallback);

      console.log(`✅ Callback HTTP registrado exitosamente: ${eventType} -> ${callbackId}`);

      res.status(201).json({
        success: true,
        message: 'Callback registrado exitosamente',
        eventType,
        callbackId,
        webhookUrl,
        method
      });
    } catch (error) {
      console.error('❌ Error registrando callback:', error);
      res.status(500).json({
        success: false,
        message: 'Error registrando callback',
        error: error.message
      });
    }
  },

  // Registrar callback function personalizado - VERSIÓN CORREGIDA
  registerCustomCallback: async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      const { code } = req.body;

      if (!code) {
        return res.status(400).json({
          success: false,
          message: 'Código de callback es requerido'
        });
      }

      const { callbackService } = getServices();

      console.log(`🎯 Registrando callback personalizado: ${eventType} -> ${callbackId}`);

      // CORRECCIÓN: Crear callback personalizado con mejor contexto
      const customCallback = async (data) => {
        try {
          console.log(`🎯 Ejecutando callback personalizado: ${callbackId}`);
          
          // Contexto seguro para el callback
          const context = {
            console: {
              log: (...args) => console.log(`[${callbackId}]`, ...args),
              error: (...args) => console.error(`[${callbackId}]`, ...args),
              warn: (...args) => console.warn(`[${callbackId}]`, ...args),
              info: (...args) => console.info(`[${callbackId}]`, ...args)
            },
            data,
            eventType,
            callbackId,
            timestamp: new Date().toISOString(),
            // Utilidades adicionales
            JSON: JSON,
            Date: Date
          };

          // Ejecutar el código del callback
          const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
          const callbackFn = new AsyncFunction('context', code);
          
          const result = await callbackFn(context);
          
          console.log(`✅ Callback personalizado ${callbackId} ejecutado`);
          return result;
        } catch (error) {
          console.error(`❌ Error en callback personalizado ${callbackId}:`, error);
          throw error;
        }
      };

      callbackService.registerCallback(eventType, callbackId, customCallback);

      console.log(`✅ Callback personalizado registrado: ${eventType} -> ${callbackId}`);

      res.status(201).json({
        success: true,
        message: 'Callback personalizado registrado',
        eventType,
        callbackId
      });
    } catch (error) {
      console.error('❌ Error registrando callback personalizado:', error);
      res.status(500).json({
        success: false,
        message: 'Error registrando callback personalizado',
        error: error.message
      });
    }
  },

  // Remover callback
  removeCallback: async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;

      const { callbackService } = getServices();
      callbackService.removeCallback(eventType, callbackId);

      console.log(`🗑️ Callback removido: ${eventType} -> ${callbackId}`);

      res.status(200).json({
        success: true,
        message: 'Callback removido exitosamente',
        eventType,
        callbackId
      });
    } catch (error) {
      console.error('❌ Error removiendo callback:', error);
      res.status(500).json({
        success: false,
        message: 'Error removiendo callback',
        error: error.message
      });
    }
  },

  // Probar callback manualmente - VERSIÓN MEJORADA
  testCallback: async (req, res) => {
    try {
      const { eventType, callbackId } = req.params;
      const testData = req.body || {
        taskId: 'test_task_' + Date.now(),
        result: { 
          id: 999,
          test: true,
          message: 'Test callback execution'
        },
        workerId: 'test_worker',
        queueName: 'test_queue',
        timestamp: new Date().toISOString()
      };

      const { callbackService } = getServices();

      console.log(`🧪 Probando callback: ${eventType} -> ${callbackId}`);
      console.log(`   Datos de prueba:`, JSON.stringify(testData, null, 2));

      const result = await callbackService.executeCallbacks(eventType, testData);

      console.log(`✅ Test de callback completado`);

      res.status(200).json({
        success: true,
        message: 'Callback test ejecutado',
        eventType,
        callbackId,
        testData,
        result
      });
    } catch (error) {
      console.error('❌ Error probando callback:', error);
      res.status(500).json({
        success: false,
        message: 'Error probando callback',
        error: error.message
      });
    }
  },

  // Obtener historial de callbacks
  getCallbackHistory: async (req, res) => {
    try {
      const { limit = 50, eventType } = req.query;

      const { callbackService } = getServices();
      const history = callbackService.getHistory(parseInt(limit), eventType);

      res.status(200).json({
        success: true,
        history,
        total: history.length,
        eventType: eventType || 'all'
      });
    } catch (error) {
      console.error('❌ Error obteniendo historial:', error);
      res.status(500).json({
        success: false,
        message: 'Error obteniendo historial de callbacks',
        error: error.message
      });
    }
  },

  // Obtener estadísticas de callbacks
  getCallbackStats: async (req, res) => {
    try {
      const { callbackService } = getServices();
      const stats = callbackService.getStats();

      res.status(200).json({
        success: true,
        stats
      });
    } catch (error) {
      console.error('❌ Error obteniendo estadísticas:', error);
      res.status(500).json({
        success: false,
        message: 'Error obteniendo estadísticas de callbacks',
        error: error.message
      });
    }
  },

  // Configurar callbacks en workers existentes
  setupWorkerCallbacks: async (req, res) => {
    try {
      const { workerId } = req.params;
      const { callbacks } = req.body;

      const { queueService, callbackService } = getServices();
      await queueService.initialize();

      const worker = queueService.queueManager.workers.get(workerId);
      if (!worker) {
        return res.status(404).json({
          success: false,
          message: `Worker '${workerId}' not found`
        });
      }

      // Configurar callbacks en el worker
      const workerCallbacks = {};

      if (callbacks.onTaskCompleted) {
        workerCallbacks.onTaskCompleted = async (data) => {
          await callbackService.executeCallbacks('task:completed', data);
        };
      }

      if (callbacks.onTaskFailed) {
        workerCallbacks.onTaskFailed = async (data) => {
          await callbackService.executeCallbacks('task:failed', data);
        };
      }

      if (callbacks.onTaskError) {
        workerCallbacks.onTaskError = async (data) => {
          await callbackService.executeCallbacks('task:error', data);
        };
      }

      worker.updateCallbacks(workerCallbacks);

      res.status(200).json({
        success: true,
        message: 'Callbacks configurados en worker',
        workerId,
        configuredCallbacks: Object.keys(workerCallbacks)
      });
    } catch (error) {
      console.error('❌ Error configurando callbacks en worker:', error);
      res.status(500).json({
        success: false,
        message: 'Error configurando callbacks en worker',
        error: error.message
      });
    }
  },

  // Listar todos los callbacks registrados
  listCallbacks: async (req, res) => {
    try {
      const { callbackService } = getServices();
      const callbacks = callbackService.listCallbacks();

      console.log('📋 Listando callbacks registrados:', callbacks);

      res.status(200).json({
        success: true,
        callbacks,
        totalEventTypes: Object.keys(callbacks).length,
        totalCallbacks: Object.values(callbacks).reduce((sum, arr) => sum + arr.length, 0)
      });
    } catch (error) {
      console.error('❌ Error listando callbacks:', error);
      res.status(500).json({
        success: false,
        message: 'Error listando callbacks',
        error: error.message
      });
    }
  }
};

module.exports = callbackController;