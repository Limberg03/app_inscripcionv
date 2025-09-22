// routes/trackingRoutes.js
const express = require('express');
const requestTracker = require('../services/RequestTracker');
const router = express.Router();

// ================ ESTADÍSTICAS GENERALES ================

// Obtener estadísticas completas
router.get('/stats', (req, res) => {
  try {
    const stats = requestTracker.getStats();
    res.json({
      success: true,
      data: stats,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener estadísticas',
      error: error.message
    });
  }
});

// Resumen rápido
router.get('/summary', (req, res) => {
  try {
    const stats = requestTracker.getStats();
    res.json({
      success: true,
      data: {
        totalRequests: stats.summary.totalRequests,
        syncRequests: stats.summary.syncRequests,
        asyncRequests: stats.summary.asyncRequests,
        methodDistribution: stats.methodDistribution,
        modelDistribution: stats.modelDistribution,
        performance: stats.performance,
        lastUpdate: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener resumen',
      error: error.message
    });
  }
});

// ================ DETALLES DE REQUESTS ================

// Obtener detalles de requests con filtros y paginación
router.get('/requests', (req, res) => {
  try {
    const {
      mode = 'all',
      method = 'all',
      model = 'all',
      page = 1,
      limit = 20,
      sortBy = 'timestamp',
      sortOrder = 'desc'
    } = req.query;

    const options = {
      mode,
      method,
      model,
      page: parseInt(page),
      limit: parseInt(limit),
      sortBy,
      sortOrder
    };

    const result = requestTracker.getRequestDetails(options);
    
    res.json({
      success: true,
      ...result,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener detalles de requests',
      error: error.message
    });
  }
});

// Obtener request específico por ID
router.get('/requests/:id', (req, res) => {
  try {
    const { id } = req.params;
    const allRequests = [
      ...requestTracker.requests.sync.details,
      ...requestTracker.requests.async.details
    ];
    
    const request = allRequests.find(req => req.id === id);
    
    if (!request) {
      return res.status(404).json({
        success: false,
        message: 'Request no encontrado'
      });
    }
    
    res.json({
      success: true,
      data: request
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener request',
      error: error.message
    });
  }
});

// ================ FILTROS ESPECÍFICOS ================

// Requests por modo (sync/async)
router.get('/requests/:mode', (req, res) => {
  try {
    const { mode } = req.params;
    const { page = 1, limit = 20 } = req.query;

    if (!['sync', 'async'].includes(mode)) {
      return res.status(400).json({
        success: false,
        message: 'Modo inválido. Use: sync o async'
      });
    }

    const result = requestTracker.getRequestDetails({
      mode,
      page: parseInt(page),
      limit: parseInt(limit)
    });

    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Error al obtener requests ${req.params.mode}`,
      error: error.message
    });
  }
});


// Requests por método HTTP
router.get('/requests/method/:method', (req, res) => {
  try {
    const { method } = req.params;
    const { mode = 'all', page = 1, limit = 20 } = req.query;
    
    const result = requestTracker.getRequestDetails({
      method: method.toUpperCase(),
      mode,
      page: parseInt(page),
      limit: parseInt(limit)
    });
    
    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Error al obtener requests ${method.toUpperCase()}`,
      error: error.message
    });
  }
});

// Requests por modelo
router.get('/requests/model/:model', (req, res) => {
  try {
    const { model } = req.params;
    const { mode = 'all', page = 1, limit = 20 } = req.query;
    
    const result = requestTracker.getRequestDetails({
      model,
      mode,
      page: parseInt(page),
      limit: parseInt(limit)
    });
    
    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: `Error al obtener requests del modelo ${model}`,
      error: error.message
    });
  }
});

// ================ ACTIVIDAD RECIENTE ================

// Actividad reciente (últimas 24 horas)
router.get('/recent', (req, res) => {
  try {
    const { limit = 50 } = req.query;
    const recentActivity = requestTracker.getRecentActivity();
    
    res.json({
      success: true,
      data: recentActivity.slice(0, parseInt(limit)),
      total: recentActivity.length,
      period: 'last_24_hours'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener actividad reciente',
      error: error.message
    });
  }
});

// ================ MÉTRICAS DE RENDIMIENTO ================

// Métricas de rendimiento detalladas
router.get('/performance', (req, res) => {
  try {
    const stats = requestTracker.getStats();
    const performance = stats.performance;
    
    // Agregar métricas adicionales
    const allRequests = [
      ...requestTracker.requests.sync.details,
      ...requestTracker.requests.async.details
    ];
    
    const syncRequests = requestTracker.requests.sync.details;
    const asyncRequests = requestTracker.requests.async.details;
    
    const syncAvgTime = syncRequests.length > 0 ? 
      syncRequests.reduce((sum, req) => sum + req.responseTime, 0) / syncRequests.length : 0;
    const asyncAvgTime = asyncRequests.length > 0 ? 
      asyncRequests.reduce((sum, req) => sum + req.responseTime, 0) / asyncRequests.length : 0;
    
    res.json({
      success: true,
      data: {
        ...performance,
        modeComparison: {
          sync: {
            requests: syncRequests.length,
            avgResponseTime: syncAvgTime.toFixed(2),
            successRate: syncRequests.length > 0 ? 
              ((syncRequests.filter(req => req.success).length / syncRequests.length) * 100).toFixed(2) : 0
          },
          async: {
            requests: asyncRequests.length,
            avgResponseTime: asyncAvgTime.toFixed(2),
            successRate: asyncRequests.length > 0 ? 
              ((asyncRequests.filter(req => req.success).length / asyncRequests.length) * 100).toFixed(2) : 0
          }
        },
        improvement: {
          timeSaved: syncAvgTime > asyncAvgTime ? 
            ((syncAvgTime - asyncAvgTime) * asyncRequests.length).toFixed(2) + 'ms' : '0ms',
          efficiency: syncAvgTime > 0 && asyncAvgTime > 0 ? 
            (((syncAvgTime - asyncAvgTime) / syncAvgTime) * 100).toFixed(2) + '%' : '0%'
        }
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al obtener métricas de rendimiento',
      error: error.message
    });
  }
});

// ================ GESTIÓN DE DATOS ================

// Limpiar historial
router.delete('/clear/:mode', (req, res) => {
  try {
    const mode = req.params.mode || 'all';

    if (!['all', 'sync', 'async'].includes(mode)) {
      return res.status(400).json({
        success: false,
        message: 'Modo inválido. Use: all, sync o async'
      });
    }

    requestTracker.clearHistory(mode);

    res.json({
      success: true,
      message: `Historial ${mode} limpiado exitosamente`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al limpiar historial',
      error: error.message
    });
  }
});

// Resetear contadores
router.post('/reset/:mode', (req, res) => {
  try {
    const { mode = 'all' } = req.params;
    
    if (mode !== 'all' && mode !== 'sync' && mode !== 'async') {
      return res.status(400).json({
        success: false,
        message: 'Modo inválido. Use: all, sync, o async'
      });
    }
    
    requestTracker.resetCounters(mode);
    
    res.json({
      success: true,
      message: `Contadores ${mode} reseteados exitosamente`,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al resetear contadores',
      error: error.message
    });
  }
});

// ================ EXPORTACIÓN DE DATOS ================

// Exportar datos en JSON
router.get('/export/json', (req, res) => {
  try {
    const data = requestTracker.exportData('json');
    
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="request_tracking_${Date.now()}.json"`);
    res.json(data);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al exportar datos JSON',
      error: error.message
    });
  }
});

// Exportar datos en CSV
router.get('/export/csv', (req, res) => {
  try {
    const csvData = requestTracker.exportData('csv');
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="request_tracking_${Date.now()}.csv"`);
    res.send(csvData);
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al exportar datos CSV',
      error: error.message
    });
  }
});

// ================ WEBSOCKET PARA TIEMPO REAL ================

// Endpoint para callbacks en tiempo real (webhooks)
router.post('/webhook/register', (req, res) => {
  try {
    const { callbackId, description } = req.body;
    
    if (!callbackId) {
      return res.status(400).json({
        success: false,
        message: 'callbackId es requerido'
      });
    }
    
    // Registrar callback básico (en implementación real usarías webhooks)
    const callback = (data) => {
      console.log(`Callback ${callbackId}:`, {
        description,
        timestamp: new Date().toISOString(),
        data: {
          type: data.type,
          requestId: data.data?.id,
          method: data.data?.method,
          url: data.data?.url,
          mode: data.data?.mode
        }
      });
    };
    
    requestTracker.registerCallback(callbackId, callback);
    
    res.json({
      success: true,
      message: 'Callback registrado exitosamente',
      callbackId,
      description
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al registrar callback',
      error: error.message
    });
  }
});

// Remover callback
router.delete('/webhook/:callbackId', (req, res) => {
  try {
    const { callbackId } = req.params;
    requestTracker.removeCallback(callbackId);
    
    res.json({
      success: true,
      message: 'Callback removido exitosamente',
      callbackId
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error al remover callback',
      error: error.message
    });
  }
});

// ================ INFORMACIÓN DE LA API ================

// Documentación de endpoints
router.get('/docs', (req, res) => {
  res.json({
    success: true,
    documentation: {
      title: "Request Tracking API",
      version: "1.0.0",
      description: "API para tracking y monitoreo de peticiones HTTP síncronas y asíncronas",
      baseUrl: "/tracking",
      endpoints: {
        statistics: {
          "/stats": {
            method: "GET",
            description: "Estadísticas completas del sistema",
            parameters: "Ninguno"
          },
          "/summary": {
            method: "GET",
            description: "Resumen rápido de métricas",
            parameters: "Ninguno"
          },
          "/performance": {
            method: "GET",
            description: "Métricas detalladas de rendimiento",
            parameters: "Ninguno"
          }
        },
        requests: {
          "/requests": {
            method: "GET",
            description: "Lista paginada de requests con filtros",
            parameters: {
              mode: "all|sync|async",
              method: "all|GET|POST|PUT|PATCH|DELETE",
              model: "nombre_del_modelo",
              page: "número_de_página",
              limit: "elementos_por_página",
              sortBy: "timestamp|responseTime|statusCode",
              sortOrder: "asc|desc"
            }
          },
          "/requests/:id": {
            method: "GET",
            description: "Detalles de un request específico",
            parameters: "id: ID del request"
          },
          "/requests/:mode": {
            method: "GET",
            description: "Requests filtrados por modo (sync/async)",
            parameters: "mode: sync|async"
          },
          "/requests/method/:method": {
            method: "GET",
            description: "Requests filtrados por método HTTP",
            parameters: "method: GET|POST|PUT|PATCH|DELETE"
          },
          "/requests/model/:model": {
            method: "GET",
            description: "Requests filtrados por modelo",
            parameters: "model: nombre_del_modelo"
          }
        },
        recent: {
          "/recent": {
            method: "GET",
            description: "Actividad reciente (últimas 24 horas)",
            parameters: {
              limit: "número_máximo_de_elementos"
            }
          }
        },
        management: {
          "/clear/:mode": {
            method: "DELETE",
            description: "Limpiar historial de requests",
            parameters: "mode: all|sync|async (opcional)"
          },
          "/reset/:mode": {
            method: "POST",
            description: "Resetear contadores",
            parameters: "mode: all|sync|async (opcional)"
          }
        },
        export: {
          "/export/json": {
            method: "GET",
            description: "Exportar datos en formato JSON",
            response: "Archivo JSON para descarga"
          },
          "/export/csv": {
            method: "GET",
            description: "Exportar datos en formato CSV",
            response: "Archivo CSV para descarga"
          }
        },
        webhooks: {
          "/webhook/register": {
            method: "POST",
            description: "Registrar callback para notificaciones en tiempo real",
            body: {
              callbackId: "identificador_único",
              description: "descripción_del_callback"
            }
          },
          "/webhook/:callbackId": {
            method: "DELETE",
            description: "Remover callback registrado",
            parameters: "callbackId: ID del callback"
          }
        }
      },
      examples: {
        sync_request: {
          url: "/sync/aulas",
          method: "POST",
          body: {
            nombre: "Laboratorio de Computación",
            capacidad: 25,
            estado: true
          }
        },
        async_request: {
          url: "/aulas",
          method: "POST",
          body: {
            nombre: "Laboratorio de Computación",
            capacidad: 25,
            estado: true
          }
        }
      }
    }
  });
});

// Health check específico para tracking
router.get('/health', (req, res) => {
  const stats = requestTracker.getStats();
  res.json({
    success: true,
    service: "Request Tracking",
    status: "operational",
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    stats: {
      totalRequests: stats.summary.totalRequests,
      activeCallbacks: requestTracker.callbacks.size
    }
  });
});

module.exports = router;