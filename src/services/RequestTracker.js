// services/RequestTracker.js
class RequestTracker {
  constructor() {
    this.requests = {
      sync: {
        total: 0,
        GET: 0,
        POST: 0,
        PUT: 0,
        PATCH: 0,
        DELETE: 0,
        details: []
      },
      async: {
        total: 0,
        GET: 0,
        POST: 0,
        PUT: 0,
        PATCH: 0,
        DELETE: 0,
        details: []
      }
    };
    this.callbacks = new Map();
    
    // ✅ NUEVO: Configuración ajustable para límites
    this.config = {
      maxHistoryPerMode: 1000000, // Aumentado de 1000 a 100,000
      maxRecentActivityHours: 24,
      enableAutoCleanup: true,
      cleanupThreshold: 1100000 // Limpiar cuando exceda este número
    };
  }

  // ✅ NUEVO: Método para actualizar configuración
  updateConfig(newConfig) {
    this.config = { ...this.config, ...newConfig };
  }

  // ✅ NUEVO: Método de limpieza automática
  autoCleanup(mode) {
    const modeData = this.requests[mode];
    if (modeData.details.length > this.config.cleanupThreshold) {
      // Mantener solo los más recientes hasta el límite máximo
      modeData.details = modeData.details
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, this.config.maxHistoryPerMode);
      
      console.log(`🧹 Auto-cleanup ejecutado para modo ${mode}. Mantenidos ${modeData.details.length} elementos.`);
    }
  }

  // Registrar callback para notificaciones en tiempo real
  registerCallback(id, callback) {
    this.callbacks.set(id, callback);
  }

  // Remover callback
  removeCallback(id) {
    this.callbacks.delete(id);
  }

  // Notificar a todos los callbacks registrados
  notifyCallbacks(data) {
    this.callbacks.forEach(callback => {
      try {
        callback(data);
      } catch (error) {
        console.error('Error en callback:', error);
      }
    });
  }

  // Registrar una nueva petición
  trackRequest(requestData) {
    const { 
      url, 
      method, 
      body = null, 
      query = {}, 
      headers = {}, 
      userAgent = null,
      ip = null,
      responseTime = 0,
      statusCode = null,
      responseSize = 0,
      error = null
    } = requestData;

    // Determinar si es síncrono o asíncrono basado en la URL
    const isSync = url.startsWith('/sync/');
    const mode = isSync ? 'sync' : 'async';

    // Crear objeto de request detallado
    const requestDetail = {
      id: this.generateRequestId(),
      timestamp: new Date().toISOString(),
      url,
      method: method.toUpperCase(),
      mode: mode.toUpperCase(),
      body: this.sanitizeBody(body),
      query,
      headers: this.sanitizeHeaders(headers),
      userAgent,
      ip,
      responseTime,
      statusCode,
      responseSize,
      error,
      success: !error && statusCode >= 200 && statusCode < 400,
      model: this.extractModelFromUrl(url)
    };

    // Incrementar contadores
    this.requests[mode].total++;
    this.requests[mode][method.toUpperCase()]++;
    this.requests[mode].details.unshift(requestDetail); // Más recientes primero

    // ✅ ACTUALIZADO: Limitar historial al nuevo límite configurable
    if (this.requests[mode].details.length > this.config.maxHistoryPerMode) {
      if (this.config.enableAutoCleanup) {
        this.autoCleanup(mode);
      } else {
        // Método anterior: corte simple
        this.requests[mode].details = this.requests[mode].details.slice(0, this.config.maxHistoryPerMode);
      }
    }

    // Notificar a callbacks en tiempo real
    this.notifyCallbacks({
      type: 'new_request',
      data: requestDetail,
      stats: this.getStats()
    });

    return requestDetail;
  }

  // Generar ID único para request
  generateRequestId() {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  // Sanitizar body para logging
  sanitizeBody(body) {
    if (!body) return null;
    
    const sanitized = { ...body };
    
    // Remover campos sensibles
    const sensitiveFields = ['password', 'token', 'secret', 'key'];
    sensitiveFields.forEach(field => {
      if (sanitized[field]) {
        sanitized[field] = '[HIDDEN]';
      }
    });
    
    return sanitized;
  }

  // Sanitizar headers
  sanitizeHeaders(headers) {
    const sanitized = { ...headers };
    
    // Remover headers sensibles
    const sensitiveHeaders = ['authorization', 'cookie', 'x-api-key'];
    sensitiveHeaders.forEach(header => {
      if (sanitized[header]) {
        sanitized[header] = '[HIDDEN]';
      }
    });
    
    return sanitized;
  }

  // Extraer modelo de la URL
  extractModelFromUrl(url) {
    // Remover prefijo /sync/ si existe
    const cleanUrl = url.replace(/^\/sync\//, '/');
    
    // Extraer el primer segmento después del /
     const match = cleanUrl.match(/^\/([^/?]+)/);
    
    return match ? match[1] : 'unknown';
  }

  // ✅ NUEVO: Obtener información de configuración actual
  getConfigInfo() {
    return {
      ...this.config,
      currentCounts: {
        sync: this.requests.sync.details.length,
        async: this.requests.async.details.length,
        total: this.requests.sync.details.length + this.requests.async.details.length
      },
      memoryUsage: {
        syncMB: (JSON.stringify(this.requests.sync.details).length / 1024 / 1024).toFixed(2),
        asyncMB: (JSON.stringify(this.requests.async.details).length / 1024 / 1024).toFixed(2)
      }
    };
  }

  // Obtener estadísticas completas
  getStats() {
    const totalRequests = this.requests.sync.total + this.requests.async.total;
    
    return {
      summary: {
        totalRequests,
        syncRequests: this.requests.sync.total,
        asyncRequests: this.requests.async.total,
        syncPercentage: totalRequests > 0 ? ((this.requests.sync.total / totalRequests) * 100).toFixed(2) : 0,
        asyncPercentage: totalRequests > 0 ? ((this.requests.async.total / totalRequests) * 100).toFixed(2) : 0,
        // ✅ NUEVO: Información adicional
        historySyncCount: this.requests.sync.details.length,
        historyAsyncCount: this.requests.async.details.length,
        maxHistoryPerMode: this.config.maxHistoryPerMode
      },
      sync: {
        ...this.requests.sync,
        details: undefined // No incluir detalles en stats básicas
      },
      async: {
        ...this.requests.async,
        details: undefined
      },
      methodDistribution: this.getMethodDistribution(),
      modelDistribution: this.getModelDistribution(),
      recentActivity: this.getRecentActivity(),
      performance: this.getPerformanceMetrics(),
      // ✅ NUEVO: Información de configuración
      config: this.getConfigInfo()
    };
  }

  // Distribución por método HTTP
  getMethodDistribution() {
    const methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'];
    const distribution = {};
    
    methods.forEach(method => {
      distribution[method] = {
        sync: this.requests.sync[method],
        async: this.requests.async[method],
        total: this.requests.sync[method] + this.requests.async[method]
      };
    });
    
    return distribution;
  }

  // Distribución por modelo
  getModelDistribution() {
    const models = {};
    
    ['sync', 'async'].forEach(mode => {
      this.requests[mode].details.forEach(req => {
        if (!models[req.model]) {
          models[req.model] = { sync: 0, async: 0, total: 0 };
        }
        models[req.model][mode]++;
        models[req.model].total++;
      });
    });
    
    return models;
  }

  // Actividad reciente (últimas 24 horas)
  getRecentActivity() {
    const recentHours = this.config.maxRecentActivityHours;
    const cutoffTime = new Date(Date.now() - recentHours * 60 * 60 * 1000);
    const recentRequests = [];
    
    ['sync', 'async'].forEach(mode => {
      this.requests[mode].details.forEach(req => {
        if (new Date(req.timestamp) > cutoffTime) {
          recentRequests.push(req);
        }
      });
    });
    
    return recentRequests.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  }

  // Métricas de rendimiento
  getPerformanceMetrics() {
    const allRequests = [
      ...this.requests.sync.details,
      ...this.requests.async.details
    ];
    
    if (allRequests.length === 0) {
      return { avgResponseTime: 0, minResponseTime: 0, maxResponseTime: 0 };
    }
    
    const responseTimes = allRequests.map(req => req.responseTime).filter(time => time > 0);
    
    return {
      avgResponseTime: responseTimes.length > 0 ? 
        (responseTimes.reduce((sum, time) => sum + time, 0) / responseTimes.length).toFixed(2) : 0,
      minResponseTime: Math.min(...responseTimes),
      maxResponseTime: Math.max(...responseTimes),
      totalRequests: allRequests.length,
      successRate: ((allRequests.filter(req => req.success).length / allRequests.length) * 100).toFixed(2)
    };
  }

  // ✅ MEJORADO: Obtener detalles paginados con mejor manejo de memoria
  getRequestDetails(options = {}) {
    const {
      mode = 'all', // 'sync', 'async', 'all'
      method = 'all', // 'GET', 'POST', etc.
      model = 'all', // nombre del modelo
      page = 1,
      limit = 50,
      sortBy = 'timestamp',
      sortOrder = 'desc'
    } = options;

    let allDetails = [];
    
    if (mode === 'all') {
      allDetails = [...this.requests.sync.details, ...this.requests.async.details];
    } else if (mode === 'sync') {
      allDetails = [...this.requests.sync.details];
    } else if (mode === 'async') {
      allDetails = [...this.requests.async.details];
    }

    // Filtrar por método
    if (method !== 'all') {
      allDetails = allDetails.filter(req => req.method === method.toUpperCase());
    }

    // Filtrar por modelo
    if (model !== 'all') {
      allDetails = allDetails.filter(req => req.model === model);
    }

    // ✅ MEJORADO: Ordenar más eficientemente
    allDetails.sort((a, b) => {
      let aValue = a[sortBy];
      let bValue = b[sortBy];
      
      if (sortBy === 'timestamp') {
        aValue = new Date(aValue);
        bValue = new Date(bValue);
      }
      
      if (sortOrder === 'desc') {
        return bValue > aValue ? 1 : -1;
      } else {
        return aValue > bValue ? 1 : -1;
      }
    });

    // Paginar
    const startIndex = (page - 1) * limit;
    const endIndex = startIndex + limit;
    const paginatedDetails = allDetails.slice(startIndex, endIndex);

    return {
      data: paginatedDetails,
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(allDetails.length / limit),
        totalItems: allDetails.length, // ✅ Ahora mostrará hasta 100,000
        itemsPerPage: limit,
        hasNextPage: endIndex < allDetails.length,
        hasPreviousPage: page > 1
      },
      filters: {
        mode,
        method,
        model,
        sortBy,
        sortOrder
      },
      // ✅ NUEVO: Información adicional para debugging
      debug: {
        configMaxHistory: this.config.maxHistoryPerMode,
        actualSyncCount: this.requests.sync.details.length,
        actualAsyncCount: this.requests.async.details.length,
        filteredCount: allDetails.length
      }
    };
  }

  // Limpiar historial
  clearHistory(mode = 'all') {
    if (mode === 'all') {
      this.requests.sync.details = [];
      this.requests.async.details = [];
    } else if (this.requests[mode]) {
      this.requests[mode].details = [];
    }
    
    this.notifyCallbacks({
      type: 'history_cleared',
      mode,
      timestamp: new Date().toISOString()
    });
  }

  // Resetear contadores
  resetCounters(mode = 'all') {
    const methods = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'];
    
    if (mode === 'all') {
      ['sync', 'async'].forEach(m => {
        this.requests[m].total = 0;
        methods.forEach(method => {
          this.requests[m][method] = 0;
        });
      });
    } else if (this.requests[mode]) {
      this.requests[mode].total = 0;
      methods.forEach(method => {
        this.requests[mode][method] = 0;
      });
    }
    
    this.notifyCallbacks({
      type: 'counters_reset',
      mode,
      timestamp: new Date().toISOString()
    });
  }

  // ✅ NUEVO: Método para optimizar memoria manualmente
  optimizeMemory(options = {}) {
    const {
      mode = 'all',
      keepRecentHours = 24,
      maxPerMode = this.config.maxHistoryPerMode
    } = options;

    const cutoffTime = new Date(Date.now() - keepRecentHours * 60 * 60 * 1000);
    let optimized = { sync: 0, async: 0 };

    const optimizeMode = (modeName) => {
      const originalCount = this.requests[modeName].details.length;
      
      // Mantener solo elementos recientes y hasta el límite
      this.requests[modeName].details = this.requests[modeName].details
        .filter(req => new Date(req.timestamp) > cutoffTime || req.important) // Mantener importantes
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp))
        .slice(0, maxPerMode);
      
      optimized[modeName] = originalCount - this.requests[modeName].details.length;
    };

    if (mode === 'all') {
      optimizeMode('sync');
      optimizeMode('async');
    } else if (this.requests[mode]) {
      optimizeMode(mode);
    }

    return {
      success: true,
      optimized,
      newCounts: {
        sync: this.requests.sync.details.length,
        async: this.requests.async.details.length
      }
    };
  }

  // Exportar datos
  exportData(format = 'json') {
    const data = {
      exportTimestamp: new Date().toISOString(),
      config: this.config,
      stats: this.getStats(),
      requests: {
        sync: this.requests.sync.details,
        async: this.requests.async.details
      }
    };

    if (format === 'csv') {
      return this.convertToCSV(data.requests);
    }
    
    return data;
  }

  // Convertir a CSV
  convertToCSV(requests) {
    const allRequests = [...requests.sync, ...requests.async];
    
    if (allRequests.length === 0) {
      return 'No hay datos para exportar';
    }

    const headers = [
      'ID', 'Timestamp', 'Mode', 'Method', 'URL', 'Model', 
      'Status Code', 'Response Time (ms)', 'Success', 'Error'
    ];
    
    const csvRows = [headers.join(',')];
    
    allRequests.forEach(req => {
      const row = [
        req.id,
        req.timestamp,
        req.mode,
        req.method,
        `"${req.url}"`,
        req.model,
        req.statusCode || 'N/A',
        req.responseTime,
        req.success,
        req.error ? `"${req.error}"` : 'N/A'
      ];
      csvRows.push(row.join(','));
    });
    
    return csvRows.join('\n');
  }
}

// Singleton instance
const requestTracker = new RequestTracker();

module.exports = requestTracker;