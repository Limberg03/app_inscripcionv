// controllers/aula.controller.js - LOCKLESS ARCHITECTURE
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
const cluster = require('cluster');

// Importar modelos
let Aula, Horario;
try {
  ({ Aula, Horario } = require('../models'));
} catch (error) {
  const models = require('../models');
  Aula = models.Aula;
  Horario = models.Horario;
}

// Sistema sin bloqueos - Queue-based architecture
class LocklessPool {
  constructor() {
    this.workerId = cluster.worker ? cluster.worker.id : 'main';
    this.processing = false;
    this.queue = [];
    this.maxQueue = 2000; // Queue masiva
    this.stats = {
      processed: 0,
      dropped: 0,
      avgTime: 0,
      maxTime: 0
    };
    
    // Procesador continuo sin bloqueos
    this.startProcessor();
  }
  
  async execute(operation, timeout = 10000) {
    return new Promise((resolve, reject) => {
      const start = Date.now();
      
      // Si la queue está saturada, rechazar inmediatamente
      if (this.queue.length >= this.maxQueue) {
        this.stats.dropped++;
        reject(new Error('QUEUE_FULL'));
        return;
      }
      
      // Agregar a la queue
      this.queue.push({
        operation,
        resolve,
        reject,
        timestamp: start,
        timeout: setTimeout(() => {
          reject(new Error('OPERATION_TIMEOUT'));
        }, timeout)
      });
    });
  }
  
  startProcessor() {
    const processNext = async () => {
      if (this.queue.length === 0) {
        setImmediate(processNext);
        return;
      }
      
      // Procesar múltiples operaciones en paralelo
      const batchSize = Math.min(50, this.queue.length); // Batch grande
      const batch = this.queue.splice(0, batchSize);
      
      // Ejecutar todas en paralelo sin esperar
      batch.forEach(async (item) => {
        const { operation, resolve, reject, timestamp, timeout: timeoutId } = item;
        
        try {
          clearTimeout(timeoutId);
          const start = Date.now();
          const result = await operation();
          const elapsed = Date.now() - start;
          
          this.stats.processed++;
          this.stats.avgTime = (this.stats.avgTime + elapsed) / 2;
          this.stats.maxTime = Math.max(this.stats.maxTime, elapsed);
          
          resolve(result);
        } catch (error) {
          clearTimeout(timeoutId);
          reject(error);
        }
      });
      
      // Continuar inmediatamente sin pausa
      setImmediate(processNext);
    };
    
    // Iniciar el procesador
    processNext();
  }
  
  getStats() {
    return {
      workerId: this.workerId,
      queueSize: this.queue.length,
      maxQueue: this.maxQueue,
      processed: this.stats.processed,
      dropped: this.stats.dropped,
      avgTime: Math.round(this.stats.avgTime),
      maxTime: this.stats.maxTime,
      utilization: ((this.queue.length / this.maxQueue) * 100).toFixed(1) + '%'
    };
  }
}

// Cache ultra rápido con pre-loading
class InstantCache {
  constructor() {
    this.cache = new Map();
    this.preloadCache = new Map();
    this.hits = 0;
    this.misses = 0;
    this.maxSize = 2000;
    this.ttl = 60000; // 1 minuto
    
    // Pre-load común cada segundo
    setInterval(() => this.preloadCommon(), 1000);
  }
  
  get(key) {
    // Primero chequear cache principal
    let item = this.cache.get(key);
    if (item && Date.now() - item.time < this.ttl) {
      this.hits++;
      return item.data;
    }
    
    // Chequear pre-load cache
    item = this.preloadCache.get(key);
    if (item && Date.now() - item.time < this.ttl) {
      this.hits++;
      // Mover a cache principal
      this.cache.set(key, item);
      return item.data;
    }
    
    this.misses++;
    return null;
  }
  
  set(key, data) {
    const now = Date.now();
    
    // Cleanup periódico muy eficiente
    if (this.cache.size >= this.maxSize) {
      const entries = Array.from(this.cache.entries());
      const cutoff = now - this.ttl;
      
      // Remover entradas viejas
      let removed = 0;
      for (const [k, v] of entries) {
        if (v.time < cutoff) {
          this.cache.delete(k);
          removed++;
          if (removed > 100) break; // Limite por ciclo
        }
      }
      
      // Si aún está lleno, remover las más viejas
      if (this.cache.size >= this.maxSize) {
        const sorted = entries.sort((a, b) => a[1].time - b[1].time);
        for (let i = 0; i < 100 && this.cache.size >= this.maxSize; i++) {
          this.cache.delete(sorted[i][0]);
        }
      }
    }
    
    this.cache.set(key, { data, time: now });
  }
  
  preloadCommon() {
    // Pre-cargar aulas más consultadas
    const commonKeys = [
      'aulas_1_10',
      'aulas_1_20',
      'aulas_2_10',
      'aulas_available',
      'aula_stats'
    ];
    
    commonKeys.forEach(key => {
      if (!this.cache.has(key) && !this.preloadCache.has(key)) {
        setImmediate(async () => {
          try {
            // Simular pre-carga (en tu caso sería una query real)
            const fakeData = {
              success: true,
              data: [],
              preloaded: true,
              timestamp: Date.now()
            };
            this.preloadCache.set(key, { data: fakeData, time: Date.now() });
          } catch (error) {
            // Ignorar errores de pre-carga
          }
        });
      }
    });
  }
  
  invalidate(pattern) {
    // Invalidación ultra rápida
    const toDelete = [];
    for (const [key] of this.cache) {
      if (key.includes(pattern)) {
        toDelete.push(key);
      }
    }
    for (const [key] of this.preloadCache) {
      if (key.includes(pattern)) {
        toDelete.push(key);
      }
    }
    
    // Eliminar async
    setImmediate(() => {
      toDelete.forEach(key => {
        this.cache.delete(key);
        this.preloadCache.delete(key);
      });
    });
  }
  
  getStats() {
    const total = this.hits + this.misses;
    return {
      size: this.cache.size,
      preloadSize: this.preloadCache.size,
      maxSize: this.maxSize,
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? (this.hits / total * 100).toFixed(1) + '%' : '0%'
    };
  }
}

// Instancias globales
const pool = new LocklessPool();
const cache = new InstantCache();

// Helpers ultra rápidos
const fastResponse = (res, status, data, time) => {
  if (res.headersSent) return;
  
  res.status(status).json({
    ...data,
    meta: {
      workerId: cluster.worker?.id || 'main',
      time: time + 'ms',
      timestamp: Date.now()
    }
  });
};

// Operaciones de base de datos optimizadas
const dbOperations = {
  async findAll(page, limit, filters = {}) {
    const where = {};
    
    // Filtros por capacidad
    if (filters.capacidadMin || filters.capacidadMax) {
      where.capacidad = {};
      if (filters.capacidadMin) where.capacidad[Op.gte] = parseInt(filters.capacidadMin);
      if (filters.capacidadMax) where.capacidad[Op.lte] = parseInt(filters.capacidadMax);
    }
    
    // Filtro por estado
    if (filters.estado !== undefined) {
      where.estado = filters.estado === 'true';
    }
    
    return await Aula.findAndCountAll({
      where,
      order: [['nombre', 'ASC']],
      limit: limit,
      offset: (page - 1) * limit,
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async findById(id) {
    return await Aula.findByPk(id, {
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async create(data) {
    return await Aula.create(data, {
      benchmark: false,
      logging: false
    });
  },
  
  async findByName(nombre) {
    return await Aula.findOne({
      where: {
        nombre: {
          [Op.iLike]: `%${nombre}%`
        }
      },
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async findAvailable(capacidadMin) {
    const where = { estado: true };
    if (capacidadMin) {
      where.capacidad = { [Op.gte]: parseInt(capacidadMin) };
    }
    
    return await Aula.findAll({
      where,
      order: [['capacidad', 'ASC'], ['nombre', 'ASC']],
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async searchAulas(searchTerm, page, limit) {
    return await Aula.findAndCountAll({
      where: {
        [Op.or]: [
          { nombre: { [Op.iLike]: `%${searchTerm}%` } },
          { capacidad: isNaN(parseInt(searchTerm)) ? null : parseInt(searchTerm) }
        ].filter(Boolean)
      },
      order: [['nombre', 'ASC']],
      limit: limit,
      offset: (page - 1) * limit,
      benchmark: false,
      logging: false
    });
  }
};

const aulaController = {
  // GET ALL - Sin bloqueos con filtros avanzados
  getAll: async (req, res) => {
    const start = Date.now();
    
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = Math.min(parseInt(req.query.limit) || 10, 50);
      const search = req.query.search;
      const { capacidadMin, capacidadMax, estado } = req.query;
      
      let cacheKey, result;
      
      if (search) {
        cacheKey = `aulas_search_${search}_${page}_${limit}`;
        const cached = cache.get(cacheKey);
        if (cached) {
          return fastResponse(res, 200, { 
            ...cached, 
            fromCache: true 
          }, Date.now() - start);
        }
        
        result = await pool.execute(async () => {
          return await dbOperations.searchAulas(search, page, limit);
        });
      } else {
        cacheKey = `aulas_${page}_${limit}_${capacidadMin || ''}_${capacidadMax || ''}_${estado || ''}`;
        const cached = cache.get(cacheKey);
        if (cached) {
          return fastResponse(res, 200, { 
            ...cached, 
            fromCache: true 
          }, Date.now() - start);
        }
        
        result = await pool.execute(async () => {
          return await dbOperations.findAll(page, limit, { capacidadMin, capacidadMax, estado });
        });
      }
      
      const data = {
        success: true,
        data: result.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(result.count / limit),
          totalItems: result.count,
          itemsPerPage: limit
        },
        filters: { capacidadMin, capacidadMax, estado },
        search: search || null
      };
      
      // Cache async
      cache.set(cacheKey, data);
      
      fastResponse(res, 200, data, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message === 'QUEUE_FULL') {
        return fastResponse(res, 503, {
          success: false,
          message: 'Server overloaded',
          code: 'QUEUE_FULL'
        }, elapsed);
      }
      
      if (error.message === 'OPERATION_TIMEOUT') {
        return fastResponse(res, 408, {
          success: false,
          message: 'Operation timeout',
          code: 'TIMEOUT'
        }, elapsed);
      }
      
      console.error('Error en getAll aulas:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener aulas'
      }, elapsed);
    }
  },
  
  // GET BY ID - Sin bloqueos
  getById: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      const cacheKey = `aula_${id}`;
      
      const cached = cache.get(cacheKey);
      if (cached) {
        return fastResponse(res, 200, { 
          ...cached, 
          fromCache: true 
        }, Date.now() - start);
      }
      
      const aula = await pool.execute(async () => {
        return await dbOperations.findById(id);
      });
      
      if (!aula) {
        return fastResponse(res, 404, {
          success: false,
          message: 'Aula no encontrada'
        }, Date.now() - start);
      }
      
      const data = { 
        success: true, 
        data: aula
      };
      
      cache.set(cacheKey, data);
      
      fastResponse(res, 200, data, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      console.error('Error en getById aula:', error);
      
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener aula'
      }, elapsed);
    }
  },
  
  // CREATE - Sin bloqueos
  create: async (req, res) => {
    const start = Date.now();
    
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return fastResponse(res, 400, {
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        }, Date.now() - start);
      }
      
      const { nombre, capacidad, estado } = req.body;
      
      const result = await pool.execute(async () => {
        // Verificar si ya existe un aula con ese nombre
        const existing = await dbOperations.findByName(nombre);
        if (existing) {
          throw new Error('Ya existe un aula con este nombre');
        }
        
        // Crear el aula
        return await dbOperations.create({
          nombre,
          capacidad,
          estado: estado !== undefined ? estado : true
        });
      });
      
      // Invalidar cache async
      cache.invalidate('aulas_');
      
      fastResponse(res, 201, {
        success: true,
        message: 'Aula creada exitosamente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('nombre')) {
        return fastResponse(res, 400, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.name === 'SequelizeUniqueConstraintError') {
        return fastResponse(res, 400, {
          success: false,
          message: 'El código de aula ya existe'
        }, elapsed);
      }
      
      if (error.message === 'QUEUE_FULL') {
        return fastResponse(res, 503, {
          success: false,
          message: 'Server overloaded',
          code: 'QUEUE_FULL'
        }, elapsed);
      }
      
      console.error('Error en create aula:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al crear aula'
      }, elapsed);
    }
  },
  
  // UPDATE - Sin bloqueos
  update: async (req, res) => {
    const start = Date.now();
    
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return fastResponse(res, 400, {
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        }, Date.now() - start);
      }
      
      const { id } = req.params;
      const { nombre, capacidad, estado } = req.body;
      
      const result = await pool.execute(async () => {
        const aula = await Aula.findByPk(id);
        
        if (!aula) {
          throw new Error('Aula no encontrada');
        }
        
        // Verificar nombre duplicado si cambió
        if (nombre && nombre !== aula.nombre) {
          const existing = await dbOperations.findByName(nombre);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un aula con este nombre');
          }
        }
        
        return await aula.update({ nombre, capacidad, estado });
      });
      
      cache.invalidate('aula');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Aula actualizada exitosamente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrada')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.message.includes('nombre') || error.name === 'SequelizeUniqueConstraintError') {
        return fastResponse(res, 400, {
          success: false,
          message: error.message.includes('nombre') ? error.message : 'El código de aula ya existe'
        }, elapsed);
      }
      
      console.error('Error en update aula:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar aula'
      }, elapsed);
    }
  },
  
  // PATCH - Sin bloqueos (actualización parcial)
  patch: async (req, res) => {
    const start = Date.now();
    
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return fastResponse(res, 400, {
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        }, Date.now() - start);
      }
      
      const { id } = req.params;
      const updateData = req.body;
      
      const result = await pool.execute(async () => {
        const aula = await Aula.findByPk(id);
        
        if (!aula) {
          throw new Error('Aula no encontrada');
        }
        
        // Verificar nombre duplicado si se está actualizando
        if (updateData.nombre && updateData.nombre !== aula.nombre) {
          const existing = await dbOperations.findByName(updateData.nombre);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un aula con este nombre');
          }
        }
        
        return await aula.update(updateData);
      });
      
      cache.invalidate('aula');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Aula actualizada parcialmente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrada')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.name === 'SequelizeUniqueConstraintError') {
        return fastResponse(res, 400, {
          success: false,
          message: 'El código de aula ya existe'
        }, elapsed);
      }
      
      console.error('Error en patch aula:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar aula'
      }, elapsed);
    }
  },
  
  // DELETE - Sin bloqueos
  delete: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      
      await pool.execute(async () => {
        const aula = await Aula.findByPk(id);
        
        if (!aula) {
          throw new Error('Aula no encontrada');
        }
        
        // Aquí podrías verificar si tiene horarios/clases asignadas si es necesario
        // const hasSchedules = await Horario.findOne({ where: { aulaId: id } });
        // if (hasSchedules) {
        //   throw new Error('No se puede eliminar: el aula tiene horarios asignados');
        // }
        
        await aula.destroy();
        return true;
      });
      
      cache.invalidate('aula');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Aula eliminada exitosamente'
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrada')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en delete aula:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al eliminar aula'
      }, elapsed);
    }
  },
  
  // GET AVAILABLE - Sin bloqueos (aulas disponibles)
  getAvailable: async (req, res) => {
    const start = Date.now();
    
    try {
      const { capacidadMin } = req.query;
      const cacheKey = `aulas_available_${capacidadMin || 'all'}`;
      
      const cached = cache.get(cacheKey);
      if (cached) {
        return fastResponse(res, 200, { 
          ...cached, 
          fromCache: true 
        }, Date.now() - start);
      }
      
      const aulas = await pool.execute(async () => {
        return await dbOperations.findAvailable(capacidadMin);
      });
      
      const data = {
        success: true,
        data: aulas
      };
      
      cache.set(cacheKey, data);
      
      fastResponse(res, 200, data, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      console.error('Error en getAvailable aulas:', error);
      
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener aulas disponibles'
      }, elapsed);
    }
  },
  
  // STATS completas
  getStats: async (req, res) => {
    const memUsage = process.memoryUsage();
    
    const stats = {
      success: true,
      timestamp: new Date().toISOString(),
      
      queue: pool.getStats(),
      cache: cache.getStats(),
      
      system: {
        memory: {
          used: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
          total: Math.round(memUsage.heapTotal / 1024 / 1024) + 'MB',
          external: Math.round(memUsage.external / 1024 / 1024) + 'MB'
        },
        uptime: Math.floor(process.uptime()) + 's',
        nodeVersion: process.version,
        workerId: cluster.worker?.id || 'main',
        pid: process.pid
      },
      
      performance: {
        activeHandles: process._getActiveHandles ? process._getActiveHandles().length : 0,
        activeRequests: process._getActiveRequests ? process._getActiveRequests().length : 0
      }
    };
    
    res.status(200).json(stats);
  }
};

module.exports = aulaController;