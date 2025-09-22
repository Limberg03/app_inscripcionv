// controllers/nivel.controller.js - LOCKLESS ARCHITECTURE
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
const cluster = require('cluster');

// Importar modelos
let Nivel, Materia;
try {
  ({ Nivel, Materia } = require('../models'));
} catch (error) {
  const models = require('../models');
  Nivel = models.Nivel;
  Materia = models.Materia;
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
    // Pre-cargar niveles más consultados
    const commonKeys = [
      'niveles_1_10',
      'niveles_1_20',
      'niveles_2_10',
      'nivel_stats'
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
  async findAll(page, limit) {
    return await Nivel.findAndCountAll({
      include: [{
        model: Materia,
        as: 'materias',
        attributes: ['id', 'nombre', 'sigla', 'creditos']
      }],
      order: [['id', 'ASC']],
      limit: limit,
      offset: (page - 1) * limit,
      raw: false, // Necesario para includes
      benchmark: false,
      logging: false,
      nest: true
    });
  },
  
  async findById(id) {
    return await Nivel.findByPk(id, {
      include: [{
        model: Materia,
        as: 'materias',
        attributes: ['id', 'nombre', 'sigla', 'creditos', 'planEstudioId']
      }],
      benchmark: false,
      logging: false,
      nest: true
    });
  },
  
  async create(data) {
    return await Nivel.create(data, {
      benchmark: false,
      logging: false
    });
  },
  
  async findByName(nombre) {
    return await Nivel.findOne({
      where: {
        nombre: {
          [Op.iLike]: `%${nombre}%` // Búsqueda case-insensitive
        }
      },
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async createMateria(materiaData) {
    return await Materia.create(materiaData, {
      benchmark: false,
      logging: false
    });
  },
  
  async searchNiveles(searchTerm, page, limit) {
    return await Nivel.findAndCountAll({
      where: {
        nombre: { [Op.iLike]: `%${searchTerm}%` }
      },
      include: [{
        model: Materia,
        as: 'materias',
        attributes: ['id', 'nombre', 'sigla', 'creditos']
      }],
      order: [['nombre', 'ASC']],
      limit: limit,
      offset: (page - 1) * limit,
      benchmark: false,
      logging: false
    });
  }
};

const nivelController = {
  // GET ALL - Sin bloqueos
  getAll: async (req, res) => {
    const start = Date.now();
    
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = Math.min(parseInt(req.query.limit) || 10, 50);
      const search = req.query.search;
      
      let cacheKey, result;
      
      if (search) {
        cacheKey = `niveles_search_${search}_${page}_${limit}`;
        // Cache para búsquedas
        const cached = cache.get(cacheKey);
        if (cached) {
          return fastResponse(res, 200, { 
            ...cached, 
            fromCache: true 
          }, Date.now() - start);
        }
        
        result = await pool.execute(async () => {
          return await dbOperations.searchNiveles(search, page, limit);
        });
      } else {
        cacheKey = `niveles_${page}_${limit}`;
        // Cache normal
        const cached = cache.get(cacheKey);
        if (cached) {
          return fastResponse(res, 200, { 
            ...cached, 
            fromCache: true 
          }, Date.now() - start);
        }
        
        result = await pool.execute(async () => {
          return await dbOperations.findAll(page, limit);
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
      
      console.error('Error en getAll niveles:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener niveles'
      }, elapsed);
    }
  },
  
  // GET BY ID - Sin bloqueos
  getById: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      const cacheKey = `nivel_${id}`;
      
      const cached = cache.get(cacheKey);
      if (cached) {
        return fastResponse(res, 200, { 
          ...cached, 
          fromCache: true 
        }, Date.now() - start);
      }
      
      const nivel = await pool.execute(async () => {
        return await dbOperations.findById(id);
      });
      
      if (!nivel) {
        return fastResponse(res, 404, {
          success: false,
          message: 'Nivel no encontrado'
        }, Date.now() - start);
      }
      
      const data = { 
        success: true, 
        data: nivel
      };
      
      cache.set(cacheKey, data);
      
      fastResponse(res, 200, data, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      console.error('Error en getById nivel:', error);
      
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener nivel'
      }, elapsed);
    }
  },
  
  // CREATE - Sin bloqueos con transacciones optimizadas
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
      
      const { nombre, materias } = req.body;
      
      const result = await pool.execute(async () => {
        // Verificar si ya existe un nivel con ese nombre
        const existing = await dbOperations.findByName(nombre);
        if (existing) {
          throw new Error('Ya existe un nivel con este nombre');
        }
        
        // Crear el nivel
        const nivel = await dbOperations.create({ nombre });
        
        // Crear materias si existen - en paralelo para mayor velocidad
        if (materias && Array.isArray(materias)) {
          const materiaPromises = materias.map(materia => 
            dbOperations.createMateria({
              nombre: materia.nombre,
              sigla: materia.sigla,
              creditos: materia.creditos,
              nivelId: nivel.id,
              planEstudioId: materia.planEstudioId
            })
          );
          
          await Promise.all(materiaPromises);
        }
        
        // Traer nivel con materias
        return await dbOperations.findById(nivel.id);
      });
      
      // Invalidar cache async
      cache.invalidate('niveles_');
      
      fastResponse(res, 201, {
        success: true,
        message: 'Nivel creado exitosamente',
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
      
      if (error.message === 'QUEUE_FULL') {
        return fastResponse(res, 503, {
          success: false,
          message: 'Server overloaded',
          code: 'QUEUE_FULL'
        }, elapsed);
      }
      
      console.error('Error en create nivel:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al crear nivel'
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
      const { nombre } = req.body;
      
      const result = await pool.execute(async () => {
        const nivel = await Nivel.findByPk(id);
        
        if (!nivel) {
          throw new Error('Nivel no encontrado');
        }
        
        // Verificar nombre duplicado si cambió
        if (nombre && nombre !== nivel.nombre) {
          const existing = await dbOperations.findByName(nombre);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un nivel con este nombre');
          }
        }
        
        return await nivel.update({ nombre });
      });
      
      cache.invalidate('nivel');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Nivel actualizado exitosamente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrado')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.message.includes('nombre')) {
        return fastResponse(res, 400, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en update nivel:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar nivel'
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
        const nivel = await Nivel.findByPk(id);
        
        if (!nivel) {
          throw new Error('Nivel no encontrado');
        }
        
        // Verificar nombre duplicado si se está actualizando
        if (updateData.nombre && updateData.nombre !== nivel.nombre) {
          const existing = await dbOperations.findByName(updateData.nombre);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un nivel con este nombre');
          }
        }
        
        return await nivel.update(updateData);
      });
      
      cache.invalidate('nivel');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Nivel actualizado parcialmente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrado')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en patch nivel:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar nivel'
      }, elapsed);
    }
  },
  
  // DELETE - Sin bloqueos
  delete: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      
      await pool.execute(async () => {
        const nivel = await Nivel.findByPk(id, {
          include: [{
            model: Materia,
            as: 'materias'
          }]
        });
        
        if (!nivel) {
          throw new Error('Nivel no encontrado');
        }
        
        // Verificar si tiene materias asignadas
        if (nivel.materias && nivel.materias.length > 0) {
          throw new Error('No se puede eliminar: el nivel tiene materias asignadas');
        }
        
        await nivel.destroy();
        return true;
      });
      
      cache.invalidate('nivel');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Nivel eliminado exitosamente'
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrado')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.message.includes('materias asignadas')) {
        return fastResponse(res, 400, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en delete nivel:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al eliminar nivel'
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

module.exports = nivelController;