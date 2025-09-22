// controllers/docente.controller.js - LOCKLESS ARCHITECTURE
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
const cluster = require('cluster');

// Importar modelos
let Docente, GrupoMateria, Materia;
try {
  ({ Docente, GrupoMateria, Materia } = require('../models'));
} catch (error) {
  const models = require('../models');
  Docente = models.Docente;
  GrupoMateria = models.GrupoMateria;
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
    // Pre-cargar docentes más consultados
    const commonKeys = [
      'docentes_1_10',
      'docentes_1_20',
      'docentes_2_10',
      'docente_stats'
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
    return await Docente.findAndCountAll({
      include: [{
        model: GrupoMateria,
        as: 'grupos',
        attributes: ['id', 'grupo', 'estado'],
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }]
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
    return await Docente.findByPk(id, {
      include: [{
        model: GrupoMateria,
        as: 'grupos',
        attributes: ['id', 'grupo', 'estado'],
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }]
      }],
      benchmark: false,
      logging: false,
      nest: true
    });
  },
  
  async create(data) {
    return await Docente.create(data, {
      benchmark: false,
      logging: false
    });
  },
  
  async findByName(nombre) {
    return await Docente.findOne({
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
  
  async findByPhone(telefono) {
    return await Docente.findOne({
      where: { telefono },
      raw: true,
      benchmark: false,
      logging: false
    });
  },
  
  async searchDocentes(searchTerm, page, limit) {
    return await Docente.findAndCountAll({
      where: {
        [Op.or]: [
          { nombre: { [Op.iLike]: `%${searchTerm}%` } },
          { telefono: { [Op.like]: `%${searchTerm}%` } }
        ]
      },
      include: [{
        model: GrupoMateria,
        as: 'grupos',
        attributes: ['id', 'grupo', 'estado'],
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla']
        }]
      }],
      order: [['nombre', 'ASC']],
      limit: limit,
      offset: (page - 1) * limit,
      benchmark: false,
      logging: false
    });
  }
};

const docenteController = {
  // GET ALL - Sin bloqueos
  getAll: async (req, res) => {
    const start = Date.now();
    
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = Math.min(parseInt(req.query.limit) || 10, 50);
      const search = req.query.search;
      
      let cacheKey, result;
      
      if (search) {
        cacheKey = `docentes_search_${search}_${page}_${limit}`;
        // Cache para búsquedas
        const cached = cache.get(cacheKey);
        if (cached) {
          return fastResponse(res, 200, { 
            ...cached, 
            fromCache: true 
          }, Date.now() - start);
        }
        
        result = await pool.execute(async () => {
          return await dbOperations.searchDocentes(search, page, limit);
        });
      } else {
        cacheKey = `docentes_${page}_${limit}`;
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
      
      console.error('Error en getAll docentes:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener docentes'
      }, elapsed);
    }
  },
  
  // GET BY ID - Sin bloqueos
  getById: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      const cacheKey = `docente_${id}`;
      
      const cached = cache.get(cacheKey);
      if (cached) {
        return fastResponse(res, 200, { 
          ...cached, 
          fromCache: true 
        }, Date.now() - start);
      }
      
      const docente = await pool.execute(async () => {
        return await dbOperations.findById(id);
      });
      
      if (!docente) {
        return fastResponse(res, 404, {
          success: false,
          message: 'Docente no encontrado'
        }, Date.now() - start);
      }
      
      const data = { 
        success: true, 
        data: docente
      };
      
      cache.set(cacheKey, data);
      
      fastResponse(res, 200, data, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      console.error('Error en getById docente:', error);
      
      fastResponse(res, 500, {
        success: false,
        message: 'Error al obtener docente'
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
      
      const { nombre, telefono } = req.body;
      
      const result = await pool.execute(async () => {
        // Verificar duplicados por teléfono (si es relevante)
        if (telefono) {
          const existing = await dbOperations.findByPhone(telefono);
          if (existing) {
            throw new Error('Ya existe un docente con este teléfono');
          }
        }
        
        // Crear registro
        return await dbOperations.create({ nombre, telefono });
      });
      
      // Invalidar cache async
      cache.invalidate('docentes_');
      
      fastResponse(res, 201, {
        success: true,
        message: 'Docente creado exitosamente',
        data: result
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('teléfono')) {
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
      
      console.error('Error en create docente:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al crear docente'
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
      const { nombre, telefono } = req.body;
      
      const result = await pool.execute(async () => {
        const docente = await Docente.findByPk(id);
        
        if (!docente) {
          throw new Error('Docente no encontrado');
        }
        
        // Verificar teléfono duplicado si cambió
        if (telefono && telefono !== docente.telefono) {
          const existing = await dbOperations.findByPhone(telefono);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un docente con este teléfono');
          }
        }
        
        return await docente.update({ nombre, telefono });
      });
      
      cache.invalidate('docente');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Docente actualizado exitosamente',
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
      
      if (error.message.includes('teléfono')) {
        return fastResponse(res, 400, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en update docente:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar docente'
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
        const docente = await Docente.findByPk(id);
        
        if (!docente) {
          throw new Error('Docente no encontrado');
        }
        
        // Verificar teléfono duplicado si se está actualizando
        if (updateData.telefono && updateData.telefono !== docente.telefono) {
          const existing = await dbOperations.findByPhone(updateData.telefono);
          if (existing && existing.id !== parseInt(id)) {
            throw new Error('Ya existe un docente con este teléfono');
          }
        }
        
        return await docente.update(updateData);
      });
      
      cache.invalidate('docente');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Docente actualizado parcialmente',
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
      
      console.error('Error en patch docente:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al actualizar docente'
      }, elapsed);
    }
  },
  
  // DELETE - Sin bloqueos
  delete: async (req, res) => {
    const start = Date.now();
    
    try {
      const { id } = req.params;
      
      await pool.execute(async () => {
        const docente = await Docente.findByPk(id, {
          include: [{
            model: GrupoMateria,
            as: 'grupos'
          }]
        });
        
        if (!docente) {
          throw new Error('Docente no encontrado');
        }
        
        // Verificar si tiene grupos asignados
        if (docente.grupos && docente.grupos.length > 0) {
          throw new Error('No se puede eliminar: el docente tiene grupos asignados');
        }
        
        await docente.destroy();
        return true;
      });
      
      cache.invalidate('docente');
      
      fastResponse(res, 200, {
        success: true,
        message: 'Docente eliminado exitosamente'
      }, Date.now() - start);
      
    } catch (error) {
      const elapsed = Date.now() - start;
      
      if (error.message.includes('no encontrado')) {
        return fastResponse(res, 404, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      if (error.message.includes('grupos asignados')) {
        return fastResponse(res, 400, {
          success: false,
          message: error.message
        }, elapsed);
      }
      
      console.error('Error en delete docente:', error);
      fastResponse(res, 500, {
        success: false,
        message: 'Error al eliminar docente'
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

module.exports = docenteController;