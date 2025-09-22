const { Aula } = require('../models');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');

// Simulación realista de latencia de red/DB (no procesamiento pesado)
const simulateNetworkLatency = () => {
  const minDelay = 5; // Latencia mínima realista
  const maxDelay = 25; // Latencia máxima realista
  const delay = Math.floor(Math.random() * (maxDelay - minDelay + 1)) + minDelay;
  
  return new Promise(resolve => setTimeout(resolve, delay));
};

// Pool de conexiones optimizado para Node.js
class OptimizedConnectionPool {
  constructor(maxConnections = 20) { // Conservador pero eficiente
    this.maxConnections = maxConnections;
    this.activeConnections = 0;
    this.queue = [];
    this.stats = {
      totalRequests: 0,
      completedRequests: 0,
      rejectedRequests: 0,
      maxQueueSize: 0,
      avgWaitTime: 0
    };
  }
  
  async acquire() {
    this.stats.totalRequests++;
    const startTime = Date.now();
    
    return new Promise((resolve, reject) => {
      // Rechazar si la cola está demasiado llena (backpressure)
      if (this.queue.length > 100) {
        this.stats.rejectedRequests++;
        reject(new Error('Connection pool saturated'));
        return;
      }

      if (this.activeConnections < this.maxConnections) {
        this.activeConnections++;
        this.stats.completedRequests++;
        resolve();
      } else {
        const queueItem = {
          resolve: () => {
            this.activeConnections++;
            this.stats.completedRequests++;
            const waitTime = Date.now() - startTime;
            this.stats.avgWaitTime = (this.stats.avgWaitTime + waitTime) / 2;
            resolve();
          },
          startTime
        };
        this.queue.push(queueItem);
        this.stats.maxQueueSize = Math.max(this.stats.maxQueueSize, this.queue.length);
      }
    });
  }
  
  release() {
    this.activeConnections--;
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      next.resolve();
    }
  }
  
  getStats() {
    return {
      ...this.stats,
      activeConnections: this.activeConnections,
      queueSize: this.queue.length,
      utilization: (this.activeConnections / this.maxConnections * 100).toFixed(2) + '%'
    };
  }
}

const connectionPool = new OptimizedConnectionPool(15); // Óptimo para la mayoría de casos

// Cache inteligente con TTL y limpieza selectiva
class SmartCache {
  constructor(maxSize = 1000, ttl = 30000) {
    this.cache = new Map();
    this.maxSize = maxSize;
    this.ttl = ttl;
    this.hits = 0;
    this.misses = 0;
  }
  
  set(key, data) {
    // Limpieza automática si excede el tamaño
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    
    this.cache.set(key, {
      data,
      timestamp: Date.now()
    });
  }
  
  get(key) {
    const entry = this.cache.get(key);
    if (entry && (Date.now() - entry.timestamp < this.ttl)) {
      this.hits++;
      return entry.data;
    }
    
    this.misses++;
    this.cache.delete(key);
    return null;
  }
  
  invalidatePattern(pattern) {
    for (const [key] of this.cache) {
      if (key.includes(pattern)) {
        this.cache.delete(key);
      }
    }
  }
  
  getStats() {
    const total = this.hits + this.misses;
    return {
      size: this.cache.size,
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? ((this.hits / total) * 100).toFixed(2) + '%' : '0%'
    };
  }
}

const cache = new SmartCache();

// Validación de unicidad para nombres de aula
const validateUniqueName = async (req, res, next) => {
  try {
    const { nombre } = req.body;
    if (nombre) {
      const existing = await Aula.findOne({ where: { nombre } });
      if (existing && existing.id !== parseInt(req.params.id || 0)) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un aula con ese nombre'
        });
      }
    }
    next();
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Error validando unicidad',
      error: error.message
    });
  }
};

const aulaController = {
  // Obtener todos los aulas - Con cache y filtros optimizados
  getAll: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { capacidadMin, capacidadMax, estado } = req.query;

      // Crear clave de cache con filtros
      const cacheKey = `aulas_${page}_${limit}_${capacidadMin || ''}_${capacidadMax || ''}_${estado || ''}`;

      // Verificar cache
      const cachedData = cache.get(cacheKey);
      if (cachedData) {
        return res.status(200).json({
          ...cachedData,
          processingInfo: {
            ...cachedData.processingInfo,
            cached: true,
            timestamp: new Date().toISOString()
          }
        });
      }

      // Construir condiciones WHERE de manera eficiente
      const where = {};
      if (capacidadMin || capacidadMax) {
        where.capacidad = {};
        if (capacidadMin) where.capacidad[Op.gte] = parseInt(capacidadMin);
        if (capacidadMax) where.capacidad[Op.lte] = parseInt(capacidadMax);
      }
      if (estado !== undefined) {
        where.estado = estado === 'true';
      }

      // Solo simular latencia de red, no procesamiento pesado
      const [aulas] = await Promise.all([
        Aula.findAndCountAll({ 
          where, 
          order: [['nombre', 'ASC']],
          limit,
          offset: (page - 1) * limit
        }),
        simulateNetworkLatency()
      ]);

      const responseData = {
        success: true,
        data: aulas.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(aulas.count / limit),
          totalItems: aulas.count,
          itemsPerPage: limit,
          hasPreviousPage: page > 1,
          hasNextPage: page < Math.ceil(aulas.count / limit)
        },
        filters: {
          capacidadMin,
          capacidadMax,
          estado: estado === 'true'
        },
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 1
        }
      };

      cache.set(cacheKey, responseData);
      res.status(200).json(responseData);

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al obtener aulas',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener por ID - Solo cache
  getById: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const cacheKey = `aula_${id}`;

      const cachedData = cache.get(cacheKey);
      if (cachedData) {
        return res.status(200).json({
          ...cachedData,
          processingInfo: {
            ...cachedData.processingInfo,
            cached: true,
            timestamp: new Date().toISOString()
          }
        });
      }

      const [aula] = await Promise.all([
        Aula.findByPk(id),
        simulateNetworkLatency()
      ]);

      if (!aula) {
        return res.status(404).json({
          success: false,
          message: 'Aula no encontrada'
        });
      }

      const responseData = {
        success: true,
        data: aula,
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 1
        }
      };

      cache.set(cacheKey, responseData);
      res.status(200).json(responseData);

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al obtener aula',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Crear - Validación en paralelo pero sin procesamiento innecesario
  create: async (req, res) => {
    let acquired = false;
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      await connectionPool.acquire();
      acquired = true;
      
      const { nombre, capacidad, estado = true } = req.body;

      // Validar unicidad de nombre
      const existingAula = await Aula.findOne({ where: { nombre } });
      if (existingAula) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un aula con ese nombre'
        });
      }

      // Validar capacidad positiva
      if (capacidad <= 0) {
        return res.status(400).json({
          success: false,
          message: 'La capacidad debe ser un número positivo'
        });
      }

      const aula = await Aula.create({ nombre, capacidad, estado });

      // Invalidar cache relacionado
      cache.invalidatePattern('aulas_');

      res.status(201).json({
        success: true,
        message: 'Aula creada exitosamente',
        data: aula,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 2
        }
      });

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El nombre del aula ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al crear aula',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Update simplificado con validaciones
  update: async (req, res) => {
    let acquired = false;
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const { nombre, capacidad, estado } = req.body;

      const aula = await Aula.findByPk(id);
      if (!aula) {
        return res.status(404).json({
          success: false,
          message: 'Aula no encontrada'
        });
      }

      // Validar unicidad de nombre si se está actualizando
      if (nombre && nombre !== aula.nombre) {
        const existingAula = await Aula.findOne({ 
          where: { 
            nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingAula) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un aula con ese nombre'
          });
        }
      }

      // Validar capacidad positiva
      if (capacidad !== undefined && capacidad <= 0) {
        return res.status(400).json({
          success: false,
          message: 'La capacidad debe ser un número positivo'
        });
      }

      const updatedAula = await aula.update({ 
        nombre: nombre || aula.nombre,
        capacidad: capacidad !== undefined ? capacidad : aula.capacidad,
        estado: estado !== undefined ? estado : aula.estado
      });

      // Invalidar cache relacionado
      cache.invalidatePattern('aula');
      cache.invalidatePattern('aulas_');

      res.status(200).json({
        success: true,
        message: 'Aula actualizada exitosamente',
        data: updatedAula,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 2
        }
      });

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El nombre del aula ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al actualizar aula',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Patch para actualizaciones parciales
  patch: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const updateData = req.body;

      const aula = await Aula.findByPk(id);
      if (!aula) {
        return res.status(404).json({
          success: false,
          message: 'Aula no encontrada'
        });
      }

      // Validar campos específicos si existen
      if (updateData.nombre && updateData.nombre !== aula.nombre) {
        const existingAula = await Aula.findOne({ 
          where: { 
            nombre: updateData.nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingAula) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un aula con ese nombre'
          });
        }
      }

      if (updateData.capacidad !== undefined && updateData.capacidad <= 0) {
        return res.status(400).json({
          success: false,
          message: 'La capacidad debe ser un número positivo'
        });
      }

      const updatedAula = await aula.update(updateData);

      // Invalidar cache relacionado
      cache.invalidatePattern('aula');
      cache.invalidatePattern('aulas_');

      res.status(200).json({ 
        success: true, 
        message: 'Aula actualizada parcialmente', 
        data: updatedAula,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 2
        }
      });
    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al actualizar aula parcialmente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Delete simplificado
  delete: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      
      const aula = await Aula.findByPk(id);
      if (!aula) {
        return res.status(404).json({
          success: false,
          message: 'Aula no encontrada'
        });
      }

      await aula.destroy();

      // Invalidar cache relacionado
      cache.invalidatePattern('aula');
      cache.invalidatePattern('aulas_');

      res.status(200).json({
        success: true,
        message: 'Aula eliminada exitosamente',
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 2
        }
      });

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al eliminar aula',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener aulas disponibles - Con cache
  getAvailable: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { capacidadMin } = req.query;
      const cacheKey = `aulas_available_${capacidadMin || ''}`;

      const cachedData = cache.get(cacheKey);
      if (cachedData) {
        return res.status(200).json({
          ...cachedData,
          processingInfo: {
            ...cachedData.processingInfo,
            cached: true,
            timestamp: new Date().toISOString()
          }
        });
      }

      const where = { estado: true };
      if (capacidadMin) {
        where.capacidad = { [Op.gte]: parseInt(capacidadMin) };
      }

      const [aulas] = await Promise.all([
        Aula.findAll({ 
          where, 
          order: [['capacidad', 'ASC'], ['nombre', 'ASC']] 
        }),
        simulateNetworkLatency()
      ]);

      const responseData = {
        success: true,
        data: aulas,
        filters: {
          capacidadMin: capacidadMin ? parseInt(capacidadMin) : null,
          estado: true
        },
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 1
        }
      };

      cache.set(cacheKey, responseData);
      res.status(200).json(responseData);

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al obtener aulas disponibles',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener estadísticas de aulas
  getStats: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;

      const [stats] = await Promise.all([
        Aula.findAll({
          attributes: [
            [Aula.sequelize.fn('COUNT', Aula.sequelize.col('id')), 'total'],
            [Aula.sequelize.fn('COUNT', Aula.sequelize.literal('CASE WHEN estado = true THEN 1 END')), 'available'],
            [Aula.sequelize.fn('AVG', Aula.sequelize.col('capacidad')), 'avgCapacity'],
            [Aula.sequelize.fn('MIN', Aula.sequelize.col('capacidad')), 'minCapacity'],
            [Aula.sequelize.fn('MAX', Aula.sequelize.col('capacidad')), 'maxCapacity']
          ]
        }),
        simulateNetworkLatency()
      ]);

      const totalAulas = await Aula.count();
      const availableAulas = await Aula.count({ where: { estado: true } });

      res.status(200).json({
        success: true,
        data: {
          total: totalAulas,
          available: availableAulas,
          unavailable: totalAulas - availableAulas,
          availabilityRate: totalAulas > 0 ? ((availableAulas / totalAulas) * 100).toFixed(2) + '%' : '0%',
          capacityStats: stats[0]
        },
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'aula',
          queryCount: 3
        }
      });

    } catch (error) {
      if (error.message === 'Connection pool saturated') {
        return res.status(503).json({
          success: false,
          message: 'Servidor temporalmente saturado, intenta más tarde',
          retryAfter: 5
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al obtener estadísticas de aulas',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Stats del sistema
  getSystemStats: async (req, res) => {
    res.status(200).json({
      success: true,
      connectionPool: connectionPool.getStats(),
      cache: cache.getStats(),
      system: {
        memoryUsage: process.memoryUsage(),
        uptime: process.uptime()
      }
    });
  }
};

module.exports = aulaController;