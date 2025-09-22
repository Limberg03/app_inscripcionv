const { Horario } = require('../models');
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

const horarioController = {
  // Obtener todos los horarios - Solo cache en lectura
  getAll: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const cacheKey = `horarios_${page}_${limit}`;

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

      // Solo simular latencia de red, no procesamiento pesado
      const [horarios] = await Promise.all([
        Horario.findAndCountAll({
          order: [['dia', 'ASC'], ['horaInicio', 'ASC']]
        }),
        simulateNetworkLatency()
      ]);

      const responseData = {
        success: true,
        data: horarios.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(horarios.count / limit),
          totalItems: horarios.count,
          itemsPerPage: limit
        },
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'horario'
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
        message: 'Error al obtener horarios',
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
      const cacheKey = `horario_${id}`;

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

      const [horario] = await Promise.all([
        Horario.findByPk(id),
        simulateNetworkLatency()
      ]);

      if (!horario) {
        return res.status(404).json({
          success: false,
          message: 'Horario no encontrado'
        });
      }

      const responseData = {
        success: true,
        data: horario,
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'horario'
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
        message: 'Error al obtener horario',
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
      
      const { dia, horaInicio, horaFin } = req.body;

      // Solo verificar conflictos, sin procesamiento adicional
      const conflicto = await Horario.findOne({
        where: {
          dia,
          [Op.or]: [
            { horaInicio: { [Op.between]: [horaInicio, horaFin] } },
            { horaFin: { [Op.between]: [horaInicio, horaFin] } },
            { [Op.and]: [{ horaInicio: { [Op.lte]: horaInicio } }, { horaFin: { [Op.gte]: horaFin } }] }
          ]
        }
      });

      if (conflicto) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un horario que se superpone en ese día y horario'
        });
      }

      const horario = await Horario.create({ dia, horaInicio, horaFin });

      // Invalidar solo cache relacionado
      cache.invalidatePattern('horarios_');

      res.status(201).json({
        success: true,
        message: 'Horario creado exitosamente',
        data: horario,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'horario'
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
        message: 'Error al crear horario',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Update simplificado
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
      const { dia, horaInicio, horaFin } = req.body;

      const horario = await Horario.findByPk(id);
      if (!horario) {
        return res.status(404).json({
          success: false,
          message: 'Horario no encontrado'
        });
      }

      // Verificar conflictos
      const conflicto = await Horario.findOne({
        where: {
          id: { [Op.ne]: id },
          dia,
          [Op.or]: [
            { horaInicio: { [Op.between]: [horaInicio, horaFin] } },
            { horaFin: { [Op.between]: [horaInicio, horaFin] } },
            { [Op.and]: [{ horaInicio: { [Op.lte]: horaInicio } }, { horaFin: { [Op.gte]: horaFin } }] }
          ]
        }
      });

      if (conflicto) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un horario que se superpone en ese día y horario'
        });
      }

      const updatedHorario = await horario.update({ dia, horaInicio, horaFin });

      // Invalidar cache relacionado
      cache.invalidatePattern('horario');

      res.status(200).json({
        success: true,
        message: 'Horario actualizado exitosamente',
        data: updatedHorario,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'horario'
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
        message: 'Error al actualizar horario',
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
      
      const horario = await Horario.findByPk(id);
      if (!horario) {
        return res.status(404).json({
          success: false,
          message: 'Horario no encontrado'
        });
      }

      await horario.destroy();

      // Invalidar cache relacionado
      cache.invalidatePattern('horario');

      res.status(200).json({
        success: true,
        message: 'Horario eliminado exitosamente',
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'horario'
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
        message: 'Error al eliminar horario',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Stats del sistema
  getStats: async (req, res) => {
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

module.exports = horarioController;