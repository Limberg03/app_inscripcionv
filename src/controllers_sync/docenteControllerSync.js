const { Docente, GrupoMateria, Materia } = require('../models');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');

// Simulación realista de latencia de red/DB
const simulateNetworkLatency = () => {
  const minDelay = 5;
  const maxDelay = 25;
  const delay = Math.floor(Math.random() * (maxDelay - minDelay + 1)) + minDelay;
  
  return new Promise(resolve => setTimeout(resolve, delay));
};

// Pool de conexiones optimizado
class OptimizedConnectionPool {
  constructor(maxConnections = 20) {
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

const connectionPool = new OptimizedConnectionPool(15);

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

// Validación de unicidad para nombre de docente
const validateUniqueName = async (req, res, next) => {
  try {
    const { nombre } = req.body;
    if (nombre) {
      const existing = await Docente.findOne({ where: { nombre } });
      if (existing && existing.id !== parseInt(req.params.id || 0)) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un docente con ese nombre'
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

const docenteController = {
  // Obtener todos los docentes - Con cache y relaciones optimizadas
  getAll: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { nombre } = req.query;

      // Crear clave de cache con filtros
      const cacheKey = `docentes_${page}_${limit}_${nombre || ''}`;

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

      // Construir condiciones WHERE
      const where = {};
      if (nombre) {
        where.nombre = { [Op.iLike]: `%${nombre}%` };
      }

      const [docentes] = await Promise.all([
        Docente.findAndCountAll({
          where,
          include: [{
            model: GrupoMateria,
            as: 'grupos',
            attributes: ['id', 'grupo', 'estado'],
            include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }]
          }],
          order: [['nombre', 'ASC']],
          limit,
          offset: (page - 1) * limit
        }),
        simulateNetworkLatency()
      ]);

      const responseData = {
        success: true,
        data: docentes.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(docentes.count / limit),
          totalItems: docentes.count,
          itemsPerPage: limit,
          hasPreviousPage: page > 1,
          hasNextPage: page < Math.ceil(docentes.count / limit)
        },
        filters: {
          nombre: nombre || null
        },
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'docente',
          queryCount: 1,
          includesRelations: true
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
        message: 'Error al obtener docentes',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener por ID - Con cache
  getById: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const cacheKey = `docente_${id}`;

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

      const [docente] = await Promise.all([
        Docente.findByPk(id, {
          include: [{
            model: GrupoMateria,
            as: 'grupos',
            attributes: ['id', 'grupo', 'estado'],
            include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }]
          }]
        }),
        simulateNetworkLatency()
      ]);

      if (!docente) {
        return res.status(404).json({
          success: false,
          message: 'Docente no encontrado'
        });
      }

      const responseData = {
        success: true,
        data: docente,
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'docente',
          queryCount: 1,
          includesRelations: true
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
        message: 'Error al obtener docente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Crear - Con validación de unicidad
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
      
      const { nombre, telefono } = req.body;

      // Validar unicidad de nombre
      const existingDocente = await Docente.findOne({ where: { nombre } });
      if (existingDocente) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un docente con ese nombre'
        });
      }

      // Validar formato de teléfono si existe
      if (telefono && !/^\+?\d{7,15}$/.test(telefono)) {
        return res.status(400).json({
          success: false,
          message: 'El número de teléfono no tiene un formato válido'
        });
      }

      const docente = await Docente.create({ nombre, telefono });

      // Invalidar cache relacionado
      cache.invalidatePattern('docentes_');
      cache.invalidatePattern('docente_');

      res.status(201).json({
        success: true,
        message: 'Docente creado exitosamente',
        data: docente,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'docente',
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
          message: 'El nombre del docente ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al crear docente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Update - Con validaciones
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
      const { nombre, telefono } = req.body;

      const docente = await Docente.findByPk(id);
      if (!docente) {
        return res.status(404).json({
          success: false,
          message: 'Docente no encontrado'
        });
      }

      // Validar unicidad de nombre si se está actualizando
      if (nombre && nombre !== docente.nombre) {
        const existingDocente = await Docente.findOne({ 
          where: { 
            nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingDocente) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un docente con ese nombre'
          });
        }
      }

      // Validar formato de teléfono si existe
      if (telefono && !/^\+?\d{7,15}$/.test(telefono)) {
        return res.status(400).json({
          success: false,
          message: 'El número de teléfono no tiene un formato válido'
        });
      }

      const updatedDocente = await docente.update({ 
        nombre: nombre || docente.nombre,
        telefono: telefono !== undefined ? telefono : docente.telefono
      });

      // Invalidar cache relacionado
      cache.invalidatePattern('docentes_');
      cache.invalidatePattern('docente_');

      res.status(200).json({
        success: true,
        message: 'Docente actualizado exitosamente',
        data: updatedDocente,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'docente',
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
          message: 'El nombre del docente ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al actualizar docente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Patch - Para actualizaciones parciales
  patch: async (req, res) => {
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
      const updateData = req.body;

      const docente = await Docente.findByPk(id);
      if (!docente) {
        return res.status(404).json({
          success: false,
          message: 'Docente no encontrado'
        });
      }

      // Validar unicidad de nombre si se está actualizando
      if (updateData.nombre && updateData.nombre !== docente.nombre) {
        const existingDocente = await Docente.findOne({ 
          where: { 
            nombre: updateData.nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingDocente) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un docente con ese nombre'
          });
        }
      }

      // Validar formato de teléfono si existe
      if (updateData.telefono && !/^\+?\d{7,15}$/.test(updateData.telefono)) {
        return res.status(400).json({
          success: false,
          message: 'El número de teléfono no tiene un formato válido'
        });
      }

      const updatedDocente = await docente.update(updateData);

      // Invalidar cache relacionado
      cache.invalidatePattern('docentes_');
      cache.invalidatePattern('docente_');

      res.status(200).json({
        success: true,
        message: 'Docente actualizado parcialmente',
        data: updatedDocente,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'docente',
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
        message: 'Error al actualizar docente parcialmente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Delete - Con invalidación de cache
  delete: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      
      const docente = await Docente.findByPk(id);
      if (!docente) {
        return res.status(404).json({
          success: false,
          message: 'Docente no encontrado'
        });
      }

      // Verificar si el docente tiene grupos asignados
      const grupoCount = await GrupoMateria.count({ where: { docenteId: id } });
      if (grupoCount > 0) {
        return res.status(400).json({
          success: false,
          message: 'No se puede eliminar el docente porque tiene grupos asignados'
        });
      }

      await docente.destroy();

      // Invalidar cache relacionado
      cache.invalidatePattern('docentes_');
      cache.invalidatePattern('docente_');

      res.status(200).json({
        success: true,
        message: 'Docente eliminado exitosamente',
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'docente',
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
        message: 'Error al eliminar docente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener estadísticas de docentes
  getStats: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;

      const [stats, grupoStats] = await Promise.all([
        Docente.findAll({
          attributes: [
            [Docente.sequelize.fn('COUNT', Docente.sequelize.col('id')), 'total'],
            [Docente.sequelize.fn('COUNT', Docente.sequelize.literal('CASE WHEN telefono IS NOT NULL THEN 1 END')), 'withPhone']
          ]
        }),
        GrupoMateria.findAll({
          attributes: [
            'docenteId',
            [GrupoMateria.sequelize.fn('COUNT', GrupoMateria.sequelize.col('id')), 'grupoCount']
          ],
          group: ['docenteId']
        }),
        simulateNetworkLatency()
      ]);

      const totalDocentes = await Docente.count();
      const docentesWithGroups = await GrupoMateria.count({
        distinct: true,
        col: 'docenteId'
      });

      res.status(200).json({
        success: true,
        data: {
          total: totalDocentes,
          withPhone: stats[0].dataValues.withPhone,
          withGroups: docentesWithGroups,
          withoutGroups: totalDocentes - docentesWithGroups,
          assignmentRate: totalDocentes > 0 ? ((docentesWithGroups / totalDocentes) * 100).toFixed(2) + '%' : '0%',
          groupStats: grupoStats.map(g => ({
            docenteId: g.docenteId,
            grupoCount: g.dataValues.grupoCount
          }))
        },
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'docente',
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
        message: 'Error al obtener estadísticas de docentes',
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

module.exports = docenteController;