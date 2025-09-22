const { Nivel, Materia } = require('../models');
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

// Validación de unicidad para nombre de nivel
const validateUniqueName = async (req, res, next) => {
  try {
    const { nombre } = req.body;
    if (nombre) {
      const existing = await Nivel.findOne({ where: { nombre } });
      if (existing && existing.id !== parseInt(req.params.id || 0)) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un nivel con ese nombre'
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

const nivelController = {
  // Obtener todos los niveles - Con cache y relaciones optimizadas
  getAll: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { nombre } = req.query;

      // Crear clave de cache con filtros
      const cacheKey = `niveles_${page}_${limit}_${nombre || ''}`;

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

      // Solo simular latencia de red, no procesamiento pesado
      const [niveles] = await Promise.all([
        Nivel.findAndCountAll({
          where,
          include: [{
            model: Materia,
            as: 'materias',
            attributes: ['id', 'nombre', 'sigla', 'creditos'],
            order: [['nombre', 'ASC']]
          }],
          order: [['nombre', 'ASC']],
          limit,
          offset: (page - 1) * limit
        }),
        simulateNetworkLatency()
      ]);

      const responseData = {
        success: true,
        data: niveles.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(niveles.count / limit),
          totalItems: niveles.count,
          itemsPerPage: limit,
          hasPreviousPage: page > 1,
          hasNextPage: page < Math.ceil(niveles.count / limit)
        },
        filters: {
          nombre: nombre || null
        },
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
        message: 'Error al obtener niveles',
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
      const cacheKey = `nivel_${id}`;

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

      const [nivel] = await Promise.all([
        Nivel.findByPk(id, {
          include: [{
            model: Materia,
            as: 'materias',
            attributes: ['id', 'nombre', 'sigla', 'creditos'],
            order: [['nombre', 'ASC']]
          }]
        }),
        simulateNetworkLatency()
      ]);

      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      const responseData = {
        success: true,
        data: nivel,
        processingInfo: {
          optimized: true,
          cached: false,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
        message: 'Error al obtener nivel',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Crear - Con validación de unicidad y creación de materias en paralelo
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
      
      const { nombre, materias } = req.body;

      // Validar unicidad de nombre
      const existingNivel = await Nivel.findOne({ where: { nombre } });
      if (existingNivel) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe un nivel con ese nombre'
        });
      }

      // Validar que el nombre no esté vacío
      if (!nombre || nombre.trim().length === 0) {
        return res.status(400).json({
          success: false,
          message: 'El nombre del nivel es requerido'
        });
      }

      // Crear nivel
      const nivel = await Nivel.create({ nombre });

      // Si hay materias, crearlas en paralelo con validaciones
      let createdMaterias = [];
      if (materias && Array.isArray(materias) && materias.length > 0) {
        const materiasPromises = materias.map(async (materia, index) => {
          // Validaciones por materia
          if (!materia.nombre || materia.nombre.trim().length === 0) {
            throw new Error(`La materia ${index + 1} no tiene nombre válido`);
          }
          
          if (!materia.sigla || materia.sigla.trim().length === 0) {
            throw new Error(`La materia ${index + 1} no tiene sigla válida`);
          }
          
          if (!materia.creditos || materia.creditos <= 0) {
            throw new Error(`Los créditos de la materia ${index + 1} deben ser un número positivo`);
          }

          // Crear materia
          return Materia.create({
            nombre: materia.nombre.trim(),
            sigla: materia.sigla.trim(),
            creditos: materia.creditos,
            nivelId: nivel.id,
            planEstudioId: materia.planEstudioId
          });
        });

        createdMaterias = await Promise.all(materiasPromises);
      }

      // Obtener nivel con materias para respuesta completa
      const [nivelConMaterias] = await Promise.all([
        Nivel.findByPk(nivel.id, {
          include: [{
            model: Materia,
            as: 'materias',
            attributes: ['id', 'nombre', 'sigla', 'creditos'],
            order: [['nombre', 'ASC']]
          }]
        }),
        simulateNetworkLatency()
      ]);

      // Invalidar cache relacionado
      cache.invalidatePattern('niveles_');
      cache.invalidatePattern('nivel_');

      res.status(201).json({
        success: true,
        message: `Nivel creado exitosamente con ${createdMaterias.length} materias`,
        data: nivelConMaterias,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
          queryCount: createdMaterias.length > 0 ? 3 : 2,
          materiasCreadas: createdMaterias.length,
          includesRelations: true
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
          message: 'El nombre del nivel ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al crear nivel',
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
      const { nombre } = req.body;

      const nivel = await Nivel.findByPk(id);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      // Validar unicidad de nombre si se está actualizando
      if (nombre && nombre !== nivel.nombre) {
        const existingNivel = await Nivel.findOne({ 
          where: { 
            nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingNivel) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un nivel con ese nombre'
          });
        }
      }

      // Validar que el nombre no esté vacío
      if (nombre && (!nombre || nombre.trim().length === 0)) {
        return res.status(400).json({
          success: false,
          message: 'El nombre del nivel es requerido'
        });
      }

      const updatedNivel = await nivel.update({ 
        nombre: nombre ? nombre.trim() : nivel.nombre
      });

      // Invalidar cache relacionado
      cache.invalidatePattern('niveles_');
      cache.invalidatePattern('nivel_');

      res.status(200).json({
        success: true,
        message: 'Nivel actualizado exitosamente',
        data: updatedNivel,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
          message: 'El nombre del nivel ya existe'
        });
      }
      
      res.status(500).json({
        success: false,
        message: 'Error al actualizar nivel',
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
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const updateData = req.body;

      const nivel = await Nivel.findByPk(id);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      // Validar unicidad de nombre si se está actualizando
      if (updateData.nombre && updateData.nombre !== nivel.nombre) {
        const existingNivel = await Nivel.findOne({ 
          where: { 
            nombre: updateData.nombre,
            id: { [Op.ne]: id }
          } 
        });
        if (existingNivel) {
          return res.status(400).json({
            success: false,
            message: 'Ya existe un nivel con ese nombre'
          });
        }
      }

      // Validar que el nombre no esté vacío
      if (updateData.nombre && (!updateData.nombre || updateData.nombre.trim().length === 0)) {
        return res.status(400).json({
          success: false,
          message: 'El nombre del nivel es requerido'
        });
      }

      const updatedNivel = await nivel.update({
        nombre: updateData.nombre ? updateData.nombre.trim() : nivel.nombre
      });

      // Invalidar cache relacionado
      cache.invalidatePattern('niveles_');
      cache.invalidatePattern('nivel_');

      res.status(200).json({
        success: true,
        message: 'Nivel actualizado parcialmente',
        data: updatedNivel,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
        message: 'Error al actualizar nivel parcialmente',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Delete - Con validación de dependencias
  delete: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      
      const nivel = await Nivel.findByPk(id);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      // Verificar si el nivel tiene materias asignadas
      const materiaCount = await Materia.count({ where: { nivelId: id } });
      if (materiaCount > 0) {
        return res.status(400).json({
          success: false,
          message: `No se puede eliminar el nivel porque tiene ${materiaCount} materias asociadas. Elimine primero las materias.`
        });
      }

      await nivel.destroy();

      // Invalidar cache relacionado
      cache.invalidatePattern('niveles_');
      cache.invalidatePattern('nivel_');

      res.status(200).json({
        success: true,
        message: 'Nivel eliminado exitosamente',
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
        message: 'Error al eliminar nivel',
        error: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Crear múltiples materias para un nivel existente
  addMateriasToNivel: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;
      
      const { id } = req.params;
      const { materias } = req.body;

      const nivel = await Nivel.findByPk(id);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      if (!materias || !Array.isArray(materias) || materias.length === 0) {
        return res.status(400).json({
          success: false,
          message: 'Se requiere al menos una materia para agregar'
        });
      }

      // Crear materias en paralelo con validaciones
      const materiasPromises = materias.map(async (materia, index) => {
        // Validaciones por materia
        if (!materia.nombre || materia.nombre.trim().length === 0) {
          throw new Error(`La materia ${index + 1} no tiene nombre válido`);
        }
        
        if (!materia.sigla || materia.sigla.trim().length === 0) {
          throw new Error(`La materia ${index + 1} no tiene sigla válida`);
        }
        
        if (!materias.creditos || materia.creditos <= 0) {
          throw new Error(`Los créditos de la materia ${index + 1} deben ser un número positivo`);
        }

        // Verificar unicidad de sigla en el nivel
        const existingSigla = await Materia.findOne({
          where: {
            sigla: materia.sigla.trim(),
            nivelId: id
          }
        });
        
        if (existingSigla) {
          throw new Error(`La sigla ${materia.sigla} ya existe en este nivel`);
        }

        return Materia.create({
          nombre: materia.nombre.trim(),
          sigla: materia.sigla.trim(),
          creditos: materia.creditos,
          nivelId: id,
          planEstudioId: materia.planEstudioId
        });
      });

      const createdMaterias = await Promise.all(materiasPromises);

      // Obtener nivel actualizado con todas las materias
      const [nivelActualizado] = await Promise.all([
        Nivel.findByPk(id, {
          include: [{
            model: Materia,
            as: 'materias',
            attributes: ['id', 'nombre', 'sigla', 'creditos'],
            order: [['nombre', 'ASC']]
          }]
        }),
        simulateNetworkLatency()
      ]);

      // Invalidar cache relacionado
      cache.invalidatePattern('nivel_');
      cache.invalidatePattern('niveles_');

      res.status(201).json({
        success: true,
        message: `Se agregaron ${createdMaterias.length} materias al nivel exitosamente`,
        data: nivelActualizado,
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
          queryCount: 2,
          materiasAgregadas: createdMaterias.length,
          includesRelations: true
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
      
      res.status(400).json({
        success: false,
        message: error.message
      });
    } finally {
      if (acquired) connectionPool.release();
    }
  },

  // Obtener estadísticas de niveles
  getStats: async (req, res) => {
    let acquired = false;
    try {
      await connectionPool.acquire();
      acquired = true;

      const [stats, materiaStats] = await Promise.all([
        Nivel.findAll({
          attributes: [
            [Nivel.sequelize.fn('COUNT', Nivel.sequelize.col('id')), 'total'],
            [Nivel.sequelize.fn('COUNT', Nivel.sequelize.literal('CASE WHEN nombre LIKE \'%Básico%\' THEN 1 END')), 'basicLevels'],
            [Nivel.sequelize.fn('COUNT', Nivel.sequelize.literal('CASE WHEN nombre LIKE \'%Avanzado%\' THEN 1 END')), 'advancedLevels']
          ]
        }),
        Materia.findAll({
          attributes: [
            'nivelId',
            [Materia.sequelize.fn('COUNT', Materia.sequelize.col('id')), 'materiaCount']
          ],
          group: ['nivelId'],
          include: [{
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'nombre']
          }]
        }),
        simulateNetworkLatency()
      ]);

      const totalNiveles = await Nivel.count();
      const totalMaterias = await Materia.count();

      const nivelStats = materiaStats.map(stat => ({
        nivelId: stat.nivelId,
        nivelNombre: stat.nivel.nombre,
        materiaCount: stat.dataValues.materiaCount,
        materiasPorNivel: totalNiveles > 0 ? (stat.dataValues.materiaCount / totalNiveles) : 0
      }));

      res.status(200).json({
        success: true,
        data: {
          total: totalNiveles,
          totalMaterias,
          materiasPorNivel: totalNiveles > 0 ? (totalMaterias / totalNiveles).toFixed(2) : 0,
          nivelStats
        },
        processingInfo: {
          optimized: true,
          timestamp: new Date().toISOString(),
          controller: 'nivel',
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
        message: 'Error al obtener estadísticas de niveles',
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

module.exports = nivelController;