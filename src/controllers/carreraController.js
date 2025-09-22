const { Carrera, PlanEstudio } = require('../models');
const { validationResult } = require('express-validator');

const carreraController = {
  // Obtener todas las carreras
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { modalidad, estado } = req.query;

      const where = {};
      if (modalidad) {
        where.modalidad = modalidad;
      }
      if (estado !== undefined) {
        where.estado = estado === 'true';
      }

      const carreras = await Carrera.findAndCountAll({
        where,
        
        include: [{
          model: PlanEstudio,
          as: 'planEstudio',
          attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
        }],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: carreras.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(carreras.count / limit),
          totalItems: carreras.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener carreras',
        error: error.message
      });
    }
  },

  // Obtener carrera por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const carrera = await Carrera.findByPk(id, {
        include: [{
          model: PlanEstudio,
          as: 'planEstudio',
          attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
        }]
      });

      if (!carrera) {
        return res.status(404).json({
          success: false,
          message: 'Carrera no encontrada'
        });
      }

      res.status(200).json({
        success: true,
        data: carrera
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener carrera',
        error: error.message
      });
    }
  },

  // Crear nueva carrera
  create: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { nombre, codigo, modalidad, estado, planEstudioId } = req.body;

      // Verificar que el plan de estudio existe
      const planEstudio = await PlanEstudio.findByPk(planEstudioId);
      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }
      
      const carrera = await Carrera.create({
        nombre,
        codigo,
        modalidad,
        estado: estado !== undefined ? estado : true,
        planEstudioId
      });

      // Obtener la carrera con el plan de estudio incluido
      const carreraCompleta = await Carrera.findByPk(carrera.id, {
        include: [{
          model: PlanEstudio,
          as: 'planEstudio',
          attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
        }]
      });

      res.status(201).json({
        success: true,
        message: 'Carrera creada exitosamente',
        data: carreraCompleta
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El código de carrera ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al crear carrera',
        error: error.message
      });
    }
  },

  // Actualizar carrera
  update: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { id } = req.params;
      const { nombre, codigo, modalidad, estado, planEstudioId } = req.body;

      const carrera = await Carrera.findByPk(id);
      if (!carrera) {
        return res.status(404).json({
          success: false,
          message: 'Carrera no encontrada'
        });
      }

      // Verificar que el plan de estudio existe si se está actualizando
      if (planEstudioId) {
        const planEstudio = await PlanEstudio.findByPk(planEstudioId);
        if (!planEstudio) {
          return res.status(404).json({
            success: false,
            message: 'Plan de estudio no encontrado'
          });
        }
      }

      await carrera.update({
        nombre,
        codigo,
        modalidad,
        estado,
        planEstudioId
      });

      // Obtener la carrera actualizada con el plan de estudio incluido
      const carreraActualizada = await Carrera.findByPk(id, {
        include: [{
          model: PlanEstudio,
          as: 'planEstudio',
          attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
        }]
      });

      res.status(200).json({
        success: true,
        message: 'Carrera actualizada exitosamente',
        data: carreraActualizada
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El código de carrera ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al actualizar carrera',
        error: error.message
      });
    }
  },

  // Eliminar carrera
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const carrera = await Carrera.findByPk(id);
      if (!carrera) {
        return res.status(404).json({
          success: false,
          message: 'Carrera no encontrada'
        });
      }

      await carrera.destroy();

      res.status(200).json({
        success: true,
        message: 'Carrera eliminada exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar carrera',
        error: error.message
      });
    }
  },

  // Actualización parcial con PATCH
  patch: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: 'Errores de validación',
          errors: errors.array()
        });
      }

      const { id } = req.params;
      const updateData = req.body;

      const carrera = await Carrera.findByPk(id);
      if (!carrera) {
        return res.status(404).json({
          success: false,
          message: 'Carrera no encontrada'
        });
      }

      // Verificar que el plan de estudio existe si se está actualizando
      if (updateData.planEstudioId) {
        const planEstudio = await PlanEstudio.findByPk(updateData.planEstudioId);
        if (!planEstudio) {
          return res.status(404).json({
            success: false,
            message: 'Plan de estudio no encontrado'
          });
        }
      }

      await carrera.update(updateData);

      // Obtener la carrera actualizada con el plan de estudio incluido
      const carreraActualizada = await Carrera.findByPk(id, {
        include: [{
          model: PlanEstudio,
          as: 'planEstudio',
          attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
        }]
      });

      res.status(200).json({
        success: true,
        message: 'Carrera actualizada parcialmente',
        data: carreraActualizada
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El código de carrera ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al actualizar carrera',
        error: error.message
      });
    }
  }
};

module.exports = carreraController;