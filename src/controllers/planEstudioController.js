const { PlanEstudio, Carrera } = require('../models');
const { validationResult } = require('express-validator');

const planEstudioController = {
  // Obtener todos los planes de estudio
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;

      const planesEstudio = await PlanEstudio.findAndCountAll({
        // limit,
        // offset,
        include: [{
          model: Carrera,
          as: 'carreras',
          attributes: ['id', 'nombre', 'codigo', 'modalidad', 'estado']
        }],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: planesEstudio.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(planesEstudio.count / limit),
          totalItems: planesEstudio.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener planes de estudio',
        error: error.message
      });
    }
  },

  // Obtener plan de estudio por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const planEstudio = await PlanEstudio.findByPk(id, {
        include: [{
          model: Carrera,
          as: 'carreras',
          attributes: ['id', 'nombre', 'codigo', 'modalidad', 'estado']
        }]
      });

      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }

      res.status(200).json({
        success: true,
        data: planEstudio
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener plan de estudio',
        error: error.message
      });
    }
  },

  // Crear nuevo plan de estudio
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

      const { nombre, fechaInicio, fechaFin } = req.body;
      
      const planEstudio = await PlanEstudio.create({
        nombre,
        fechaInicio,
        fechaFin
      });

      res.status(201).json({
        success: true,
        message: 'Plan de estudio creado exitosamente',
        data: planEstudio
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al crear plan de estudio',
        error: error.message
      });
    }
  },

  // Actualizar plan de estudio completo
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
      const { nombre, fechaInicio, fechaFin } = req.body;

      const planEstudio = await PlanEstudio.findByPk(id);
      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }

      await planEstudio.update({
        nombre,
        fechaInicio,
        fechaFin
      });

      res.status(200).json({
        success: true,
        message: 'Plan de estudio actualizado exitosamente',
        data: planEstudio
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar plan de estudio',
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

      const planEstudio = await PlanEstudio.findByPk(id);
      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }

      await planEstudio.update(updateData);

      res.status(200).json({
        success: true,
        message: 'Plan de estudio actualizado parcialmente',
        data: planEstudio
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar plan de estudio',
        error: error.message
      });
    }
  }
};

module.exports = planEstudioController;