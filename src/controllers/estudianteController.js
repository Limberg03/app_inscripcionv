const { Estudiante, Inscripcion } = require('../models');
const { validationResult } = require('express-validator');

const estudianteController = {
  // Obtener todos los estudiantes
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;

      const estudiantes = await Estudiante.findAndCountAll({
        
        include: [{
          model: Inscripcion,
          as: 'inscripciones',
          attributes: ['id', 'fecha', 'gestion']
        }],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: estudiantes.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(estudiantes.count / limit),
          totalItems: estudiantes.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener estudiantes',
        error: error.message
      });
    }
  },

  // Obtener estudiante por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const estudiante = await Estudiante.findByPk(id, {
        include: [{
          model: Inscripcion,
          as: 'inscripciones',
          attributes: ['id', 'fecha', 'gestion']
        }]
      });

      if (!estudiante) {
        return res.status(404).json({
          success: false,
          message: 'Estudiante no encontrado'
        });
      }

      res.status(200).json({
        success: true,
        data: estudiante
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener estudiante',
        error: error.message
      });
    }
  },

  // Crear nuevo estudiante
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

      const { registro, nombre, telefono, fechaNac } = req.body;
      
      const estudiante = await Estudiante.create({
        registro,
        nombre,
        telefono,
        fechaNac
      });

      res.status(201).json({
        success: true,
        message: 'Estudiante creado exitosamente',
        data: estudiante
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El número de estudiante ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al crear estudiante',
        error: error.message
      });
    }
  },

  // Actualizar estudiante
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
      const { registro, nombre, telefono, fechaNac } = req.body;

      const estudiante = await Estudiante.findByPk(id);
      if (!estudiante) {
        return res.status(404).json({
          success: false,
          message: 'Estudiante no encontrado'
        });
      }

      await estudiante.update({
        registro,
        nombre,
        telefono,
        fechaNac
      });

      res.status(200).json({
        success: true,
        message: 'Estudiante actualizado exitosamente',
        data: estudiante
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'El número de estudiante ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al actualizar estudiante',
        error: error.message
      });
    }
  },

  // Eliminar estudiante
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const estudiante = await Estudiante.findByPk(id);
      if (!estudiante) {
        return res.status(404).json({
          success: false,
          message: 'Estudiante no encontrado'
        });
      }

      await estudiante.destroy();

      res.status(200).json({
        success: true,
        message: 'Estudiante eliminado exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar estudiante',
        error: error.message
      });
    }
  }
};

module.exports = estudianteController;