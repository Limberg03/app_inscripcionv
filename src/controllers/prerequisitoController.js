const { Prerequisito, Materia } = require('../models');
const { validationResult } = require('express-validator');

const prerequisitoController = {
  // Obtener todos los prerequisitos
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { materiaId } = req.query;

      const where = {};
      if (materiaId) {
        where.materiaId = materiaId;
      }

      const prerequisitos = await Prerequisito.findAndCountAll({
        where,
        // limit,
        // offset,
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: prerequisitos.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(prerequisitos.count / limit),
          totalItems: prerequisitos.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener prerequisitos',
        error: error.message
      });
    }
  },

  // Obtener prerequisito por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const prerequisito = await Prerequisito.findByPk(id, {
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }]
      });

      if (!prerequisito) {
        return res.status(404).json({
          success: false,
          message: 'Prerequisito no encontrado'
        });
      }

      res.status(200).json({
        success: true,
        data: prerequisito
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener prerequisito',
        error: error.message
      });
    }
  },

  // Crear nuevo prerequisito
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

      const { materiaId } = req.body;

      // Verificar que la materia existe
      const materia = await Materia.findByPk(materiaId);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }
      
      const prerequisito = await Prerequisito.create({
        materiaId
      });

      // Obtener el prerequisito con la materia incluida
      const prerequisitoCompleto = await Prerequisito.findByPk(prerequisito.id, {
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }]
      });

      res.status(201).json({
        success: true,
        message: 'Prerequisito creado exitosamente',
        data: prerequisitoCompleto
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al crear prerequisito',
        error: error.message
      });
    }
  },

  // Actualizar prerequisito
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
      const { materiaId } = req.body;

      const prerequisito = await Prerequisito.findByPk(id);
      if (!prerequisito) {
        return res.status(404).json({
          success: false,
          message: 'Prerequisito no encontrado'
        });
      }

      // Verificar que la materia existe
      const materia = await Materia.findByPk(materiaId);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }

      await prerequisito.update({
        materiaId
      });

      // Obtener el prerequisito actualizado con la materia incluida
      const prerequisitoActualizado = await Prerequisito.findByPk(id, {
        include: [{
          model: Materia,
          as: 'materia',
          attributes: ['id', 'nombre', 'sigla', 'creditos']
        }]
      });

      res.status(200).json({
        success: true,
        message: 'Prerequisito actualizado exitosamente',
        data: prerequisitoActualizado
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar prerequisito',
        error: error.message
      });
    }
  },

  // Eliminar prerequisito
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const prerequisito = await Prerequisito.findByPk(id);
      if (!prerequisito) {
        return res.status(404).json({
          success: false,
          message: 'Prerequisito no encontrado'
        });
      }

      await prerequisito.destroy();

      res.status(200).json({
        success: true,
        message: 'Prerequisito eliminado exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar prerequisito',
        error: error.message
      });
    }
  },

  // Obtener prerequisitos por materia
  getByMateria: async (req, res) => {
    try {
      const { materiaId } = req.params;
      
      const materia = await Materia.findByPk(materiaId);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }

      const prerequisitos = await Prerequisito.findAll({
        where: { materiaId },
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: prerequisitos
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener prerequisitos de la materia',
        error: error.message
      });
    }
  }
};

module.exports = prerequisitoController;