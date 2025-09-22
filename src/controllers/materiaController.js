const { Materia, PlanEstudio, Nivel, Prerequisito, GrupoMateria } = require('../models');
const { validationResult } = require('express-validator');

const materiaController = {
  // Obtener todas las materias
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { nivelId, planEstudioId } = req.query;

      const where = {};
      if (nivelId) {
        where.nivelId = nivelId;
      }
      if (planEstudioId) {
        where.planEstudioId = planEstudioId;
      }

      const materias = await Materia.findAndCountAll({
        where,
       
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre']
          },
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'nombre']
          },
          {
            model: Prerequisito,
            as: 'prerequisitos',
            attributes: ['id','materia_id', 'requiere_id']
          }          
        ],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: materias.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(materias.count / limit),
          totalItems: materias.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener materias',
        error: error.message
      });
    }
  },

  // Obtener materia por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const materia = await Materia.findByPk(id, {
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin']
          },
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'numNivel']
          },
          {
            model: Prerequisito,
            as: 'prerequisitos',
            attributes: ['id']
          },
          {
            model: GrupoMateria,
            as: 'gruposMateria',
            attributes: ['id', 'grupo', 'estado']
          }
        ]
      });

      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }

      res.status(200).json({
        success: true,
        data: materia
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener materia',
        error: error.message
      });
    }
  },

  // Crear nueva materia
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

      const { nombre, sigla, creditos, planEstudioId, nivelId } = req.body;

      // Verificar que el plan de estudio existe
      const planEstudio = await PlanEstudio.findByPk(planEstudioId);
      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }

      // Verificar que el nivel existe
      const nivel = await Nivel.findByPk(nivelId);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }
      
      const materia = await Materia.create({
        nombre,
        sigla,
        creditos,
        planEstudioId,
        nivelId
      });

      // Obtener la materia con las relaciones incluidas
      const materiaCompleta = await Materia.findByPk(materia.id, {
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre']
          },
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'numNivel']
          }
        ]
      });

      res.status(201).json({
        success: true,
        message: 'Materia creada exitosamente',
        data: materiaCompleta
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'La sigla de la materia ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al crear materia',
        error: error.message
      });
    }
  },

  // Actualizar materia
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
      const { nombre, sigla, creditos, planEstudioId, nivelId } = req.body;

      const materia = await Materia.findByPk(id);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
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

      // Verificar que el nivel existe si se está actualizando
      if (nivelId) {
        const nivel = await Nivel.findByPk(nivelId);
        if (!nivel) {
          return res.status(404).json({
            success: false,
            message: 'Nivel no encontrado'
          });
        }
      }

      await materia.update({
        nombre,
        sigla,
        creditos,
        planEstudioId,
        nivelId
      });

      // Obtener la materia actualizada con las relaciones incluidas
      const materiaActualizada = await Materia.findByPk(id, {
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre']
          },
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'numNivel']
          }
        ]
      });

      res.status(200).json({
        success: true,
        message: 'Materia actualizada exitosamente',
        data: materiaActualizada
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'La sigla de la materia ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al actualizar materia',
        error: error.message
      });
    }
  },

  // Eliminar materia
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const materia = await Materia.findByPk(id);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }

      await materia.destroy();

      res.status(200).json({
        success: true,
        message: 'Materia eliminada exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar materia',
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

      const materia = await Materia.findByPk(id);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: 'Materia no encontrada'
        });
      }

      // Verificar relaciones si están en los datos de actualización
      if (updateData.planEstudioId) {
        const planEstudio = await PlanEstudio.findByPk(updateData.planEstudioId);
        if (!planEstudio) {
          return res.status(404).json({
            success: false,
            message: 'Plan de estudio no encontrado'
          });
        }
      }

      if (updateData.nivelId) {
        const nivel = await Nivel.findByPk(updateData.nivelId);
        if (!nivel) {
          return res.status(404).json({
            success: false,
            message: 'Nivel no encontrado'
          });
        }
      }

      await materia.update(updateData);

      // Obtener la materia actualizada con las relaciones incluidas
      const materiaActualizada = await Materia.findByPk(id, {
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre']
          },
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'numNivel']
          }
        ]
      });

      res.status(200).json({
        success: true,
        message: 'Materia actualizada parcialmente',
        data: materiaActualizada
      });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({
          success: false,
          message: 'La sigla de la materia ya existe'
        });
      }

      res.status(500).json({
        success: false,
        message: 'Error al actualizar materia',
        error: error.message
      });
    }
  },
  // Obtener materias por nivel
  getByNivel: async (req, res) => {
    try {
      const { nivelId } = req.params;
      
      const nivel = await Nivel.findByPk(nivelId);
      if (!nivel) {
        return res.status(404).json({
          success: false,
          message: 'Nivel no encontrado'
        });
      }

      const materias = await Materia.findAll({
        where: { nivelId },
        include: [
          {
            model: PlanEstudio,
            as: 'planEstudio',
            attributes: ['id', 'nombre']
          },
          {
            model: GrupoMateria,
            as: 'gruposMateria',
            attributes: ['id', 'grupo', 'estado']
          }
        ],
        order: [['nombre', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: materias
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener materias del nivel',
        error: error.message
      });
    }
  },

  // Obtener materias por plan de estudio
  getByPlanEstudio: async (req, res) => {
    try {
      const { planEstudioId } = req.params;
      
      const planEstudio = await PlanEstudio.findByPk(planEstudioId);
      if (!planEstudio) {
        return res.status(404).json({
          success: false,
          message: 'Plan de estudio no encontrado'
        });
      }

      const materias = await Materia.findAll({
        where: { planEstudioId },
        include: [
          {
            model: Nivel,
            as: 'nivel',
            attributes: ['id', 'numNivel']
          },
          {
            model: GrupoMateria,
            as: 'gruposMateria',
            attributes: ['id', 'grupo', 'estado']
          }
        ],
        order: [['nivel', 'numNivel', 'ASC'], ['nombre', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: materias
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener materias del nivel',
        error: error.message
      });
    }
  }
};

module.exports = materiaController;
   