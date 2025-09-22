const { Nota, GrupoMateria, Estudiante,Materia,
  Docente,  
  PlanEstudio,
  Nivel,
  Prerequisito } = require('../models');
const { validationResult } = require('express-validator');

const notaController = {
  // Obtener todas las notas
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { grupoMateriaId, notaMinima } = req.query;

      const where = {};
      if (grupoMateriaId) {
        where.grupoMateriaId = grupoMateriaId;
      }
      
      if (notaMinima) {
        where.notaFinal = { [require('sequelize').Op.gte]: parseFloat(notaMinima) };
      }

      const notas = await Nota.findAndCountAll({
        where,
        // limit,
        // offset,
         attributes: { exclude: ["grupoMateriaId", "estudianteId"] },
        include: [
          {
             model: GrupoMateria,
            as: 'grupoMateria',
            attributes: ['id', 'grupo', 'estado'],
            include: [
              {
                model: Materia,
                as: 'materia',
                attributes: ['id', 'nombre', 'sigla', 'creditos'],
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
                    attributes: ['id', 'materia_id', 'requiere_id']
                  }
                ]
              },
              {
                model: Docente,
                as: 'docente',
                attributes: ['id', 'nombre', 'telefono']
              }
            ]
          },
          {
            
           
              model: Estudiante,
              as: 'estudiante',
              attributes: ['id', 'nombre', 'registro']          
          }
        ],
        order: [['id', 'ASC']]
      });

      res.status(200).json({
        success: true,
        data: notas.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(notas.count / limit),
          totalItems: notas.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener notas',
        error: error.message
      });
    }
  },

  // Obtener nota por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const nota = await Nota.findByPk(id, {
        include: [
          {
            model: GrupoMateria,
            as: 'grupoMateria',
            attributes: ['id', 'grupo', 'estado']
          },
          {
            
           
              model: Estudiante,
              as: 'estudiante',
              attributes: ['id', 'nombre', 'registro']          
          }
        ]
      });

      if (!nota) {
        return res.status(404).json({
          success: false,
          message: 'Nota no encontrada'
        });
      }

      res.status(200).json({
        success: true,
        data: nota
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener nota',
        error: error.message
      });
    }
  },

  // Crear nueva nota
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

      const { notaFinal, observacion, grupoMateriaId } = req.body;

      // Verificar que el grupo de materia existe
      const grupoMateria = await GrupoMateria.findByPk(grupoMateriaId);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: 'Grupo de materia no encontrado'
        });
      }

      // Verificar que la inscripción existe
      
      // Verificar que no exista ya una nota para esta combinación
      const notaExistente = await Nota.findOne({
        where: { grupoMateriaId, estudianteId }
      });

      if (notaExistente) {
        return res.status(400).json({
          success: false,
          message: 'Ya existe una nota registrada para este estudiante en este grupo'
        });
      }
      
      const nota = await Nota.create({
        notaFinal,
        observacion,
        grupoMateriaId
        
      });

      // Obtener la nota con las relaciones incluidas
      const notaCompleta = await Nota.findByPk(nota.id, {
        include: [
          {
             model: GrupoMateria,
            as: 'grupoMateria',
            attributes: ['id', 'grupo', 'estado', 'materiaId', 'docenteId'],
            include: [
              {
                model: Materia,
                as: 'materia',
                attributes: ['id', 'nombre', 'sigla', 'creditos', 'nivelId', 'planEstudioId'],
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
                    attributes: ['id', 'materia_id', 'requiere_id']
                  }
                ]
              },
              {
                model: Docente,
                as: 'docente',
                attributes: ['id', 'nombre', 'telefono']
              }
            ]
          },
           {
            
           
              model: Estudiante,
              as: 'estudiante',
              attributes: ['id', 'nombre', 'registro']          
          }
        ]
      });

      res.status(201).json({
        success: true,
        message: 'Nota creada exitosamente',
        data: notaCompleta
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al crear nota',
        error: error.message
      });
    }
  },

  // Actualizar nota
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
      const { notaFinal, observacion, grupoMateriaId, estudianteId } = req.body;

      const nota = await Nota.findByPk(id);
      if (!nota) {
        return res.status(404).json({
          success: false,
          message: 'Nota no encontrada'
        });
      }

      // Verificar que el grupo de materia existe si se está actualizando
      if (grupoMateriaId) {
        const grupoMateria = await GrupoMateria.findByPk(grupoMateriaId);
        if (!grupoMateria) {
          return res.status(404).json({
            success: false,
            message: 'Grupo de materia no encontrado'
          });
        }
      }

     

      await nota.update({
        notaFinal,
        observacion,
        grupoMateriaId
      });

      // Obtener la nota actualizada con las relaciones incluidas
      const notaActualizada = await Nota.findByPk(id, {
        include: [
          {
            model: GrupoMateria,
            as: 'grupoMateria',
            attributes: ['id', 'grupo', 'estado']
          },
         
        ]
      });

      res.status(200).json({
        success: true,
        message: 'Nota actualizada exitosamente',
        data: notaActualizada
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar nota',
        error: error.message
      });
    }
  },

  // Eliminar nota
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const nota = await Nota.findByPk(id);
      if (!nota) {
        return res.status(404).json({
          success: false,
          message: 'Nota no encontrada'
        });
      }

      await nota.destroy();

      res.status(200).json({
        success: true,
        message: 'Nota eliminada exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar nota',
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

      const nota = await Nota.findByPk(id);
      if (!nota) {
        return res.status(404).json({
          success: false,
          message: 'Nota no encontrada'
        });
      }

      // Verificar relaciones si están en los datos de actualización
      if (updateData.grupoMateriaId) {
        const grupoMateria = await GrupoMateria.findByPk(updateData.grupoMateriaId);
        if (!grupoMateria) {
          return res.status(404).json({
            success: false,
            message: 'Grupo de materia no encontrado'
          });
        }
      }

     

      await nota.update(updateData);

      // Obtener la nota actualizada con las relaciones incluidas
      const notaActualizada = await Nota.findByPk(id, {
        include: [
          {
            model: GrupoMateria,
            as: 'grupoMateria',
            attributes: ['id', 'grupo', 'estado']
          },
         {
            
           
              model: Estudiante,
              as: 'estudiante',
              attributes: ['id', 'nombre', 'registro']          
          }
        ]
      });

      res.status(200).json({
        success: true,
        message: 'Nota actualizada parcialmente',
        data: notaActualizada
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar nota',
        error: error.message
      });
    }
  },

  // Obtener notas por grupo de materia
  getByGrupoMateria: async (req, res) => {
    try {
      const { grupoMateriaId } = req.params;
      
      const grupoMateria = await GrupoMateria.findByPk(grupoMateriaId);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: 'Grupo de materia no encontrado'
        });
      }

      const notas = await Nota.findAll({
        where: { grupoMateriaId },
        include: [{
            
           
              model: Estudiante,
              as: 'estudiante',
              attributes: ['id', 'nombre', 'registro']          
          }],
        order: [['notaFinal', 'DESC']]
      });

      res.status(200).json({
        success: true,
        data: notas
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener notas del grupo',
        error: error.message
      });
    }
  },
  
};

module.exports = notaController;