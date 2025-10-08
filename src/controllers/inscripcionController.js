const { Inscripcion, Estudiante, GrupoMateria, Materia, Docente, PlanEstudio, Nivel, Prerequisito } = require('../models');
const { validationResult } = require('express-validator');
const QueueService = require("../services/QueueService");

let queueServiceInstance = null;

const getQueueService = () => {
  if (!queueServiceInstance) {
    queueServiceInstance = new QueueService();
  }
  return queueServiceInstance;
};

const inscripcionController = {


requestSeat: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Validation errors",
          errors: errors.array(),
        });
      }

      const { estudianteId, grupoMateriaId, gestion } = req.body;

      console.log("🎓 Solicitud de inscripción:");
      console.log("  Estudiante ID:", estudianteId);
      console.log("  Grupo Materia ID:", grupoMateriaId);
      console.log("  Gestión:", gestion);

      const grupo = await GrupoMateria.findByPk(grupoMateriaId);
      
      if (!grupo) {
        return res.status(404).json({
          success: false,
          status: 'rejected',
          reason: 'group_not_found',
          message: `Grupo Materia ${grupoMateriaId} no encontrado`
        });
      }

      if (!grupo.estado) {
        return res.status(400).json({
          success: false,
          status: 'rejected',
          reason: 'group_inactive',
          message: `Grupo ${grupo.grupo} está inactivo`
        });
      }

      if (grupo.cupo <= 0) {
        return res.status(409).json({
          success: false,
          status: 'rejected',
          reason: 'no_seats_available',
          message: `Sin cupos disponibles en grupo ${grupo.grupo}`,
          //cuposRestantes: 0
        });
      }

      const existingInscription = await Inscripcion.findOne({
        where: {
          estudianteId,
          grupoMateriaId
        }
      });





      if (existingInscription) {
        return res.status(409).json({
          success: false,
          status: 'rejected',
          reason: 'already_enrolled',
          message: `Estudiante ${estudianteId} ya está inscrito en este grupo`
        });
      }

      const service = getQueueService();
      
      const result = await service.enqueueTaskAutoBalance({
        type: 'inscription',
        model: 'Inscripcion',
        operation: 'requestSeat',
        data: {
          estudianteId,
          grupoMateriaId,
          gestion: gestion || new Date().getFullYear()
        }
      });

      console.log(`✅ Solicitud encolada en: ${result.queueName} (cupos disponibles: ${grupo.cupo})`);

      res.status(202).json({
        ...result,
        message: 'Seat request queued successfully',
        status: 'pending'
        //cuposDisponibles: grupo.cupo
      });
    } catch (error) {
      console.error("Error in requestSeat:", error);
      res.status(500).json({
        success: false,
        message: "Error requesting seat",
        error: error.message,
      });
    }
  },

  // Obtener todas las inscripciones
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { gestion } = req.query;

      const where = {};
      if (gestion) {
        where.gestion = gestion;
      }

      const inscripciones = await Inscripcion.findAndCountAll({
        where,
        
        include: [
          {
            model: Estudiante,
            as: 'estudiante',
            attributes: ['id', 'registro', 'nombre', 'telefono']
          },
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
          }
        ],
        order: [['fecha', 'DESC']]
      });

      res.status(200).json({
        success: true,
        data: inscripciones.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(inscripciones.count / limit),
          totalItems: inscripciones.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener inscripciones',
        error: error.message
      });
    }
  },

  // Obtener inscripción por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const inscripcion = await Inscripcion.findByPk(id, {
        include: [
          {
            model: Estudiante,
            as: 'estudiante',
            attributes: ['id', 'registro', 'nombre', 'telefono', 'fechaNac']
          },
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
          }
        ]
      });

      if (!inscripcion) {
        return res.status(404).json({
          success: false,
          message: 'Inscripción no encontrada'
        });
      }

      res.status(200).json({
        success: true,
        data: inscripcion
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener inscripción',
        error: error.message
      });
    }
  },

  // Crear nueva inscripción
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

      const { fecha, gestion, estudianteId, grupoMateriaId } = req.body;

      // Verificar que el estudiante existe
      const estudiante = await Estudiante.findByPk(estudianteId);
      if (!estudiante) {
        return res.status(404).json({
          success: false,
          message: 'Estudiante no encontrado'
        });
      }

      // Verificar que el grupo de materia existe
      const grupoMateria = await GrupoMateria.findByPk(grupoMateriaId);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: 'Grupo de materia no encontrado'
        });
      }

      const inscripcion = await Inscripcion.create({
        fecha: fecha || new Date(),
        gestion,
        estudianteId,
        grupoMateriaId
      });

      // Obtener la inscripción con todas las relaciones incluidas
      const inscripcionCompleta = await Inscripcion.findByPk(inscripcion.id, {
        include: [
          {
            model: Estudiante,
            as: 'estudiante',
            attributes: ['id', 'registro', 'nombre', 'telefono']
          },
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
          }
        ]
      });

      res.status(201).json({
        success: true,
        message: 'Inscripción creada exitosamente',
        data: inscripcionCompleta
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al crear inscripción',
        error: error.message
      });
    }
  },

  // Actualizar inscripción
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
      const { fecha, gestion, estudianteId, grupoMateriaId } = req.body;

      const inscripcion = await Inscripcion.findByPk(id);
      if (!inscripcion) {
        return res.status(404).json({
          success: false,
          message: 'Inscripción no encontrada'
        });
      }

      // Verificar que el estudiante existe si se está actualizando
      if (estudianteId) {
        const estudiante = await Estudiante.findByPk(estudianteId);
        if (!estudiante) {
          return res.status(404).json({
            success: false,
            message: 'Estudiante no encontrado'
          });
        }
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

      await inscripcion.update({
        fecha,
        gestion,
        estudianteId,
        grupoMateriaId
      });

      // Obtener la inscripción actualizada con todas las relaciones incluidas
      const inscripcionActualizada = await Inscripcion.findByPk(id, {
        include: [
          {
            model: Estudiante,
            as: 'estudiante',
            attributes: ['id', 'registro', 'nombre', 'telefono']
          },
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
          }
        ]
      });

      res.status(200).json({
        success: true,
        message: 'Inscripción actualizada exitosamente',
        data: inscripcionActualizada
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al actualizar inscripción',
        error: error.message
      });
    }
  },

  // Eliminar inscripción
  delete: async (req, res) => {
    try {
      const { id } = req.params;
      
      const inscripcion = await Inscripcion.findByPk(id);
      if (!inscripcion) {
        return res.status(404).json({
          success: false,
          message: 'Inscripción no encontrada'
        });
      }

      await inscripcion.destroy();

      res.status(200).json({
        success: true,
        message: 'Inscripción eliminada exitosamente'
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al eliminar inscripción',
        error: error.message
      });
    }
  },

  // Obtener inscripciones por estudiante
  getByEstudiante: async (req, res) => {
    try {
      const { estudianteId } = req.params;
      
      const estudiante = await Estudiante.findByPk(estudianteId);
      if (!estudiante) {
        return res.status(404).json({
          success: false,
          message: 'Estudiante no encontrado'
        });
      }

      const inscripciones = await Inscripcion.findAll({
        where: { estudianteId },
        include: [
          {
            model: Estudiante,
            as: 'estudiante',
            attributes: ['id', 'registro', 'nombre', 'telefono']
          },
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
          }
        ],
        order: [['gestion', 'DESC'], ['fecha', 'DESC']]
      });

      res.status(200).json({
        success: true,
        data: inscripciones
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: 'Error al obtener inscripciones del estudiante',
        error: error.message
      });
    }
  }
};

module.exports = inscripcionController;