const {
  GrupoMateria,
  Materia,
  Docente,
  Horario,
  Nota,
  PlanEstudio,
  Nivel,
  Prerequisito,
} = require("../models");
const { validationResult } = require("express-validator");

const grupoMateriaController = {
  // Obtener todos los grupos de materia
  getAll: async (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const offset = (page - 1) * limit;
      const { cupo, estado, materiaId, docenteId, horarioId } = req.query;

      const where = {};

      if (cupo) {
        where.cupo = cupo;
      }

      if (estado !== undefined) {
        where.estado = estado === "true";
      }
      if (materiaId) {
        // Asumiendo que esta columna también es snake_case en la BD
        where.materia_id = materiaId;
      }
      if (docenteId) {
        // Asumiendo que esta columna también es snake_case en la BD
        where.docente_id = docenteId;
      }
      if (horarioId) {
        // Utiliza el nombre real de la columna de la base de datos
        where.horario_id = horarioId;
      }

      const gruposMateria = await GrupoMateria.findAndCountAll({
        where,
        
        attributes: { exclude: ["materiaId", "docenteId", "horarioId"] },
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: ["id", "nombre", "sigla", "creditos"],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "horaInicio", "horaFin"],
          },
        ],
        order: [["id", "ASC"]],
      });

      res.status(200).json({
        success: true,
        data: gruposMateria.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(gruposMateria.count / limit),
          totalItems: gruposMateria.count,
          itemsPerPage: limit,
        },
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al obtener grupos de materia",
        error: error.message,
      });
    }
  },

  // Obtener grupo de materia por ID
  getById: async (req, res) => {
    try {
      const { id } = req.params;
      const grupoMateria = await GrupoMateria.findByPk(id, {
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "horaInicio", "horaFin"],
          },
        ],
      });

      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: "Grupo de materia no encontrado",
        });
      }

      res.status(200).json({
        success: true,
        data: grupoMateria,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al obtener grupo de materia",
        error: error.message,
      });
    }
  },

  // Crear nuevo grupo de materia
  create: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Errores de validación",
          errors: errors.array(),
        });
      }

      const { cupo, grupo, estado, materiaId, docenteId } = req.body;

      // Verificar que la materia existe
      const materia = await Materia.findByPk(materiaId);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: "Materia no encontrada",
        });
      }

      // Verificar que el docente existe
      const docente = await Docente.findByPk(docenteId);
      if (!docente) {
        return res.status(404).json({
          success: false,
          message: "Docente no encontrado",
        });
      }

      const grupoMateria = await GrupoMateria.create({
        cupo,
        grupo,
        estado: estado !== undefined ? estado : true,
        materiaId,
        docenteId,
      });

      // Obtener el grupo con las relaciones incluidas
      const grupoCompleto = await GrupoMateria.findByPk(grupoMateria.id, {
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "horaInicio", "horaFin"],
          },
        ],
      });

      res.status(201).json({
        success: true,
        message: "Grupo de materia creado exitosamente",
        data: grupoCompleto,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al crear grupo de materia",
        error: error.message,
      });
    }
  },

  // Actualizar grupo de materia
  update: async (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({
          success: false,
          message: "Errores de validación",
          errors: errors.array(),
        });
      }

      const { id } = req.params;
      const { cupo, grupo, estado, materiaId, docenteId } = req.body;

      const grupoMateria = await GrupoMateria.findByPk(id);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: "Grupo de materia no encontrado",
        });
      }

      // Verificar que la materia existe si se está actualizando
      if (materiaId) {
        const materia = await Materia.findByPk(materiaId);
        if (!materia) {
          return res.status(404).json({
            success: false,
            message: "Materia no encontrada",
          });
        }
      }

      // Verificar que el docente existe si se está actualizando
      if (docenteId) {
        const docente = await Docente.findByPk(docenteId);
        if (!docente) {
          return res.status(404).json({
            success: false,
            message: "Docente no encontrado",
          });
        }
      }

      await grupoMateria.update({
        cupo,
        grupo,
        estado,
        materiaId,
        docenteId,
      });

      // Obtener el grupo actualizado con las relaciones incluidas
      const grupoActualizado = await GrupoMateria.findByPk(id, {
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "horaInicio", "horaFin"],
          },
        ],
      });

      res.status(200).json({
        success: true,
        message: "Grupo de materia actualizado exitosamente",
        data: grupoActualizado,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al actualizar grupo de materia",
        error: error.message,
      });
    }
  },

  // Eliminar grupo de materia
  delete: async (req, res) => {
    try {
      const { id } = req.params;

      const grupoMateria = await GrupoMateria.findByPk(id);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: "Grupo de materia no encontrado",
        });
      }

      await grupoMateria.destroy();

      res.status(200).json({
        success: true,
        message: "Grupo de materia eliminado exitosamente",
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al eliminar grupo de materia",
        error: error.message,
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
          message: "Errores de validación",
          errors: errors.array(),
        });
      }

      const { id } = req.params;
      const updateData = req.body;

      const grupoMateria = await GrupoMateria.findByPk(id);
      if (!grupoMateria) {
        return res.status(404).json({
          success: false,
          message: "Grupo de materia no encontrado",
        });
      }

      // Verificar relaciones si están en los datos de actualización
      if (updateData.materiaId) {
        const materia = await Materia.findByPk(updateData.materiaId);
        if (!materia) {
          return res.status(404).json({
            success: false,
            message: "Materia no encontrada",
          });
        }
      }

      if (updateData.docenteId) {
        const docente = await Docente.findByPk(updateData.docenteId);
        if (!docente) {
          return res.status(404).json({
            success: false,
            message: "Docente no encontrado",
          });
        }
      }

      await grupoMateria.update(updateData);

      // Obtener el grupo actualizado con las relaciones incluidas
      const grupoActualizado = await GrupoMateria.findByPk(id, {
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "horaInicio", "horaFin"],
          },
        ],
      });

      res.status(200).json({
        success: true,
        message: "Grupo de materia actualizado parcialmente",
        data: grupoActualizado,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al actualizar grupo de materia",
        error: error.message,
      });
    }
  },

  // Obtener grupos por materia
  getByMateria: async (req, res) => {
    try {
      const { materiaId } = req.params;

      const materia = await Materia.findByPk(materiaId);
      if (!materia) {
        return res.status(404).json({
          success: false,
          message: "Materia no encontrada",
        });
      }

      const grupos = await GrupoMateria.findAll({
        where: { materiaId },
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
          {
            model: Docente,
            as: "docente",
            attributes: ["id", "nombre", "telefono"],
          },
          {
            model: Horario,
            as: "horario",
            attributes: ["id", "dia", "fechaIni", "fechaFinal"],
          },
        ],
        order: [["grupo", "ASC"]],
      });

      res.status(200).json({
        success: true,
        data: grupos,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al obtener grupos de la materia",
        error: error.message,
      });
    }
  },

  // Obtener grupos por docente
  getByDocente: async (req, res) => {
    try {
      const { docenteId } = req.params;

      const docente = await Docente.findByPk(docenteId);
      if (!docente) {
        return res.status(404).json({
          success: false,
          message: "Docente no encontrado",
        });
      }

      const grupos = await GrupoMateria.findAll({
        where: { docenteId },
        include: [
          {
            model: Materia,
            as: "materia",
            attributes: [
              "id",
              "nombre",
              "sigla",
              "creditos",
              "nivelId",
              "planEstudioId",
            ],
            include: [
              {
                model: PlanEstudio,
                as: "planEstudio",
                attributes: ["id", "nombre"],
              },
              {
                model: Nivel,
                as: "nivel",
                attributes: ["id", "nombre"],
              },
              {
                model: Prerequisito,
                as: "prerequisitos",
                attributes: ["id", "materia_id", "requiere_id"],
              },
            ],
          },
         
        ],
        order: [["grupo", "ASC"]],
      });

      res.status(200).json({
        success: true,
        data: grupos,
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: "Error al obtener grupos del docente",
        error: error.message,
      });
    }
  },
};

module.exports = grupoMateriaController;
