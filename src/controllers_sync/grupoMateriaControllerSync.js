// controllers_sync/grupoMateriaControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { GrupoMateria, Materia, Docente, Horario, PlanEstudio, Nivel, Prerequisito } = require("../models");
const { validationResult } = require("express-validator");
const deasync = require('deasync');

// Función auxiliar para convertir una función que retorna una promesa en síncrona
const makeSync = (promiseFn) => {
  let result, error, done = false;
  promiseFn()
    .then(res => { result = res; })
    .catch(err => { error = err; })
    .finally(() => { done = true; });
  deasync.loopWhile(() => !done); // Bucle bloqueante
  if (error) throw error;
  return result;
};

const grupoMateriaControllerSync = {
  // Obtener todos los grupos de materia
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { estado, materiaId, docenteId, horarioId } = req.query;

      const where = {};
      if (estado !== undefined) where.estado = estado === "true";
      if (materiaId) where.materiaId = materiaId;
      if (docenteId) where.docenteId = docenteId;
      if (horarioId) where.horarioId = horarioId;

      const gruposMateria = makeSync(() => GrupoMateria.findAndCountAll({
        where,
        include: [
          { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
          { model: Docente, as: "docente" }, { model: Horario, as: "horario" },
        ],
        order: [["id", "ASC"]],
      }));

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
      res.status(500).json({ success: false, message: "Error al obtener grupos de materia", error: error.message });
    }
  },

  // Obtener grupo de materia por ID
  getById: (req, res) => {
    try {
        const { id } = req.params;
        const grupoMateria = makeSync(() => GrupoMateria.findByPk(id, {
            include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
                { model: Docente, as: "docente" }, { model: Horario, as: "horario" },
            ],
        }));

        if (!grupoMateria) {
            return res.status(404).json({ success: false, message: "Grupo de materia no encontrado" });
        }
        res.status(200).json({ success: true, data: grupoMateria });
    } catch (error) {
        res.status(500).json({ success: false, message: "Error al obtener grupo de materia", error: error.message });
    }
  },

  // Crear nuevo grupo de materia
  create: (req, res) => {
    try {
        const { grupo, estado, materiaId, docenteId } = req.body;

        const materia = makeSync(() => Materia.findByPk(materiaId));
        if (!materia) return res.status(404).json({ success: false, message: "Materia no encontrada" });

        const docente = makeSync(() => Docente.findByPk(docenteId));
        if (!docente) return res.status(404).json({ success: false, message: "Docente no encontrado" });

        const grupoMateria = makeSync(() => GrupoMateria.create({
            grupo, estado: estado !== undefined ? estado : true, materiaId, docenteId,
        }));

        const grupoCompleto = makeSync(() => GrupoMateria.findByPk(grupoMateria.id, {
            include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
                { model: Docente, as: "docente" }, { model: Horario, as: "horario" },
            ],
        }));

        res.status(201).json({ success: true, message: "Grupo de materia creado exitosamente", data: grupoCompleto });
    } catch (error) {
        res.status(500).json({ success: false, message: "Error al crear grupo de materia", error: error.message });
    }
  },
  
  // Actualizar grupo de materia
  update: (req, res) => {
    try {
        const { id } = req.params;
        const { grupo, estado, materiaId, docenteId } = req.body;

        const grupoMateriaInstance = makeSync(() => GrupoMateria.findByPk(id));
        if (!grupoMateriaInstance) return res.status(404).json({ success: false, message: "Grupo de materia no encontrado" });

        if (materiaId) {
            const materia = makeSync(() => Materia.findByPk(materiaId));
            if (!materia) return res.status(404).json({ success: false, message: "Materia no encontrada" });
        }

        if (docenteId) {
            const docente = makeSync(() => Docente.findByPk(docenteId));
            if (!docente) return res.status(404).json({ success: false, message: "Docente no encontrado" });
        }

        makeSync(() => grupoMateriaInstance.update({ grupo, estado, materiaId, docenteId }));
        
        const grupoActualizado = makeSync(() => GrupoMateria.findByPk(id, {
             include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
                { model: Docente, as: "docente" }, { model: Horario, as: "horario" },
            ],
        }));

        res.status(200).json({ success: true, message: "Grupo de materia actualizado exitosamente", data: grupoActualizado });
    } catch(error) {
        res.status(500).json({ success: false, message: "Error al actualizar grupo de materia", error: error.message });
    }
  },

  // Eliminar grupo de materia
  delete: (req, res) => {
    try {
        const { id } = req.params;
        const grupoMateria = makeSync(() => GrupoMateria.findByPk(id));
        if (!grupoMateria) {
            return res.status(404).json({ success: false, message: "Grupo de materia no encontrado" });
        }
        
        makeSync(() => grupoMateria.destroy());
        
        res.status(200).json({ success: true, message: "Grupo de materia eliminado exitosamente" });
    } catch(error) {
        res.status(500).json({ success: false, message: "Error al eliminar grupo de materia", error: error.message });
    }
  },
  
  // Actualización parcial con PATCH
  patch: (req, res) => {
      try {
        const { id } = req.params;
        const updateData = req.body;

        const grupoMateriaInstance = makeSync(() => GrupoMateria.findByPk(id));
        if (!grupoMateriaInstance) return res.status(404).json({ success: false, message: "Grupo de materia no encontrado" });

        if (updateData.materiaId) {
            const materia = makeSync(() => Materia.findByPk(updateData.materiaId));
            if (!materia) return res.status(404).json({ success: false, message: "Materia no encontrada" });
        }
        
        if (updateData.docenteId) {
            const docente = makeSync(() => Docente.findByPk(updateData.docenteId));
            if (!docente) return res.status(404).json({ success: false, message: "Docente no encontrado" });
        }

        makeSync(() => grupoMateriaInstance.update(updateData));

        const grupoActualizado = makeSync(() => GrupoMateria.findByPk(id, {
            include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
                { model: Docente, as: "docente" }, { model: Horario, as: "horario" },
            ],
        }));
        
        res.status(200).json({ success: true, message: "Grupo de materia actualizado parcialmente", data: grupoActualizado });
      } catch(error) {
          res.status(500).json({ success: false, message: "Error al actualizar grupo de materia", error: error.message });
      }
  },

  // Obtener grupos por materia
  getByMateria: (req, res) => {
    try {
        const { materiaId } = req.params;
        const materia = makeSync(() => Materia.findByPk(materiaId));
        if (!materia) {
            return res.status(404).json({ success: false, message: "Materia no encontrada" });
        }

        const grupos = makeSync(() => GrupoMateria.findAll({
            where: { materiaId },
            include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] },
                { model: Docente, as: "docente" }, { model: Horario, as: "horario" }
            ],
            order: [["grupo", "ASC"]]
        }));
        
        res.status(200).json({ success: true, data: grupos });
    } catch (error) {
        res.status(500).json({ success: false, message: "Error al obtener grupos de la materia", error: error.message });
    }
  },

  // Obtener grupos por docente
  getByDocente: (req, res) => {
    try {
        const { docenteId } = req.params;
        const docente = makeSync(() => Docente.findByPk(docenteId));
        if (!docente) {
            return res.status(404).json({ success: false, message: "Docente no encontrado" });
        }

        const grupos = makeSync(() => GrupoMateria.findAll({
            where: { docenteId },
            include: [
                { model: Materia, as: "materia", include: [{ model: PlanEstudio, as: "planEstudio" }, { model: Nivel, as: "nivel" }, { model: Prerequisito, as: "prerequisitos" }] }
            ],
            order: [["grupo", "ASC"]]
        }));
        
        res.status(200).json({ success: true, data: grupos });
    } catch (error) {
        res.status(500).json({ success: false, message: "Error al obtener grupos del docente", error: error.message });
    }
  },
};

module.exports = grupoMateriaControllerSync;