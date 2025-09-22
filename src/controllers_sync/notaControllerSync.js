// controllers_sync/notaControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Nota, GrupoMateria, Estudiante, Materia, Docente, PlanEstudio, Nivel, Prerequisito } = require('../models');
const { validationResult } = require('express-validator');
const { Op } = require('sequelize');
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

const notaControllerSync = {
  // Obtener todas las notas
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { grupoMateriaId, notaMinima } = req.query;

      const where = {};
      if (grupoMateriaId) where.grupoMateriaId = grupoMateriaId;
      if (notaMinima) where.notaFinal = { [Op.gte]: parseFloat(notaMinima) };

      const notas = makeSync(() => Nota.findAndCountAll({
        where,
        attributes: { exclude: ["grupoMateriaId", "estudianteId"] },
        include: [
            { model: GrupoMateria, as: 'grupoMateria', include: [{model: Materia, as: 'materia', include: [ { model: PlanEstudio, as: 'planEstudio'}, {model: Nivel, as: 'nivel'}, {model: Prerequisito, as: 'prerequisitos'}]}, {model: Docente, as: 'docente'}] },
            { model: Estudiante, as: 'estudiante' }
        ],
        order: [['id', 'ASC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener notas', error: error.message });
    }
  },

  // Obtener nota por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const nota = makeSync(() => Nota.findByPk(id, {
        include: [
          { model: GrupoMateria, as: 'grupoMateria' },
          { model: Estudiante, as: 'estudiante' }
        ]
      }));

      if (!nota) {
        return res.status(404).json({ success: false, message: 'Nota no encontrada' });
      }
      res.status(200).json({ success: true, data: nota });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener nota', error: error.message });
    }
  },

  // Crear nueva nota
  create: (req, res) => {
    try {
      const { notaFinal, observacion, grupoMateriaId, estudianteId } = req.body;

      const grupoMateria = makeSync(() => GrupoMateria.findByPk(grupoMateriaId));
      if (!grupoMateria) return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });

      const notaExistente = makeSync(() => Nota.findOne({ where: { grupoMateriaId, estudianteId } }));
      if (notaExistente) return res.status(400).json({ success: false, message: 'Ya existe una nota registrada para este estudiante en este grupo' });
      
      const nota = makeSync(() => Nota.create({ notaFinal, observacion, grupoMateriaId, estudianteId }));
      
      const notaCompleta = makeSync(() => Nota.findByPk(nota.id, {
        include: [
          { model: GrupoMateria, as: 'grupoMateria', include: [{model: Materia, as: 'materia', include: [ { model: PlanEstudio, as: 'planEstudio'}, {model: Nivel, as: 'nivel'}, {model: Prerequisito, as: 'prerequisitos'}]}, {model: Docente, as: 'docente'}] },
          { model: Estudiante, as: 'estudiante' }
        ]
      }));

      res.status(201).json({ success: true, message: 'Nota creada exitosamente', data: notaCompleta });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al crear nota', error: error.message });
    }
  },

  // Actualizar nota
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { notaFinal, observacion, grupoMateriaId } = req.body;

      const notaInstance = makeSync(() => Nota.findByPk(id));
      if (!notaInstance) return res.status(404).json({ success: false, message: 'Nota no encontrada' });

      if (grupoMateriaId) {
        const grupoMateria = makeSync(() => GrupoMateria.findByPk(grupoMateriaId));
        if (!grupoMateria) return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });
      }

      makeSync(() => notaInstance.update({ notaFinal, observacion, grupoMateriaId }));

      const notaActualizada = makeSync(() => Nota.findByPk(id, {
        include: [ { model: GrupoMateria, as: 'grupoMateria' }, { model: Estudiante, as: 'estudiante' } ]
      }));

      res.status(200).json({ success: true, message: 'Nota actualizada exitosamente', data: notaActualizada });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar nota', error: error.message });
    }
  },

  // Eliminar nota
  delete: (req, res) => {
    try {
      const { id } = req.params;
      const nota = makeSync(() => Nota.findByPk(id));
      if (!nota) {
        return res.status(404).json({ success: false, message: 'Nota no encontrada' });
      }
      makeSync(() => nota.destroy());
      res.status(200).json({ success: true, message: 'Nota eliminada exitosamente' });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al eliminar nota', error: error.message });
    }
  },

  // Actualización parcial con PATCH
  patch: (req, res) => {
    try {
      const { id } = req.params;
      const updateData = req.body;
      
      const notaInstance = makeSync(() => Nota.findByPk(id));
      if (!notaInstance) return res.status(404).json({ success: false, message: 'Nota no encontrada' });

      if (updateData.grupoMateriaId) {
        const grupoMateria = makeSync(() => GrupoMateria.findByPk(updateData.grupoMateriaId));
        if (!grupoMateria) return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });
      }

      makeSync(() => notaInstance.update(updateData));

      const notaActualizada = makeSync(() => Nota.findByPk(id, {
        include: [ { model: GrupoMateria, as: 'grupoMateria' }, { model: Estudiante, as: 'estudiante' } ]
      }));

      res.status(200).json({ success: true, message: 'Nota actualizada parcialmente', data: notaActualizada });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar nota', error: error.message });
    }
  },

  // Obtener notas por grupo de materia
  getByGrupoMateria: (req, res) => {
    try {
      const { grupoMateriaId } = req.params;
      const grupoMateria = makeSync(() => GrupoMateria.findByPk(grupoMateriaId));
      if (!grupoMateria) {
        return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });
      }

      const notas = makeSync(() => Nota.findAll({
        where: { grupoMateriaId },
        include: [{ model: Estudiante, as: 'estudiante', attributes: ['id', 'nombre', 'registro'] }],
        order: [['notaFinal', 'DESC']]
      }));
      
      res.status(200).json({ success: true, data: notas });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener notas del grupo', error: error.message });
    }
  },
};

module.exports = notaControllerSync;