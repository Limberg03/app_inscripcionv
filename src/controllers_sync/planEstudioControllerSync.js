// controllers_sync/planEstudioControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { PlanEstudio, Carrera } = require('../models');
const { validationResult } = require('express-validator');
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

const planEstudioControllerSync = {
  // Obtener todos los planes de estudio
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;

      const planesEstudio = makeSync(() => PlanEstudio.findAndCountAll({
        include: [{ model: Carrera, as: 'carreras', attributes: ['id', 'nombre', 'codigo', 'modalidad', 'estado'] }],
        order: [['id', 'ASC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener planes de estudio', error: error.message });
    }
  },

  // Obtener plan de estudio por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const planEstudio = makeSync(() => PlanEstudio.findByPk(id, {
        include: [{ model: Carrera, as: 'carreras', attributes: ['id', 'nombre', 'codigo', 'modalidad', 'estado'] }]
      }));

      if (!planEstudio) {
        return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
      }
      res.status(200).json({ success: true, data: planEstudio });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener plan de estudio', error: error.message });
    }
  },

  // Crear nuevo plan de estudio
  create: (req, res) => {
    try {
      const { nombre, fechaInicio, fechaFin } = req.body;
      const planEstudio = makeSync(() => PlanEstudio.create({ nombre, fechaInicio, fechaFin }));

      res.status(201).json({ success: true, message: 'Plan de estudio creado exitosamente', data: planEstudio });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al crear plan de estudio', error: error.message });
    }
  },

  // Actualizar plan de estudio completo
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { nombre, fechaInicio, fechaFin } = req.body;

      const planEstudio = makeSync(() => PlanEstudio.findByPk(id));
      if (!planEstudio) {
        return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
      }

      const updatedPlan = makeSync(() => planEstudio.update({ nombre, fechaInicio, fechaFin }));

      res.status(200).json({ success: true, message: 'Plan de estudio actualizado exitosamente', data: updatedPlan });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar plan de estudio', error: error.message });
    }
  },

  // Actualización parcial con PATCH
  patch: (req, res) => {
    try {
      const { id } = req.params;
      const updateData = req.body;

      const planEstudio = makeSync(() => PlanEstudio.findByPk(id));
      if (!planEstudio) {
        return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
      }

      const updatedPlan = makeSync(() => planEstudio.update(updateData));

      res.status(200).json({ success: true, message: 'Plan de estudio actualizado parcialmente', data: updatedPlan });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar plan de estudio', error: error.message });
    }
  }
};

module.exports = planEstudioControllerSync;