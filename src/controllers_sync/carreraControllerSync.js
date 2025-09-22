// controllers_sync/carreraControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Carrera, PlanEstudio } = require('../models');
const { validationResult } = require('express-validator');
const deasync = require('deasync');

// Función auxiliar para convertir una función que retorna una promesa en síncrona
const makeSync = (promiseFn) => {
  let result, error, done = false;
  promiseFn()
    .then(res => { result = res; })
    .catch(err => { error = err; })
    .finally(() => { done = true; });
  deasync.loopWhile(() => !done); // Bucle bloqueante hasta que la promesa se resuelva
  if (error) throw error;
  return result;
};

const carreraControllerSync = {
  // Obtener todas las carreras
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { modalidad, estado } = req.query;

      const where = {};
      if (modalidad) where.modalidad = modalidad;
      if (estado !== undefined) where.estado = estado === 'true';

      const carreras = makeSync(() => Carrera.findAndCountAll({
        where,
        include: [{ model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin'] }],
        order: [['id', 'ASC']]
      }));

      res.status(200).json({
        success: true,
        data: carreras.rows,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(carreras.count / limit),
          totalItems: carreras.count,
          itemsPerPage: limit
        }
      });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener carreras', error: error.message });
    }
  },

  // Obtener carrera por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const carrera = makeSync(() => Carrera.findByPk(id, {
        include: [{ model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin'] }]
      }));

      if (!carrera) {
        return res.status(404).json({ success: false, message: 'Carrera no encontrada' });
      }
      res.status(200).json({ success: true, data: carrera });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener carrera', error: error.message });
    }
  },

  // Crear nueva carrera
  create: (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ success: false, message: 'Errores de validación', errors: errors.array() });
      }

      const { nombre, codigo, modalidad, estado, planEstudioId } = req.body;

      const planEstudio = makeSync(() => PlanEstudio.findByPk(planEstudioId));
      if (!planEstudio) {
        return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
      }
      
      const carrera = makeSync(() => Carrera.create({
        nombre, codigo, modalidad, estado: estado !== undefined ? estado : true, planEstudioId
      }));

      const carreraCompleta = makeSync(() => Carrera.findByPk(carrera.id, {
        include: [{ model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin'] }]
      }));

      res.status(201).json({ success: true, message: 'Carrera creada exitosamente', data: carreraCompleta });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'El código de carrera ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al crear carrera', error: error.message });
    }
  },

  // Actualizar carrera
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { nombre, codigo, modalidad, estado, planEstudioId } = req.body;

      const carrera = makeSync(() => Carrera.findByPk(id));
      if (!carrera) {
        return res.status(404).json({ success: false, message: 'Carrera no encontrada' });
      }

      if (planEstudioId) {
        const planEstudio = makeSync(() => PlanEstudio.findByPk(planEstudioId));
        if (!planEstudio) {
          return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
        }
      }

      makeSync(() => carrera.update({ nombre, codigo, modalidad, estado, planEstudioId }));

      const carreraActualizada = makeSync(() => Carrera.findByPk(id, {
        include: [{ model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin'] }]
      }));

      res.status(200).json({ success: true, message: 'Carrera actualizada exitosamente', data: carreraActualizada });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'El código de carrera ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al actualizar carrera', error: error.message });
    }
  },

  // Eliminar carrera
  delete: (req, res) => {
    try {
      const { id } = req.params;
      const carrera = makeSync(() => Carrera.findByPk(id));
      if (!carrera) {
        return res.status(404).json({ success: false, message: 'Carrera no encontrada' });
      }

      makeSync(() => carrera.destroy());

      res.status(200).json({ success: true, message: 'Carrera eliminada exitosamente' });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al eliminar carrera', error: error.message });
    }
  },

  // Actualización parcial con PATCH
  patch: (req, res) => {
    try {
      const { id } = req.params;
      const updateData = req.body;

      const carrera = makeSync(() => Carrera.findByPk(id));
      if (!carrera) {
        return res.status(404).json({ success: false, message: 'Carrera no encontrada' });
      }

      if (updateData.planEstudioId) {
        const planEstudio = makeSync(() => PlanEstudio.findByPk(updateData.planEstudioId));
        if (!planEstudio) {
          return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
        }
      }

      makeSync(() => carrera.update(updateData));
      
      const carreraActualizada = makeSync(() => Carrera.findByPk(id, {
        include: [{ model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre', 'fechaInicio', 'fechaFin'] }]
      }));

      res.status(200).json({ success: true, message: 'Carrera actualizada parcialmente', data: carreraActualizada });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'El código de carrera ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al actualizar carrera', error: error.message });
    }
  }
};

module.exports = carreraControllerSync;