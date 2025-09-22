// controllers_sync/estudianteControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Estudiante, Inscripcion } = require('../models');
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

const estudianteControllerSync = {
  // Obtener todos los estudiantes
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;

      const estudiantes = makeSync(() => Estudiante.findAndCountAll({
        include: [{ model: Inscripcion, as: 'inscripciones', attributes: ['id', 'fecha', 'gestion'] }],
        order: [['id', 'ASC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener estudiantes', error: error.message });
    }
  },

  // Obtener estudiante por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const estudiante = makeSync(() => Estudiante.findByPk(id, {
        include: [{ model: Inscripcion, as: 'inscripciones', attributes: ['id', 'fecha', 'gestion'] }]
      }));

      if (!estudiante) {
        return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });
      }
      res.status(200).json({ success: true, data: estudiante });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener estudiante', error: error.message });
    }
  },

  // Crear nuevo estudiante
  create: (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ success: false, message: 'Errores de validación', errors: errors.array() });
      }

      const { registro, nombre, telefono, fechaNac } = req.body;
      const estudiante = makeSync(() => Estudiante.create({ registro, nombre, telefono, fechaNac }));

      res.status(201).json({ success: true, message: 'Estudiante creado exitosamente', data: estudiante });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'El número de estudiante ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al crear estudiante', error: error.message });
    }
  },

  // Actualizar estudiante
  update: (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ success: false, message: 'Errores de validación', errors: errors.array() });
      }
      const { id } = req.params;
      const { registro, nombre, telefono, fechaNac } = req.body;

      const estudiante = makeSync(() => Estudiante.findByPk(id));
      if (!estudiante) {
        return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });
      }

      const updatedEstudiante = makeSync(() => estudiante.update({ registro, nombre, telefono, fechaNac }));

      res.status(200).json({ success: true, message: 'Estudiante actualizado exitosamente', data: updatedEstudiante });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'El número de estudiante ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al actualizar estudiante', error: error.message });
    }
  },

  // Eliminar estudiante
  delete: (req, res) => {
    try {
      const { id } = req.params;
      const estudiante = makeSync(() => Estudiante.findByPk(id));
      if (!estudiante) {
        return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });
      }

      makeSync(() => estudiante.destroy());

      res.status(200).json({ success: true, message: 'Estudiante eliminado exitosamente' });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al eliminar estudiante', error: error.message });
    }
  }
};

module.exports = estudianteControllerSync;