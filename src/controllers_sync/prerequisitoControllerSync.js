// controllers_sync/prerequisitoControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Prerequisito, Materia } = require('../models');
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

const prerequisitoControllerSync = {
  // Obtener todos los prerequisitos
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { materiaId } = req.query;

      const where = {};
      if (materiaId) where.materiaId = materiaId;

      const prerequisitos = makeSync(() => Prerequisito.findAndCountAll({
        where,
        include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }],
        order: [['id', 'ASC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener prerequisitos', error: error.message });
    }
  },

  // Obtener prerequisito por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const prerequisito = makeSync(() => Prerequisito.findByPk(id, {
        include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }]
      }));

      if (!prerequisito) {
        return res.status(404).json({ success: false, message: 'Prerequisito no encontrado' });
      }
      res.status(200).json({ success: true, data: prerequisito });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener prerequisito', error: error.message });
    }
  },

  // Crear nuevo prerequisito
  create: (req, res) => {
    try {
      const { materiaId } = req.body;

      const materia = makeSync(() => Materia.findByPk(materiaId));
      if (!materia) {
        return res.status(404).json({ success: false, message: 'Materia no encontrada' });
      }
      
      const prerequisito = makeSync(() => Prerequisito.create({ materiaId }));

      const prerequisitoCompleto = makeSync(() => Prerequisito.findByPk(prerequisito.id, {
        include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }]
      }));

      res.status(201).json({ success: true, message: 'Prerequisito creado exitosamente', data: prerequisitoCompleto });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al crear prerequisito', error: error.message });
    }
  },

  // Actualizar prerequisito
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { materiaId } = req.body;

      const prerequisito = makeSync(() => Prerequisito.findByPk(id));
      if (!prerequisito) {
        return res.status(404).json({ success: false, message: 'Prerequisito no encontrado' });
      }

      const materia = makeSync(() => Materia.findByPk(materiaId));
      if (!materia) {
        return res.status(404).json({ success: false, message: 'Materia no encontrada' });
      }

      makeSync(() => prerequisito.update({ materiaId }));

      const prerequisitoActualizado = makeSync(() => Prerequisito.findByPk(id, {
        include: [{ model: Materia, as: 'materia', attributes: ['id', 'nombre', 'sigla', 'creditos'] }]
      }));

      res.status(200).json({ success: true, message: 'Prerequisito actualizado exitosamente', data: prerequisitoActualizado });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar prerequisito', error: error.message });
    }
  },

  // Eliminar prerequisito
  delete: (req, res) => {
    try {
      const { id } = req.params;
      const prerequisito = makeSync(() => Prerequisito.findByPk(id));
      if (!prerequisito) {
        return res.status(404).json({ success: false, message: 'Prerequisito no encontrado' });
      }

      makeSync(() => prerequisito.destroy());

      res.status(200).json({ success: true, message: 'Prerequisito eliminado exitosamente' });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al eliminar prerequisito', error: error.message });
    }
  },

  // Obtener prerequisitos por materia
  getByMateria: (req, res) => {
    try {
      const { materiaId } = req.params;
      const materia = makeSync(() => Materia.findByPk(materiaId));
      if (!materia) {
        return res.status(404).json({ success: false, message: 'Materia no encontrada' });
      }

      const prerequisitos = makeSync(() => Prerequisito.findAll({
        where: { materiaId },
        order: [['id', 'ASC']]
      }));

      res.status(200).json({ success: true, data: prerequisitos });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener prerequisitos de la materia', error: error.message });
    }
  }
};

module.exports = prerequisitoControllerSync;