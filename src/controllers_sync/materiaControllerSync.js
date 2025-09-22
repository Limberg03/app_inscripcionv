// controllers_sync/materiaControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Materia, PlanEstudio, Nivel, Prerequisito, GrupoMateria } = require('../models');
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

const materiaControllerSync = {
  // Obtener todas las materias
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { nivelId, planEstudioId } = req.query;

      const where = {};
      if (nivelId) where.nivelId = nivelId;
      if (planEstudioId) where.planEstudioId = planEstudioId;

      const materias = makeSync(() => Materia.findAndCountAll({
        where,
        include: [
          { model: PlanEstudio, as: 'planEstudio', attributes: ['id', 'nombre'] },
          { model: Nivel, as: 'nivel', attributes: ['id', 'nombre'] },
          { model: Prerequisito, as: 'prerequisitos', attributes: ['id', 'materia_id', 'requiere_id'] }
        ],
        order: [['id', 'ASC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener materias', error: error.message });
    }
  },

  // Obtener materia por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const materia = makeSync(() => Materia.findByPk(id, {
        include: [
          { model: PlanEstudio, as: 'planEstudio' },
          { model: Nivel, as: 'nivel' },
          { model: Prerequisito, as: 'prerequisitos' },
          { model: GrupoMateria, as: 'gruposMateria' }
        ]
      }));

      if (!materia) {
        return res.status(404).json({ success: false, message: 'Materia no encontrada' });
      }
      res.status(200).json({ success: true, data: materia });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener materia', error: error.message });
    }
  },

  // Crear nueva materia
  create: (req, res) => {
    try {
      const { nombre, sigla, creditos, planEstudioId, nivelId } = req.body;

      const planEstudio = makeSync(() => PlanEstudio.findByPk(planEstudioId));
      if (!planEstudio) return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });

      const nivel = makeSync(() => Nivel.findByPk(nivelId));
      if (!nivel) return res.status(404).json({ success: false, message: 'Nivel no encontrado' });

      const materia = makeSync(() => Materia.create({ nombre, sigla, creditos, planEstudioId, nivelId }));

      const materiaCompleta = makeSync(() => Materia.findByPk(materia.id, {
        include: [
          { model: PlanEstudio, as: 'planEstudio' },
          { model: Nivel, as: 'nivel' }
        ]
      }));

      res.status(201).json({ success: true, message: 'Materia creada exitosamente', data: materiaCompleta });
    } catch (error) {
      if (error.name === 'SequelizeUniqueConstraintError') {
        return res.status(400).json({ success: false, message: 'La sigla de la materia ya existe' });
      }
      res.status(500).json({ success: false, message: 'Error al crear materia', error: error.message });
    }
  },

  // Actualizar materia
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { nombre, sigla, creditos, planEstudioId, nivelId } = req.body;

      const materiaInstance = makeSync(() => Materia.findByPk(id));
      if (!materiaInstance) return res.status(404).json({ success: false, message: 'Materia no encontrada' });

      if (planEstudioId) {
        const planEstudio = makeSync(() => PlanEstudio.findByPk(planEstudioId));
        if (!planEstudio) return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
      }
      if (nivelId) {
        const nivel = makeSync(() => Nivel.findByPk(nivelId));
        if (!nivel) return res.status(404).json({ success: false, message: 'Nivel no encontrado' });
      }

      makeSync(() => materiaInstance.update({ nombre, sigla, creditos, planEstudioId, nivelId }));

      const materiaActualizada = makeSync(() => Materia.findByPk(id, {
        include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }]
      }));

      res.status(200).json({ success: true, message: 'Materia actualizada exitosamente', data: materiaActualizada });
    } catch (error) {
        if (error.name === 'SequelizeUniqueConstraintError') {
            return res.status(400).json({ success: false, message: 'La sigla de la materia ya existe' });
        }
      res.status(500).json({ success: false, message: 'Error al actualizar materia', error: error.message });
    }
  },

  // Eliminar materia
  delete: (req, res) => {
    try {
        const { id } = req.params;
        const materia = makeSync(() => Materia.findByPk(id));
        if (!materia) {
            return res.status(404).json({ success: false, message: 'Materia no encontrada' });
        }
        makeSync(() => materia.destroy());
        res.status(200).json({ success: true, message: 'Materia eliminada exitosamente' });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al eliminar materia', error: error.message });
    }
  },

  // Actualización parcial con PATCH
  patch: (req, res) => {
    try {
        const { id } = req.params;
        const updateData = req.body;

        const materiaInstance = makeSync(() => Materia.findByPk(id));
        if (!materiaInstance) return res.status(404).json({ success: false, message: 'Materia no encontrada' });
        
        if (updateData.planEstudioId) {
            const planEstudio = makeSync(() => PlanEstudio.findByPk(updateData.planEstudioId));
            if (!planEstudio) return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
        }
        if (updateData.nivelId) {
            const nivel = makeSync(() => Nivel.findByPk(updateData.nivelId));
            if (!nivel) return res.status(404).json({ success: false, message: 'Nivel no encontrado' });
        }

        makeSync(() => materiaInstance.update(updateData));

        const materiaActualizada = makeSync(() => Materia.findByPk(id, {
            include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }]
        }));

        res.status(200).json({ success: true, message: 'Materia actualizada parcialmente', data: materiaActualizada });
    } catch (error) {
        if (error.name === 'SequelizeUniqueConstraintError') {
            return res.status(400).json({ success: false, message: 'La sigla de la materia ya existe' });
        }
        res.status(500).json({ success: false, message: 'Error al actualizar materia', error: error.message });
    }
  },

  // Obtener materias por nivel
  getByNivel: (req, res) => {
    try {
        const { nivelId } = req.params;
        const nivel = makeSync(() => Nivel.findByPk(nivelId));
        if (!nivel) {
            return res.status(404).json({ success: false, message: 'Nivel no encontrado' });
        }

        const materias = makeSync(() => Materia.findAll({
            where: { nivelId },
            include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: GrupoMateria, as: 'gruposMateria' }],
            order: [['nombre', 'ASC']]
        }));
        
        res.status(200).json({ success: true, data: materias });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al obtener materias del nivel', error: error.message });
    }
  },

  // Obtener materias por plan de estudio
  getByPlanEstudio: (req, res) => {
    try {
        const { planEstudioId } = req.params;
        const planEstudio = makeSync(() => PlanEstudio.findByPk(planEstudioId));
        if (!planEstudio) {
            return res.status(404).json({ success: false, message: 'Plan de estudio no encontrado' });
        }

        const materias = makeSync(() => Materia.findAll({
            where: { planEstudioId },
            include: [{ model: Nivel, as: 'nivel' }, { model: GrupoMateria, as: 'gruposMateria' }],
            order: [[{ model: Nivel, as: 'nivel' }, 'numNivel', 'ASC'], ['nombre', 'ASC']]
        }));
        
        res.status(200).json({ success: true, data: materias });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Error al obtener materias del plan de estudio', error: error.message });
    }
  }
};

module.exports = materiaControllerSync;