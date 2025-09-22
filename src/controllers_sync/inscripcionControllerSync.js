// controllers_sync/inscripcionControllerSync.js
// VERSIÓN 100% SÍNCRONA (BLOQUEANTE) USANDO DEASYNC - NO USAR EN PRODUCCIÓN

const { Inscripcion, Estudiante, GrupoMateria, Materia, Docente, PlanEstudio, Nivel, Prerequisito } = require('../models');
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

const inscripcionControllerSync = {
  // Obtener todas las inscripciones
  getAll: (req, res) => {
    try {
      const page = parseInt(req.query.page) || 1;
      const limit = parseInt(req.query.limit) || 10;
      const { gestion } = req.query;
      const where = {};
      if (gestion) where.gestion = gestion;

      const inscripciones = makeSync(() => Inscripcion.findAndCountAll({
        where,
        include: [
          { model: Estudiante, as: 'estudiante', attributes: ['id', 'registro', 'nombre', 'telefono'] },
          {
            model: GrupoMateria, as: 'grupoMateria', attributes: ['id', 'grupo', 'estado'],
            include: [
              { model: Materia, as: 'materia', include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }, { model: Prerequisito, as: 'prerequisitos' }] },
              { model: Docente, as: 'docente' }
            ]
          }
        ],
        order: [['fecha', 'DESC']]
      }));

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
      res.status(500).json({ success: false, message: 'Error al obtener inscripciones', error: error.message });
    }
  },

  // Obtener inscripción por ID
  getById: (req, res) => {
    try {
      const { id } = req.params;
      const inscripcion = makeSync(() => Inscripcion.findByPk(id, {
        include: [
          { model: Estudiante, as: 'estudiante' },
          { model: GrupoMateria, as: 'grupoMateria', include: [{ model: Materia, as: 'materia', include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }, { model: Prerequisito, as: 'prerequisitos' }] }, { model: Docente, as: 'docente' }] }
        ]
      }));

      if (!inscripcion) {
        return res.status(404).json({ success: false, message: 'Inscripción no encontrada' });
      }
      res.status(200).json({ success: true, data: inscripcion });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener inscripción', error: error.message });
    }
  },

  // Crear nueva inscripción
  create: (req, res) => {
    try {
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(400).json({ success: false, message: 'Errores de validación', errors: errors.array() });
      }

      const { fecha, gestion, estudianteId, grupoMateriaId } = req.body;

      const estudiante = makeSync(() => Estudiante.findByPk(estudianteId));
      if (!estudiante) return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });

      const grupoMateria = makeSync(() => GrupoMateria.findByPk(grupoMateriaId));
      if (!grupoMateria) return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });

      const inscripcion = makeSync(() => Inscripcion.create({ fecha: fecha || new Date(), gestion, estudianteId, grupoMateriaId }));

      const inscripcionCompleta = makeSync(() => Inscripcion.findByPk(inscripcion.id, {
        include: [
          { model: Estudiante, as: 'estudiante' },
          { model: GrupoMateria, as: 'grupoMateria', include: [{ model: Materia, as: 'materia', include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }, { model: Prerequisito, as: 'prerequisitos' }] }, { model: Docente, as: 'docente' }] }
        ]
      }));

      res.status(201).json({ success: true, message: 'Inscripción creada exitosamente', data: inscripcionCompleta });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al crear inscripción', error: error.message });
    }
  },

  // Actualizar inscripción
  update: (req, res) => {
    try {
      const { id } = req.params;
      const { fecha, gestion, estudianteId, grupoMateriaId } = req.body;

      const inscripcionInstance = makeSync(() => Inscripcion.findByPk(id));
      if (!inscripcionInstance) return res.status(404).json({ success: false, message: 'Inscripción no encontrada' });

      if (estudianteId) {
        const estudiante = makeSync(() => Estudiante.findByPk(estudianteId));
        if (!estudiante) return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });
      }

      if (grupoMateriaId) {
        const grupoMateria = makeSync(() => GrupoMateria.findByPk(grupoMateriaId));
        if (!grupoMateria) return res.status(404).json({ success: false, message: 'Grupo de materia no encontrado' });
      }

      makeSync(() => inscripcionInstance.update({ fecha, gestion, estudianteId, grupoMateriaId }));

      const inscripcionActualizada = makeSync(() => Inscripcion.findByPk(id, {
        include: [
          { model: Estudiante, as: 'estudiante' },
          { model: GrupoMateria, as: 'grupoMateria', include: [{ model: Materia, as: 'materia', include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }, { model: Prerequisito, as: 'prerequisitos' }] }, { model: Docente, as: 'docente' }] }
        ]
      }));

      res.status(200).json({ success: true, message: 'Inscripción actualizada exitosamente', data: inscripcionActualizada });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al actualizar inscripción', error: error.message });
    }
  },

  // Eliminar inscripción
  delete: (req, res) => {
    try {
      const { id } = req.params;
      const inscripcion = makeSync(() => Inscripcion.findByPk(id));
      if (!inscripcion) {
        return res.status(404).json({ success: false, message: 'Inscripción no encontrada' });
      }

      makeSync(() => inscripcion.destroy());

      res.status(200).json({ success: true, message: 'Inscripción eliminada exitosamente' });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al eliminar inscripción', error: error.message });
    }
  },

  // Obtener inscripciones por estudiante
  getByEstudiante: (req, res) => {
    try {
      const { estudianteId } = req.params;
      const estudiante = makeSync(() => Estudiante.findByPk(estudianteId));
      if (!estudiante) {
        return res.status(404).json({ success: false, message: 'Estudiante no encontrado' });
      }

      const inscripciones = makeSync(() => Inscripcion.findAll({
        where: { estudianteId },
        include: [
          { model: Estudiante, as: 'estudiante' },
          { model: GrupoMateria, as: 'grupoMateria', include: [{ model: Materia, as: 'materia', include: [{ model: PlanEstudio, as: 'planEstudio' }, { model: Nivel, as: 'nivel' }, { model: Prerequisito, as: 'prerequisitos' }] }, { model: Docente, as: 'docente' }] }
        ],
        order: [['gestion', 'DESC'], ['fecha', 'DESC']]
      }));

      res.status(200).json({ success: true, data: inscripciones });
    } catch (error) {
      res.status(500).json({ success: false, message: 'Error al obtener inscripciones del estudiante', error: error.message });
    }
  }
};

module.exports = inscripcionControllerSync;