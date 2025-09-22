const sequelize = require('../config/database');

// Importar todos los modelos
const Estudiante = require('./Estudiante');
const Inscripcion = require('./Inscripcion');
const PlanEstudio = require('./PlanEstudio');
const Carrera = require('./Carrera');
const Nivel = require('./Nivel');
const Aula = require('./Aula');
const Materia = require('./Materia');
const Prerequisito = require('./Prerequisito');
const Docente = require('./Docente');
const GrupoMateria = require('./GrupoMateria');
const Horario = require('./Horario');
const Nota = require('./Nota');

// ¡AGREGAR EL MODELO USUARIO!
const Usuario = require('./Usuario')(sequelize, require('sequelize').DataTypes);

// Definir todas las relaciones

// Plan_Estudio -> Carrera (1:N)
PlanEstudio.hasMany(Carrera, { 
  foreignKey: 'planEstudioId', 
  as: 'carreras' 
});
Carrera.belongsTo(PlanEstudio, { 
  foreignKey: 'planEstudioId', 
  as: 'planEstudio' 
});

// Nivel -> Materia (1:N)
Nivel.hasMany(Materia, { 
  foreignKey: 'nivelId', 
  as: 'materias' 
});
Materia.belongsTo(Nivel, { 
  foreignKey: 'nivelId', 
  as: 'nivel' 
});

// Materia -> Prerequisito (relación N:M a través de Prerequisito)
// Una materia puede tener muchos prerequisitos
Materia.hasMany(Prerequisito, { 
  foreignKey: 'materiaId', 
  as: 'prerequisitos' 
});
Prerequisito.belongsTo(Materia, { 
  foreignKey: 'materiaId', 
  as: 'materia' 
});

// Una materia puede ser prerequisito de muchas otras
Materia.hasMany(Prerequisito, { 
  foreignKey: 'requiereId', 
  as: 'esPrerequisitoDe' 
});
Prerequisito.belongsTo(Materia, { 
  foreignKey: 'requiereId', 
  as: 'materiaPrerrequisito' 
});

// Materia -> GrupoMateria (1:N)
Materia.hasMany(GrupoMateria, { 
  foreignKey: 'materiaId', 
  as: 'grupos' 
});
GrupoMateria.belongsTo(Materia, { 
  foreignKey: 'materiaId', 
  as: 'materia' 
});

// Docente -> GrupoMateria (1:N)
Docente.hasMany(GrupoMateria, { 
  foreignKey: 'docenteId', 
  as: 'grupos' 
});
GrupoMateria.belongsTo(Docente, { 
  foreignKey: 'docenteId', 
  as: 'docente' 
});

// Horario -> GrupoMateria (1:N)
Horario.hasMany(GrupoMateria, { 
  foreignKey: 'horarioId', 
  as: 'grupos' 
});
GrupoMateria.belongsTo(Horario, { 
  foreignKey: 'horarioId', 
  as: 'horario' 
});



// GrupoMateria -> Inscripcion (1:N)
GrupoMateria.hasMany(Inscripcion, { 
  foreignKey: 'grupoMateriaId', 
  as: 'inscripciones' 
});
Inscripcion.belongsTo(GrupoMateria, { 
  foreignKey: 'grupoMateriaId', 
  as: 'grupoMateria' 
});



// GrupoMateria -> Nota (1:N)
GrupoMateria.hasMany(Nota, { 
  foreignKey: 'grupoMateriaId', 
  as: 'notas' 
});
Nota.belongsTo(GrupoMateria, { 
  foreignKey: 'grupoMateriaId', 
  as: 'grupoMateria' 
});

// Estudiante -> Nota (1:N)
Estudiante.hasMany(Nota, { 
  foreignKey: 'estudianteId', 
  as: 'notas' 
});
Nota.belongsTo(Estudiante, { 
  foreignKey: 'estudianteId', 
  as: 'estudiante' 
});

// Estudiante -> Inscripcion (1:N) - ya existente
Estudiante.hasMany(Inscripcion, { 
  foreignKey: 'estudianteId', 
  as: 'inscripciones' 
});
Inscripcion.belongsTo(Estudiante, { 
  foreignKey: 'estudianteId', 
  as: 'estudiante' 
});

// PlanEstudio -> Materia (1:N) 
PlanEstudio.hasMany(Materia, { 
  foreignKey: 'planEstudioId', 
  as: 'materias' 
});
Materia.belongsTo(PlanEstudio, { 
  foreignKey: 'planEstudioId', 
  as: 'planEstudio' 
});


module.exports = {
  sequelize,
  Usuario, 
  Estudiante,
  Inscripcion,
  PlanEstudio,
  Carrera,
  Nivel,
  Aula,
  Materia,
  Prerequisito,
  Docente,
  GrupoMateria,
  Horario,
  Nota
};