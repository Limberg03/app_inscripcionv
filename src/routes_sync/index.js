const express = require('express');
const router = express.Router();

// ✅ IMPORTAR TUS MIDDLEWARES EXISTENTES
const { 
  authMiddleware, 
  roleMiddleware, 
  optionalAuthMiddleware 
} = require('../middleware/authMiddleware');

// Importar todas las rutas
const estudianteRoutes = require('./estudianteRoutes');
const inscripcionRoutes = require('./inscripcionRoutes');
const carreraRoutes = require('./carreraRoutes');
const docenteRoutes = require('./docenteRoutes');
const materiaRoutes = require('./materiaRoutes');
const aulaRoutes = require('./aulaRoutes');
const nivelRoutes = require('./nivelRoutes');
const grupoMateriaRoutes = require('./grupoMateriaRoutes');
const horarioRoutes = require('./horarioRoutes');
const notaRoutes = require('./notaRoutes');
const prerequisitoRoutes = require('./prerequisitoRoutes');
const planEstudioRoutes = require('./planEstudioRoutes');

router.get('/health', (req, res) => {
  res.status(200).json({
    success: true,
    message: 'Microservicio académico funcionando correctamente',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

router.get('/', (req, res) => {
  res.status(200).json({
    success: true,
    message: 'API REST - Sistema Académico Universitario',
    version: '1.0.0',
    endpoints: {
      auth: '/auth',
      health: '/health',
      estudiantes: '/estudiantes',
      inscripciones: '/inscripciones',
      carreras: '/carreras',
      docentes: '/docentes',
      materias: '/materias',
      aulas: '/aulas',
      niveles: '/niveles',
      gruposMateria: '/grupos-materia',
      horarios: '/horarios',
      notas: '/notas',
      prerequisitos: '/prerequisitos',
      planesEstudio: '/planes-estudio'
    },
    authentication: {
      type: 'JWT Bearer Token',
      loginUrl: '/auth/login',
      registerUrl: '/auth/register',
      profileUrl: '/auth/profile',
      headerFormat: 'Authorization: Bearer <token>'
    },
    documentation: {
      base_url: process.env.NODE_ENV === 'production' 
        ? 'https://tu-dominio.com/api/v1' 
        : 'http://localhost:3000/api/v1',
      pagination: 'Usar ?page=1&limit=10 para paginación',
      filters: 'Usar query parameters específicos según endpoint',
      authentication: 'Usar JWT tokens en header Authorization'
    }
  });
});

//router.use('/auth', authRoutes);


//router.use(authMiddleware);

router.use('/estudiantes', estudianteRoutes);
router.use('/inscripciones', inscripcionRoutes);
router.use('/carreras', carreraRoutes);
router.use('/docentes', docenteRoutes);
router.use('/materias', materiaRoutes);
router.use('/aulas', aulaRoutes);
router.use('/niveles', nivelRoutes);
router.use('/grupos-materia', grupoMateriaRoutes);
router.use('/horarios', horarioRoutes);
router.use('/notas', notaRoutes);
router.use('/prerequisitos', prerequisitoRoutes);
router.use('/planes-estudio', planEstudioRoutes);

module.exports = router;