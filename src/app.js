// app.js - VERSIÓN REFACTORIZADA CON REDIS
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const path = require('path');

// Importaciones de la aplicación
const { sequelize } = require('./models/index');
const routes = require('./routes/index');
const routesSync = require('./routes_sync/index');



const queueRoutes = require('./routes/queueRoutes');
const trackingRoutes = require('./routes/trackingRoutes');
const { notFound, errorHandler, requestLogger } = require('./middleware/index');
const requestTrackingMiddleware = require('./middleware/requestTrackingMiddleware');
const QueueService = require('./services/QueueService');
const CallbackService = require('./services/CallbackService');

const app = express();
const PORT = process.env.PORT || 3000;

// ================ MIDDLEWARE Y CONFIGURACIÓN ================
app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        ...helmet.contentSecurityPolicy.getDefaultDirectives(),
        "script-src": [
          "'self'", 
          "'unsafe-inline'", 
          "https://cdnjs.cloudflare.com"  // ✅ Agregar esto
        ],
        "img-src": [
          "'self'", 
          "data:", 
          "https:"  // Para imágenes de Chart.js
        ],
        "connect-src": [
          "'self'", 
          "https://cdnjs.cloudflare.com"  // Para fetch de Chart.js
        ]
      },
    },
  })
);
app.use(cors({
  origin: process.env.NODE_ENV === 'production' 
    ? ['https://yourdomain.com'] 
    : ['http://localhost:3000', 'http://localhost:3001'],
  credentials: true
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

app.use(requestTrackingMiddleware);

  app.use(requestLogger);


// ================ RUTAS ================
app.use('/public', express.static(path.join(__dirname, 'public')));


app.use('/tracking', trackingRoutes);
app.use('/', routes); 
app.use('/sync', routesSync); 
app.use('/queue', queueRoutes);


// Ruta raíz para health check
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'Sistema Académico con Redis - API en funcionamiento.',
    version: '3.0.0'
  });
});

// Middlewares de manejo de errores
app.use(notFound);
app.use(errorHandler);

// Variables de servicio (se inicializan en `initializeApp`)
let queueService = null;
let callbackService = null;

// ================ LÓGICA DE INICIALIZACIÓN ================
const initializeApp = async () => {
  try {
    console.log('🚀 Iniciando la aplicación...');

    // 1. Conectar a PostgreSQL
    await sequelize.authenticate();
    console.log('Conexión a PostgreSQL establecida.');

    // 2. Inicializar servicios
    callbackService = new CallbackService();
    callbackService.setupDefaultCallbacks();
    console.log(' Sistema de Callbacks inicializado.');

    queueService = new QueueService();
    await queueService.initialize();
    console.log('✅ Sistema de Colas con Redis inicializado.');

    // 3. Verificar conexión a Redis
    const redisInfo = await queueService.getRedisInfo();


    

    if (process.env.NODE_ENV === 'development') {
      await sequelize.sync();
      console.log('✅ Modelos sincronizados con la base de datos.');
    }
    
  } catch (error) {
    console.error('❌ Error fatal al inicializar la aplicación:', error.message);
    if (error.message.includes('Redis') || error.code === 'ECONNREFUSED') {
      console.error('💡 SOLUCIÓN: Asegúrate de que el servidor Redis esté en ejecución y sea accesible.');
      console.error('   - Docker: docker run -d -p 6379:6379 redis:alpine');
      console.error('   - Local: redis-server');
    }
    process.exit(1);
  }
};

const gracefulShutdown = async (signal) => {
  console.log(`\n🔄 Recibida señal ${signal}. Cerrando la aplicación de forma segura...`);
  try {
    if (queueService) await queueService.shutdown();
    await sequelize.close();
    console.log('✅ Recursos liberados. Cierre completado.');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error durante el cierre seguro:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', (error) => {
  console.error('❌ Excepción no capturada:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Rechazo de promesa no manejado en:', promise, 'razón:', reason);
  process.exit(1);
});

// ================ INICIO DE LA APLICACIÓN ================
if (require.main === module) {
  initializeApp();
}

module.exports = { 
  app, 
   initializeApp,
  queueService: () => queueService, 
  callbackService: () => callbackService 
};
