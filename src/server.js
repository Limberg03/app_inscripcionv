// server.js - VERSIÓN INTEGRADA RESPETANDO TU ARQUITECTURA
const cluster = require('cluster');
const os = require('os');
const http = require('http');

// Configuraciones de clustering y optimización
const SERVER_CONFIG = {
  port: process.env.PORT || 3000,
  maxConnections: 2000,
  timeout: 30000,
  keepAliveTimeout: 65000,
  headersTimeout: 66000,
  maxRequestsPerSocket: 0,
  backlog: 1024,
  enableClustering: process.env.NODE_ENV === 'production' // Solo en producción
};

// Rate limiter compartido entre workers
const rateLimiter = new Map();
const RATE_LIMIT_WINDOW = 60000; // 1 minuto
const RATE_LIMIT_MAX = 100; // 100 requests por minuto por IP

const checkRateLimit = (ip) => {
  const now = Date.now();
  const windowStart = now - RATE_LIMIT_WINDOW;
  
  if (!rateLimiter.has(ip)) {
    rateLimiter.set(ip, []);
  }
  
  const requests = rateLimiter.get(ip);
  const validRequests = requests.filter(time => time > windowStart);
  
  if (validRequests.length >= RATE_LIMIT_MAX) {
    return false;
  }
  
  validRequests.push(now);
  rateLimiter.set(ip, validRequests);
  return true;
};

const rateLimitMiddleware = (req, res, next) => {
  // Solo aplicar rate limiting en producción
  if (process.env.NODE_ENV !== 'production') {
    return next();
  }
  
  const ip = req.ip || req.connection.remoteAddress;
  
  if (!checkRateLimit(ip)) {
    return res.status(429).json({
      success: false,
      message: 'Rate limit exceeded',
      retryAfter: 60,
      workerId: cluster.worker ? cluster.worker.id : 'main'
    });
  }
  
  next();
};

// CLUSTERING CONDICIONAL
if (SERVER_CONFIG.enableClustering && cluster.isMaster) {
  // ================ PROCESO MAESTRO ================
  const numCPUs = Math.min(os.cpus().length, 4); // Máximo 4 workers
  
  // Crear workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  
  // Manejo de workers que fallan
  cluster.on('exit', (worker, code, signal) => {
    console.log(`💀 Worker ${worker.process.pid} died (${signal || code}). Restarting...`);
    cluster.fork();
  });
  
  // Estadísticas del cluster cada 30 segundos
  if (process.env.NODE_ENV === 'development') {
    setInterval(() => {
      const workers = Object.keys(cluster.workers).length;
      console.log(`📊 Cluster status: ${workers} workers running`);
    }, 30000);
  }
  
  // Graceful shutdown del cluster
  process.on('SIGTERM', () => {
    console.log('🛑 Master process shutting down...');
    for (const id in cluster.workers) {
      cluster.workers[id].kill();
    }
  });
  
} else {
  // ================ PROCESO WORKER O DESARROLLO ================
  const startWorker = async () => {
    try {
      // Importar TU app.js existente
      const { app, initializeApp, queueService, callbackService } = require('./app');
      
      
      await initializeApp();
      app.use('/api', rateLimitMiddleware);
      
      // Headers de optimización
      app.use((req, res, next) => {
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('Keep-Alive', 'timeout=65');
        res.setHeader('X-Powered-By', 'Optimized-Node');
        if (cluster.worker) {
          res.setHeader('X-Worker-ID', cluster.worker.id);
        }
        next();
      });

      app.get('/health', (req, res) => {
        const qService = queueService ? queueService() : null;
        const cService = callbackService ? callbackService() : null;
        
        res.status(200).json({ 
          status: 'ok', 
          timestamp: new Date().toISOString(),
          worker: cluster.worker ? cluster.worker.id : 'main',
          uptime: process.uptime(),
          services: {
            queue: qService ? 'active' : 'inactive',
            callback: cService ? 'active' : 'inactive'
          },
          memory: {
            rss: Math.round(process.memoryUsage().rss / 1024 / 1024) + 'MB',
            heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB'
          }
        });
      });
      
      // Stats detallados
      app.get('/cluster-stats', async (req, res) => {
        const qService = queueService ? queueService() : null;
        let redisInfo = null;
        
        try {
          if (qService && typeof qService.getRedisInfo === 'function') {
            redisInfo = await qService.getRedisInfo();
          }
        } catch (error) {
          // Redis no disponible
        }
        
        res.status(200).json({
          success: true,
          worker: {
            id: cluster.worker ? cluster.worker.id : 'main',
            pid: process.pid,
            uptime: process.uptime(),
            memory: process.memoryUsage()
          },
          server: {
            nodeVersion: process.version,
            platform: process.platform,
            arch: process.arch
          },
          services: {
            redis: redisInfo ? 'connected' : 'disconnected',
            queue: qService ? 'active' : 'inactive'
          },
          rateLimiter: {
            activeIPs: rateLimiter.size,
            totalRequests: Array.from(rateLimiter.values()).reduce((acc, reqs) => acc + reqs.length, 0)
          }
        });
      });
      
      // 4. CREAR SERVIDOR HTTP OPTIMIZADO CON TU APP
      const server = http.createServer(app);
      
      // Aplicar configuraciones de rendimiento
      server.maxConnections = SERVER_CONFIG.maxConnections;
      server.timeout = SERVER_CONFIG.timeout;
      server.keepAliveTimeout = SERVER_CONFIG.keepAliveTimeout;
      server.headersTimeout = SERVER_CONFIG.headersTimeout;
      server.maxRequestsPerSocket = SERVER_CONFIG.maxRequestsPerSocket;
      
      // 5. INICIAR SERVIDOR
      server.listen(SERVER_CONFIG.port, SERVER_CONFIG.backlog, () => {
        const workerInfo = cluster.worker ? `Worker ${cluster.worker.id}` : 'Single process';
        
        if (!cluster.worker || cluster.worker.id === 1) {
        }
      });
      
      const gracefulShutdown = async (signal) => {
        console.log(`\n🔄 ${cluster.worker ? `Worker ${cluster.worker.id}` : 'Process'} received ${signal}. Shutting down gracefully...`);
        
        server.close(async () => {
          try {
            const qService = queueService ? queueService() : null;
            if (qService && typeof qService.shutdown === 'function') {
              await qService.shutdown();
            }
            
            process.exit(0);
          } catch (error) {
            console.error('❌ Error during graceful shutdown:', error);
            process.exit(1);
          }
        });
      };
      
      // Event listeners para shutdown
      process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
      process.on('SIGINT', () => gracefulShutdown('SIGINT'));
      
      // Monitoreo de memoria en desarrollo
      if (process.env.NODE_ENV === 'development') {
        setInterval(() => {
          const mem = process.memoryUsage();
          const workerInfo = cluster.worker ? `Worker ${cluster.worker.id}` : 'Process';
          console.log(`${workerInfo} Memory: ${Math.round(mem.rss / 1024 / 1024)}MB RSS, ${Math.round(mem.heapUsed / 1024 / 1024)}MB Heap`);
        }, 120000); // Cada 2 minutos
      }
      
    } catch (error) {
      console.error(`❌ ${cluster.worker ? `Worker ${cluster.worker.id}` : 'Process'} failed to start:`, error);
      
      // Información útil para debugging
      if (error.message.includes('Redis') || error.code === 'ECONNREFUSED') {
        console.error('💡 SOLUCIÓN: Asegúrate de que el servidor Redis esté en ejecución');
        console.error('   - Docker: docker run -d -p 6379:6379 redis:alpine');
        console.error('   - Local: redis-server');
      }
      
      process.exit(1);
    }
  };
  
  // Iniciar el worker
  startWorker();
}
