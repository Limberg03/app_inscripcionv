const requestTracker = require('../services/RequestTracker');

// Lista de rutas que NO deben ser trackeadas
const EXCLUDED_PATHS = [
  // Rutas del dashboard
  '/dashboard',
  
  // Rutas de tracking (evita bucle infinito)
  /^\/tracking(\/.*)?$/,
  
  // Rutas estáticas
  /^\/public(\/.*)?$/,
  /^\/js(\/.*)?$/,
  /^\/css(\/.*)?$/,
  /^\/images(\/.*)?$/,
  
  // Health checks
  '/health',
  '/status',
  
  // Rutas de autenticación (opcional, descomenta si las tienes)
  // '/login',
  // '/auth',
  // '/api/auth'
];

const shouldExcludePath = (path) => {
  return EXCLUDED_PATHS.some(exclusion => {
    if (typeof exclusion === 'string') {
      return path === exclusion;
    } else if (exclusion instanceof RegExp) {
      return exclusion.test(path);
    }
    return false;
  });
};

const requestTrackingMiddleware = (req, res, next) => {
  const startTime = process.hrtime.bigint();
  
  // ✅ NUEVO: Excluir rutas del dashboard y tracking
  if (shouldExcludePath(req.path)) {
    return next(); // Saltar el tracking para estas rutas
  }
  
  // Capturar datos originales de la respuesta
  const originalSend = res.send;
  const originalJson = res.json;
  const originalEnd = res.end;
  
  let responseBody = null;
  let responseSize = 0;

  // Override res.json para capturar response
  res.json = function(data) {
    responseBody = data;
    responseSize = JSON.stringify(data).length;
    return originalJson.call(this, data);
  };

  // Override res.send para capturar response
  res.send = function(data) {
    if (data && typeof data === 'object') {
      responseBody = data;
      responseSize = JSON.stringify(data).length;
    } else if (data) {
      responseSize = data.toString().length;
    }
    return originalSend.call(this, data);
  };

  // Override res.end para tracking final
  res.end = function(chunk, encoding) {
    const endTime = process.hrtime.bigint();
    const responseTime = Number(endTime - startTime) / 1000000; // Convert to ms

    // Capturar información del request
    const requestData = {
      url: req.originalUrl || req.url,
      method: req.method,
      body: req.body || null,
      query: req.query || {},
      headers: {
        'content-type': req.get('Content-Type'),
        'user-agent': req.get('User-Agent'),
        'x-forwarded-for': req.get('X-Forwarded-For'),
        'host': req.get('Host')
      },
      userAgent: req.get('User-Agent'),
      ip: req.ip || req.connection.remoteAddress || req.socket.remoteAddress,
      responseTime: Math.round(responseTime * 100) / 100, // Round to 2 decimals
      statusCode: res.statusCode,
      responseSize,
      error: res.statusCode >= 400 ? `HTTP ${res.statusCode}` : null
    };

    // Track the request
    try {
      requestTracker.trackRequest(requestData);
    } catch (error) {
      console.error('Error tracking request:', error);
    }

    return originalEnd.call(this, chunk, encoding);
  };

  next();
};

module.exports = requestTrackingMiddleware;