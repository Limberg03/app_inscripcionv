const { Sequelize } = require('sequelize');
require('dotenv').config();

const sequelize = new Sequelize(
  process.env.DB_NAME || 'inscripcion',
  process.env.DB_USER || 'postgres',
  process.env.DB_PASS || '68867805',
  {
    host: process.env.DB_HOST || 'database-1.cojiy0g4k0bt.us-east-1.rds.amazonaws.com',
    port: process.env.DB_PORT || 5432,
    dialect: 'postgres',
    logging: false, // opcional, para no ver tanto log en consola
  }
);

module.exports = sequelize;
