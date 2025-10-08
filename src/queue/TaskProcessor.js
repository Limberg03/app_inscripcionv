// TaskProcessor.js - VERSIÓN GLOBALIZADA PARA TODOS LOS MODELOS
const { sequelize } = require('../models');

class TaskProcessor {
  constructor() {
    this.models = null;
  }

  async initialize() {
    this.models = sequelize.models;
    console.log('✅ TaskProcessor inicializado con modelos:', Object.keys(this.models));
  }

  async processTask(task) {
    console.log('🔧 Procesando tarea:', task.id);
    console.log('   Modelo:', task.model);
    console.log('   Operación:', task.operation);
    console.log('   Datos completos:', JSON.stringify(task, null, 2));

    // VALIDACIÓN CRÍTICA: Verificar que la tarea tenga todos los campos necesarios
    if (!task || !task.model || !task.operation) {
      console.error('❌ Tarea incompleta recibida:', task);
      throw new Error(`Tarea incompleta: model=${task?.model}, operation=${task?.operation}`);
    }

    const { model, operation, data, id } = task;

 if (operation.toLowerCase() === 'requestseat') {
      return await this.handleRequestSeat(data);
    }

    // BÚSQUEDA DINÁMICA DEL MODELO
    const Model = this.findModel(model);
    if (!Model) {
      const availableModels = Object.keys(this.models);
      throw new Error(`Modelo '${model}' no encontrado. Modelos disponibles: ${availableModels.join(', ')}`);
    }

    console.log(`✅ Modelo '${model}' encontrado`);

    let result;

    try {
      switch (operation.toLowerCase()) {
        case 'create':
          result = await this.handleCreate(Model, data);
          break;

        case 'update':
          result = await this.handleUpdate(Model, data.id, data.updateData || data);
          break;

        case 'delete':
          result = await this.handleDelete(Model, data.id || id);
          break;

        case 'bulkcreate':
          result = await this.handleBulkCreate(Model, data.records || data);
          break;

        case 'bulkupdate':
          result = await this.handleBulkUpdate(Model, data);
          break;

        case 'bulkdelete':
          result = await this.handleBulkDelete(Model, data);
          break;

        default:
          throw new Error(`Operación '${operation}' no soportada`);
      }

      console.log(`✅ Tarea ${task.id} completada exitosamente`);
      console.log(`   Resultado:`, JSON.stringify(result, null, 2));
      
      return {
        success: true,
        operation,
        model,
        result
      };

    } catch (error) {
      console.error(`❌ Error procesando tarea ${task.id}:`, error);
      throw {
        message: error.message,
        retry: this.shouldRetry(error)
      };
    }
  }

  // BÚSQUEDA INTELIGENTE DE MODELOS
  findModel(modelName) {
    // 1. Búsqueda exacta
    if (this.models[modelName]) {
      return this.models[modelName];
    }

    // 2. Búsqueda case-insensitive
    const modelKeys = Object.keys(this.models);
    const exactMatch = modelKeys.find(key => 
      key.toLowerCase() === modelName.toLowerCase()
    );
    
    if (exactMatch) {
      return this.models[exactMatch];
    }

    // 3. Búsqueda por nombre de tabla (plurales)
    const tableNameMatch = modelKeys.find(key => {
      const model = this.models[key];
      if (model.tableName) {
        return model.tableName.toLowerCase() === modelName.toLowerCase();
      }
      return false;
    });

    if (tableNameMatch) {
      return this.models[tableNameMatch];
    }

    // 4. Búsqueda flexible (singulares/plurales)
    const flexibleMatch = modelKeys.find(key => {
      const keyLower = key.toLowerCase();
      const modelLower = modelName.toLowerCase();
      
      // Comparar singular/plural
      return keyLower === modelLower + 's' || 
             keyLower === modelLower.replace(/s$/, '') ||
             keyLower + 's' === modelLower ||
             keyLower.replace(/s$/, '') === modelLower;
    });

    if (flexibleMatch) {
      return this.models[flexibleMatch];
    }

    return null;
  }

  async handleCreate(Model, data) {
    console.log(`📝 Creando registro en modelo ${Model.name}`);
    console.log(`   Datos:`, JSON.stringify(data, null, 2));
    
    // VALIDACIÓN DE DATOS
    if (!data || typeof data !== 'object') {
      throw new Error('Datos de creación inválidos');
    }

    const record = await Model.create(data);
    
    const result = {
      id: record.id,
      created: true,
      data: record.toJSON(),
      model: Model.name,
      tableName: Model.tableName
    };

    console.log(`✅ Registro creado con ID: ${record.id}`);
    return result;
  }

  async handleUpdate(Model, id, data) {
    console.log(`📝 Actualizando registro ${id} en modelo ${Model.name}`);
    
    if (!id) {
      throw new Error('ID es requerido para actualizar');
    }

    if (!data || typeof data !== 'object') {
      throw new Error('Datos de actualización inválidos');
    }

    const [updated, records] = await Model.update(data, {
      where: { id },
      returning: true
    });

    if (updated === 0) {
      throw new Error(`Registro con id ${id} no encontrado`);
    }

    const result = {
      id,
      updated: true,
      updatedCount: updated,
      data: records[0]?.toJSON(),
      model: Model.name
    };

    console.log(`✅ Registro ${id} actualizado`);
    return result;
  }

  async handleDelete(Model, id) {
    console.log(`🗑️ Eliminando registro ${id} en modelo ${Model.name}`);
    
    if (!id) {
      throw new Error('ID es requerido para eliminar');
    }

    const deleted = await Model.destroy({
      where: { id }
    });

    if (deleted === 0) {
      throw new Error(`Registro con id ${id} no encontrado`);
    }

    const result = {
      id,
      deleted: true,
      deletedCount: deleted,
      model: Model.name
    };

    console.log(`✅ Registro ${id} eliminado`);
    return result;
  }

  async handleBulkCreate(Model, records) {
    console.log(`📝 Creación masiva en modelo ${Model.name}`);
    console.log(`   Cantidad de registros: ${records.length}`);
    
    if (!Array.isArray(records) || records.length === 0) {
      throw new Error('Array de registros válido es requerido');
    }

    const created = await Model.bulkCreate(records, {
      validate: true,
      returning: true
    });

    const result = {
      created: created.length,
      records: created.map(r => r.toJSON()),
      model: Model.name
    };

    console.log(`✅ ${created.length} registros creados masivamente`);
    return result;
  }

  async handleBulkUpdate(Model, { where, data }) {
    console.log(`📝 Actualización masiva en modelo ${Model.name}`);
    
    if (!where || !data) {
      throw new Error('where y data son requeridos para actualización masiva');
    }

    const [updated] = await Model.update(data, {
      where,
      returning: false
    });

    const result = {
      updated,
      where,
      data,
      model: Model.name
    };

    console.log(`✅ ${updated} registros actualizados masivamente`);
    return result;
  }

  async handleBulkDelete(Model, { where }) {
    console.log(`🗑️ Eliminación masiva en modelo ${Model.name}`);
    
    if (!where) {
      throw new Error('where es requerido para eliminación masiva');
    }

    const deleted = await Model.destroy({
      where
    });

    const result = {
      deleted,
      where,
      model: Model.name
    };

    console.log(`✅ ${deleted} registros eliminados masivamente`);
    return result;
  }


    async handleRequestSeat(data) {
    const { estudianteId, grupoMateriaId, gestion } = data;
    
    console.log(`🎓 Procesando solicitud de inscripción:`);
    console.log(`   Estudiante: ${estudianteId}`);
    console.log(`   Grupo Materia: ${grupoMateriaId}`);

    const GrupoMateria = this.models.GrupoMateria;
    const Inscripcion = this.models.Inscripcion;

    if (!GrupoMateria || !Inscripcion) {
      throw new Error('Modelos GrupoMateria o Inscripcion no encontrados');
    }

    // Iniciar transacción para garantizar atomicidad
    const transaction = await sequelize.transaction();

    try {
      // 1. Obtener grupo con bloqueo (FOR UPDATE)
      const grupo = await GrupoMateria.findByPk(grupoMateriaId, {
        lock: transaction.LOCK.UPDATE,
        transaction
      });

      if (!grupo) {
        throw new Error(`Grupo Materia ${grupoMateriaId} no encontrado`);
      }

      if (!grupo.estado) {
        throw new Error(`Grupo Materia ${grupoMateriaId} está inactivo`);
      }

      if (grupo.cupo <= 0) {
        await transaction.rollback();
        throw {
          success: false,
          status: 'rejected',
          reason: 'no_seats_available',
          message: `Sin cupos disponibles en grupo ${grupo.grupo}`,
          grupoMateriaId,
          estudianteId,
          cuposRestantes: 0,
          retry: false // ✅ NO reintentar
        };
      }

      // 3. Verificar si ya está inscrito
      const existingInscription = await Inscripcion.findOne({
        where: {
          estudianteId,
          grupoMateriaId
        },
        transaction
      });

      if (existingInscription) {
        await transaction.rollback();
        return {
          success: false,
          status: 'rejected',
          reason: 'already_enrolled',
          message: `Estudiante ${estudianteId} ya inscrito en este grupo`,
          grupoMateriaId,
          estudianteId
        };
      }

      // 4. Crear inscripción
      const inscripcion = await Inscripcion.create({
        estudianteId,
        grupoMateriaId,
        gestion,
        fecha: new Date()
      }, { transaction });

      // 5. Decrementar cupo
      await grupo.decrement('cupo', { 
        by: 1, 
        transaction 
      });

      await transaction.commit();

      const cuposRestantes = grupo.cupo - 1;

      console.log(`✅ Inscripción confirmada: ID ${inscripcion.id}`);
      console.log(`   Cupos restantes: ${cuposRestantes}`);

      return {
        success: true,
        status: 'confirmed',
        inscripcionId: inscripcion.id,
        estudianteId,
        grupoMateriaId,
        grupo: grupo.grupo,
        cuposRestantes,
        message: 'Seat confirmed successfully'
      };

     } catch (error) {
      await transaction.rollback();
      console.error(`❌ Error en inscripción:`, error.message);
      
      // ✅ Si es rechazo por cupos, NO es error técnico
      if (error.reason === 'no_seats_available' || error.reason === 'already_enrolled') {
        throw error; // Propagar tal cual
      }
      
      throw {
        success: false,
        status: 'rejected',
        reason: 'processing_error',
        message: error.message,
        retry: this.shouldRetry(error)
      };
    }
  }

  shouldRetry(error) {
    // Determinar si el error es recuperable
    const retryableErrors = [
      'SequelizeConnectionError',
      'SequelizeConnectionTimedOutError',
      'SequelizeHostNotReachableError',
      'SequelizeConnectionRefusedError',
      'SequelizeTimeoutError'
    ];

    const isRetryableError = retryableErrors.includes(error.name) || 
                           error.message.includes('connection') ||
                           error.message.includes('timeout') ||
                           error.message.includes('ECONNRESET');

    console.log(`🔄 ¿Error recuperable? ${isRetryableError} (${error.name})`);
    return isRetryableError;
  }
}

module.exports = TaskProcessor;