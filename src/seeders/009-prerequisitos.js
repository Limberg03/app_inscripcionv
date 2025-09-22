'use strict';

module.exports = {
  up: async (queryInterface, Sequelize) => {
    // Función helper para obtener el ID de una materia por su nombre
    const getMateriaIdByName = async (nombre) => {
      const result = await queryInterface.sequelize.query(
        'SELECT id FROM materias WHERE nombre = ?',
        { 
          replacements: [nombre],
          type: Sequelize.QueryTypes.SELECT 
        }
      );
      
      if (result.length === 0) {
        throw new Error(`No se encontró la materia con nombre: ${nombre}`);
      }
      
      return result[0].id;
    };

    // Obtener los IDs de todas las materias que necesitamos
    const matI_id = await getMateriaIdByName('Matemáticas I');
    const matII_id = await getMateriaIdByName('Matemáticas II');
    const progI_id = await getMateriaIdByName('Introducción a la Programación');
    const poo_id = await getMateriaIdByName('Programación Orientada a Objetos');
    const fisI_id = await getMateriaIdByName('Física I');
    const fisII_id = await getMateriaIdByName('Física II');
    const estDat_id = await getMateriaIdByName('Estructura de Datos');
    const bdI_id = await getMateriaIdByName('Base de Datos I');
    const bdII_id = await getMateriaIdByName('Base de Datos II');
    const algComp_id = await getMateriaIdByName('Algoritmos y Complejidad');
    const ingSwI_id = await getMateriaIdByName('Ingeniería de Software I');

    await queryInterface.bulkInsert('prerequisitos', [
      // Matemáticas II requiere Matemáticas I
      {
        materia_id: matII_id,
        requiere_id: matI_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // POO requiere Introducción a la Programación
      {
        materia_id: poo_id,
        requiere_id: progI_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Física II requiere Física I
      {
        materia_id: fisII_id,
        requiere_id: fisI_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Base de Datos I requiere POO
      {
        materia_id: bdI_id,
        requiere_id: poo_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Estructura de Datos requiere POO
      {
        materia_id: estDat_id,
        requiere_id: poo_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Algoritmos y Complejidad requiere Estructura de Datos
      {
        materia_id: algComp_id,
        requiere_id: estDat_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Base de Datos II requiere Base de Datos I
      {
        materia_id: bdII_id,
        requiere_id: bdI_id,
        created_at: new Date(),
        updated_at: new Date()
      },
      // Ingeniería de Software I requiere Estructura de Datos
      {
        materia_id: ingSwI_id,
        requiere_id: estDat_id,
        created_at: new Date(),
        updated_at: new Date()
      }
    ], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('prerequisitos', null, {});
  }
};