const mongoose = require("mongoose");
const ObjectId = require("mongoose").Types.ObjectId;

// Schema

const Puntos_ganados_establecimientoSchema = mongoose.Schema({
  punto_entrega: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  pedido: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido",
    required: true,
  },
  puntos_ganados: { type: Number, required: true },
  movimiento: {
    type: String,
    required: true,
    default: "Aplica",
    enum: [
      "Aplica", //Cuando los puntos son aplicados en un pedido mediante un codigo de descuento previamente generado
      "Revierte", //Cuando el pedido es cancelado
      "Por congelar", // Cuando un pedido es recibido pero no ha sido calificado
      "Congelados", // Cuando un pedido finaliza existosamente en calificado , se congelan para que ya queden disponibles
      "CodigoGenerado", // Cuando los puntos fueron usados en generar un codigo de descuento
      "Congelados feat", // Cuando el pedido fue aceptado fuera de tiempo, y no se otorgan al usuario
      "Redimidos", //Cuando finalmente se redimen mediante un codigo de descuento aplicado a un nuevo pedido
    ],
  },
  fecha: { type: Date, required: true },
  estado: {
    type: String,
    required: true,
    default: "Pendiente",
    enum: ["Pendiente", "Saldado"],
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Puntos_ganados_establecimientoSchema.statics = {
  get: function (query, callback) {
    this.findOne(query).exec(callback);
  },
  getAll: function (query, callback) {
    this.find(query).exec(callback);
  },
  updateById: function (id, updateData, callback) {
    this.findOneAndUpdate(
      { _id: id },
      { $set: updateData },
      { new: true },
      callback
    );
  },
  removeById: function (removeData, callback) {
    this.findOneAndRemove(removeData, callback);
  },
  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const Puntos_ganados_establecimiento = new this(data);
    Puntos_ganados_establecimiento.save(callback);
  },
  getPuntosGanadosDisponible: function (obj, callback) {
    const query = {
      punto_entrega: obj.punto_entrega,
      movimiento: "Congelados",
    };
    Puntos_ganados_establecimiento.find(query, callback);
  },
  getPuntosGanadosDetallado: function (query, callback) {
    Puntos_ganados_establecimiento.find(query)
      .populate({
        path: "pedido",
        populate: {
          path: "distribuidor punto_entrega codigo_descuento",
        },
      })
      .exec(callback);
  },
  getPuntosGanadosMarca: function (query, callback) {
    Puntos_ganados_establecimiento.find(query)
      .populate({
        path: "pedido",
        populate: {
          path: "punto_entrega productos",
          populate: {
            path: "product",
            populate: {
              path: "marca_producto",
            },
          },
        },
      })
      .exec(callback);
  },
  getfilterPuntosByPedido: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      pedido: new ObjectId(obj.pedido),
      // movimiento: "CodigoGenerado", este ddeberia ser el estado real
      movimiento: "Congelados",
    };
    this.find(query).exec(callback);
  },
  getDetalleByIdPunto: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          punto_entrega: new ObjectId(query._id),
        },
      },
      {
        $group: {
          _id: "$movimiento",
          total_puntos: { $sum: "$puntos_ganados" },
        },
      },
    ]).exec(callback);
  },
  getBalanceFinalPuntosEstablecimiento: function (query, callback) {
    let ObjectId = require("mongoose").Types.ObjectId;
    this.aggregate([
      {
        $match: {
          punto_entrega: new ObjectId(query.idPuntoEntrega),
        },
      },
      {
        $facet: {
          // Sumatoria de puntos en estado diferente a redimidos y revierte (CodigoGenerado,Congelados)
          acumulados: [
            {
              $match: {
                $or: [
                  { movimiento: "Congelados" },
                  { movimiento: "CodigoGenerado" },
                  { movimiento: "Aplica" }
                ],
              },
            },
          ],
          // Unicamente puntos redimidos
          redimidos: [
            {
              $match: {
                $or: [
                  { movimiento: "Redimidos" },
                ],
              },
            },
          ],
          todos: [],
        },
      },
      {
        $project: {
          acumulados: [
            {
              $facet: {
                acumulados: [
                  {
                    $match: {
                      $or: [
                        { movimiento: "Congelados" },
                        { movimiento: "CodigoGenerado" },
                        { movimiento: "Redimidos" },
                      ],
                    },
                  },
                ],
                disponibles: [
                  {
                    $match: {
                      movimiento: "Congelados",
                    },
                  },
                ],
                redimidos: [
                  {
                    $match: {
                      $or: [
                        { movimiento: "CodigoGenerado" },
                        { movimiento: "Redimidos" },
                      ],
                    },
                  },
                ],
              },
            },
            {
              label: "Puntos disponibles",
              total: { $subtract: [acumulados, redimidos] },
            },
          ],
          redimidos: [
            {
              label: "Puntos redimidos",
              total: { $sum: "$redimidos.puntos_ganados" },
            },
          ],
          disponibles: [
            {
              label: "Puntos disponibles",
              total: { $subtract: [{ $sum: "$acumulados.puntos_ganados" }, { $sum: "$redimidos.puntos_ganados" }] },
            },
          ],
          todos: [
            {
              label: "Todos",
              total: { $sum: "$todos.puntos_ganados" },
            },
          ],
        },
      },
    ]).exec(callback);
  },
  /** Graficas Horeca - Puntos Feat */
  // getPuntosFtAcumuladosMes: function (query, callback) {
  //   let ObjectId = require("mongoose").Types.ObjectId;
  //   this.aggregate([
  //     {
  //       $lookup: {
  //         from: "punto_entregas", //Nombre de la colecccion a relacionar
  //         localField: query.idPunto, //Nombre del campo de la coleccion actual
  //         foreignField: "usuario_horeca", //Nombre del campo de la coleccion a relacionar
  //         pipeline: [
  //           // {
  //           //   $project: {
  //           //     distribuidor: "$distribuidor",
  //           //   },
  //           // },
  //         ],
  //         as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
  //       },
  //     },
  //     // {
  //     //   $match: {
  //     //     punto_entrega: new ObjectId(query.idPunto),
  //     //     movimiento: "Congelados",
  //     //     fecha: {
  //     //       $gte: new Date(query.inicio),
  //     //       $lte: new Date(query.fin),
  //     //     },
  //     //   },
  //     // },
  //     // {
  //     //   $addFields: {
  //     //     fechaGroup: {
  //     //       $dateToString: {
  //     //         format: "%Y-%m",
  //     //         date: "$fecha",
  //     //       },
  //     //     },
  //     //   },
  //     // },
  //     // {
  //     //   $group: {
  //     //     _id: "$fechaGroup",
  //     //     total: { $sum: "$puntos_ganados" },
  //     //   },
  //     // },
  //     // {
  //     //   $sort: { _id: 1 },
  //     // },
  //   ]).exec(callback);
  // },
  // getPuntosFtAcumuladosDistribuidor: function (query, callback) {
  //   let ObjectId = require("mongoose").Types.ObjectId;
  //   this.aggregate([
  //     {
  //       $match: {
  //         punto_entrega: new ObjectId(query.idPunto),
  //         movimiento: "Congelados",
  //         fecha: {
  //           $gte: new Date(query.inicio),
  //           $lte: new Date(query.fin),
  //         },
  //       },
  //     },
  //     {
  //       $lookup: {
  //         from: "pedidos", //Nombre de la colecccion a relacionar
  //         localField: "pedido", //Nombre del campo de la coleccion actual
  //         foreignField: "_id", //Nombre del campo de la coleccion a relacionar
  //         pipeline: [
  //           {
  //             $project: {
  //               distribuidor: "$distribuidor",
  //             },
  //           },
  //         ],
  //         as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
  //       },
  //     },
  //     {
  //       $lookup: {
  //         from: "distribuidors", //Nombre de la colecccion a relacionar
  //         localField: "data_pedido.distribuidor", //Nombre del campo de la coleccion actual
  //         foreignField: "_id", //Nombre del campo de la coleccion a relacionar
  //         pipeline: [
  //           {
  //             $project: {
  //               distribuidor_nombre: "$nombre",
  //             },
  //           },
  //         ],
  //         as: "data_distribuidor", //Nombre del campo donde se insertara todos los documentos relacionados
  //       },
  //     },
  //     {
  //       $group: {
  //         _id: {
  //           $arrayElemAt: ["$data_distribuidor.distribuidor_nombre", 0],
  //         },
  //         total: { $sum: "$puntos_ganados" },
  //       },
  //     },
  //     {
  //       $sort: { _id: 1 },
  //     },
  //   ]).exec(callback);
  // },
  // getPuntosFtAcumuladosOrganizacion: function (query, callback) {
  //   let ObjectId = require("mongoose").Types.ObjectId;
  //   this.aggregate([
  //     {
  //       $match: {
  //         punto_entrega: new ObjectId(query.idPunto),
  //         movimiento: "Congelados",
  //         fecha: {
  //           $gte: new Date(query.inicio),
  //           $lte: new Date(query.fin),
  //         },
  //       },
  //     },
  //     {
  //       $lookup: {
  //         from: "reportepedidos", //Nombre de la colecccion a relacionar
  //         localField: "pedido", //Nombre del campo de la coleccion actual
  //         foreignField: "idPedido", //Nombre del campo de la coleccion a relacionar
  //         as: "data_pedido", //Nombre del campo donde se insertara todos los documentos relacionados
  //       },
  //     },
  //     {
  //       $unwind: "$data_pedido",
  //     },
  //     {
  //       $replaceRoot: {
  //         newRoot: "$data_pedido",
  //       },
  //     },
  //     {
  //       $project: {
  //         producto_precios: {
  //           $arrayElemAt: ["$detalleProducto.precios", 0],
  //         },
  //         producto_unidades_compradas: "$unidadesCompradas",
  //         organizacion_id: "$idOrganizacion",
  //       },
  //     },
  //     {
  //       $addFields: {
  //         total_puntos: {
  //           $multiply: [
  //             "$producto_unidades_compradas",
  //             "$producto_precios.puntos_ft_unidad",
  //           ],
  //         },
  //       },
  //     },
  //     {
  //       $group: {
  //         _id: "$organizacion_id",
  //         total: { $sum: "$total_puntos" },
  //       },
  //     },
  //     {
  //       $lookup: {
  //         from: "organizacions", //Nombre de la colecccion a relacionar
  //         localField: "_id", //Nombre del campo de la coleccion actual
  //         foreignField: "_id", //Nombre del campo de la coleccion a relacionar
  //         pipeline: [
  //           {
  //             $project: {
  //               organizacion_nombre: "$nombre",
  //             },
  //           },
  //         ],
  //         as: "data_organizacion", //Nombre del campo donde se insertara todos los documentos relacionados
  //       },
  //     },
  //     {
  //       $project: {
  //         _id: {
  //           $arrayElemAt: ["$data_organizacion.organizacion_nombre", 0],
  //         },
  //         total: "$total",
  //       },
  //     },
  //     {
  //       $sort: { _id: 1 },
  //     },
  //   ]).exec(callback);
  // },
};
const Puntos_ganados_establecimiento = (module.exports = mongoose.model(
  "Puntos_ganados_establecimiento",
  Puntos_ganados_establecimientoSchema
));
