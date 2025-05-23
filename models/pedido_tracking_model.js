const mongoose = require("mongoose");
const ObjectId = require("mongoose").Types.ObjectId;
const moment = require('moment-timezone');

// Schema

const Pedido_trackingSchema = mongoose.Schema({
  pedido: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido",
    required: true,
  },
  estado_anterior: {
    type: String,
    required: true,
    default: "Pendiente",
    enum: [
      "Sugerido", //distribuidor al planeador de pedidos
      "Pendiente", //creado pendiente por aprobacion de admin horeca
      "Aprobado Interno", //admin del horeca
      "Aprobado Externo", //distribuidor
      "Alistamiento",
      "Despachado",
      "Facturado", //intermedio opcional del distribuidor
      "Entregado", //lo coloca el distribuidor al momento de la entrega
      "Recibido", // lo confirma el punto de entrega que recibe
      "Calificado", //obcional despues de la entrega
      "Rechazado", //admin del horeca
      "Cancelado por horeca", //admin del horeca
      "Cancelado por distribuidor",
    ],
  },
  estado_nuevo: {
    type: String,
    required: true,
    default: "Pendiente",
    enum: [
      "Sugerido", //distribuidor al planeador de pedidos
      "Pendiente", //creado pendiente por aprobacion de admin horeca
      "Aprobado Interno", //admin del horeca
      "Aprobado Externo", //distribuidor
      "Alistamiento",
      "Despachado",
      "Facturado", //intermedio opcional del distribuidor
      "Entregado", //lo coloca el distribuidor al momento de la entrega
      "Recibido", // lo confirma el punto de entrega que recibe
      "Calificado", //obcional despues de la entrega
      "Rechazado", //admin del horeca
      "Cancelado por horeca", //admin del horeca
      "Cancelado por distribuidor",
    ],
  },
  usuario: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Trabajador",
    required: true,
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Pedido_trackingSchema.statics = {
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
  updateByIdPunto: function (id, updateData, callback) {
    this.findOneAndUpdate(
      { pedido: id },
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

    const Pedido_tracking = new this(data);
    Pedido_tracking.save(callback);
  },
  getPedido_trackingsByPunto: function (obj, callback) {
    const query = {
      usuario_horeca: new ObjectId(obj.usuario_horeca),
      punto_entrega: new ObjectId(obj.punto_entrega),
      //$and: [{ fecha: { $lte: obj.dateMax } }, { fecha: { $gte: obj.dateMin } }],
    };
    Pedido_tracking.find(query, callback);
  },
  getPedido_trackingsByDistribuidor: function (obj, callback) {
    const query = {
      usuario_horeca: new ObjectId(obj.usuario_horeca),
      //$and: [{ fecha: { $lte: obj.dateMax } }, { fecha: { $gte: obj.dateMin } }]
    };
    Pedido_tracking.find(query, callback).populate(
      "trabajador distribuidor punto_entrega"
    );
  },
  getPedido_trackingsByTrabajador: function (obj, callback) {
    const query = {
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido_tracking.count(query, callback);
  },
  getPedido_trackingsSugeridos: function (obj, callback) {
    const query = {
      estado: "Sugerido",
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido_tracking.count(query, callback);
  },
  lastStatusPedido: function (query, callback) {
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },
  /** Recupera la fecha de aprobación de un pedido por el distribuidor */
  trackingFechaAprobadoExterno: function (query, callback) {
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },
  /** Recupera la fecha de entrega de un pedido por el distribuidor */
  trackingFechaEntregado: function (query, callback) {
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },
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
  get_traking_pedidos: function (query, callback) {
    this.aggregate([
      {
        $match: {
          createdAt: {
            $gte: new Date(query.inicio),
            $lte: new Date(query.fin),
          },
        },
      },
      {
        $group: {
          _id: "$pedido",
          estado_nuevo: { $last: "$estado_nuevo" },
        },
      },
      {
        $group: {
          _id: "$estado_nuevo",
          cantidad: { $sum: 1 },
        },
      },
    ]).exec(callback);
  },
  removeById: function (removeData, callback) {
    this.findOneAndRemove(removeData, callback);
  },
  removeAll: function (query, callback) {
    this.remove(query).exec(callback);
  },
  create: function (data, callback) {
    const Pedido_tracking = new this(data);
    Pedido_tracking.save(callback);
  },
  getPedido_trackingsByPunto: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      usuario_horeca: new ObjectId(obj.usuario_horeca),
      punto_entrega: new ObjectId(obj.punto_entrega),
      //$and: [{ fecha: { $lte: obj.dateMax } }, { fecha: { $gte: obj.dateMin } }],
    };
    Pedido_tracking.find(query, callback);
  },
  getPedido_trackingsByDistribuidor: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      usuario_horeca: new ObjectId(obj.usuario_horeca),
      //$and: [{ fecha: { $lte: obj.dateMax } }, { fecha: { $gte: obj.dateMin } }]
    };
    Pedido_tracking.find(query, callback).populate(
      "trabajador distribuidor punto_entrega"
    );
  },
  getPedido_trackingsByTrabajador: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido_tracking.count(query, callback);
  },
  getPedido_trackingsSugeridos: function (obj, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    const query = {
      estado: "Sugerido",
      trabajador: new ObjectId(obj.trabajador),
    };
    Pedido_tracking.count(query, callback);
  },
  lastStatusPedido: function (query, callback) {
    var ObjectId = require("mongoose").Types.ObjectId;
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },
  /** Recupera la fecha de aprobación de un pedido por el distribuidor */
  trackingFechaAprobadoExterno: function (query, callback) {
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },
  /** Recupera la fecha de entrega de un pedido por el distribuidor */
  trackingFechaEntregado: function (query, callback) {
    Pedido_tracking.findOne(query).sort({ createdAt: -1 }).exec(callback);
  },

  // Recupera el total de tiempo de entrega de un pedido luego de finalizado y su detalle
  getTiempoEntregaPedido: function (query, callback) {
    this.aggregate([
      /** Recupera los trabajadores asociados al distribuidor */
      {
        $match: {
          pedido: new ObjectId(query),
        },
      },
      {
        $facet: {
          aprobado: [
            {
              $match: { estado_nuevo: "Aprobado Interno" },
            },
          ],
          entregado: [
            {
              $match: { estado_nuevo: "Entregado" },
            },
          ],
        },
      },
      {
        $project: {
          aprobado: {
            $arrayElemAt: ["$aprobado.createdAt", 0],
          },
          entregado: {
            $arrayElemAt: ["$entregado.createdAt", 0],
          },
        },
      },
      {
        $addFields: {
          total_horas: {
            $divide: [
              {
                $subtract: ["$entregado", "$aprobado"],
              },
              3600000,
            ],
          },
        },
      },
    ]).exec(callback);
  },
};

const Pedido_tracking = (module.exports = mongoose.model(
  "Pedido_tracking",
  Pedido_trackingSchema
));
