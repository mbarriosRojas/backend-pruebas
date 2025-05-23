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
  
};

const Pedido_tracking = (module.exports = mongoose.model(
  "Pedido_tracking",
  Pedido_trackingSchema
));
