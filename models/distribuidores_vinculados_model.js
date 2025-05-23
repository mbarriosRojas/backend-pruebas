const mongoose = require("mongoose");
const Distribuidores_vinculadosSchema = mongoose.Schema({
  estado: {
    type: String,
    required: true,
    default: "Aprobado",
    enum: ["Pendiente", "Aprobado", "Rechazado", "Cancelado"],
  },
  convenio: {
    type: Boolean,
    required: true,
  },
  cartera: {
    type: Boolean,
    required: true,
  },
  punto_entrega: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  usuario_registrado: {
    type: Boolean,
    default: false
  },
  vendedor: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Trabajador",
    required: true,
  },
  pazysalvo: {
    type: Boolean,
    required: true,
  },
  slug_fact: {
    type: String,
    required: false,
  },
  creado_distribuidor: {
    type: Boolean,
    required: false,
    default: false
  },
  createdAt: {
    type: Date,
    required: false,
    default: Date.now,
  },
});

Distribuidores_vinculadosSchema.statics = {}