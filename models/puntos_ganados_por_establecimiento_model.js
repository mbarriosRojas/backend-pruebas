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

};
const Puntos_ganados_establecimiento = (module.exports = mongoose.model(
  "Puntos_ganados_establecimiento",
  Puntos_ganados_establecimientoSchema
));
