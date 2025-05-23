const mongoose = require("mongoose");
// Schema
var ObjectId = require("mongoose").Types.ObjectId;

const PedidoSchema = mongoose.Schema({
  usuario_horeca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  punto_entrega: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  trabajador: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Trabajador",
    required: true,
  },
  distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  id_pedido: { type: Number, required: true },
  fecha: { type: Date, required: true },
  estado: {
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
  metodo_pago: { type: String, required: true },
  ciudad: { type: String, required: true },
  direccion: { type: String, required: true },
  total_pedido: { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  subtotal_pedido: { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  descuento_pedido: { type: Number, required: false, default: 0 },
  puntos_ganados: { type: Number, required: false },
  puntos_redimidos: { type: Number, required: false },
  tiempo_estimado_entrega: { type: String, required: true },
  tiempo_tracking_hora: { type: String, required: true },
  pre_factura: { type: String, required: false },
  calificacion: { type: Object, required: false },
  productos: [
    {
      product: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Producto",
        required: true,
      },
      caja: { type: Number, required: false },
      unidad: { type: Number, required: false },
      referencia: { type: String, required: false },
      categoria: { type: String, required: false },
      linea: { type: String, required: false },
      marca: { type: String, required: false },
      puntos_ft_unidad: { type: Number, required: false },
      puntos_ft_caja: { type: Number, required: false },
      data_precioProducto: [],
      unidad_medida: { type: String, required: false },
      cantidad_medida: { type: Number, required: false },
      precio_original: { type: Number, required: false, default: 0 },
      und_x_caja: { type: Number, required: false },
      precio_caja: { type: Number, required: false, default: 0 },
      porcentaje_descuento: { type: Boolean, required: false },
      precioEspecial: []
    },
  ],
  codigo_descuento: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Codigos_descuento_generados",
    required: false,
  },
  createdAt: { type: Date, required: false, default: Date.now },
});
PedidoSchema.statics = {
 
};
const Pedido = (module.exports = mongoose.model("Pedido", PedidoSchema));