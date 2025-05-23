const mongoose = require("mongoose");
const moment = require("moment");
const { Decimal128 } = mongoose.Schema.Types;

// Schema

const ProductoSchema = mongoose.Schema({
  fotos: [
    {
      type: String,
      required: false,

    },
  ],
  nombre: { type: String, required: true },
  descripcion: { type: String, required: false, default: "" },
  precios: [
    {
      unidad_medida: { type: String, required: false },
      cantidad_medida: { type: Number, required: false },
      estado: {
        type: String,
        required: false,
        default: "Disponible",
        enum: ["Disponible", "Agotado"],
      },
      precio_unidad: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      precio_caja: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      precio_descuento: { 
        type: Number,
        default: 0,
        set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
      },
      inventario_unidad: { type: Number, required: false, default: 0 },
      inventario_caja: { type: Number, required: false, default: 0 },
      puntos_ft_unidad: { type: Number, required: false, default: 0 },
      puntos_ft_caja: { type: Number, required: false, default: 0 },
      und_x_caja: { type: Number, required: false, default: 0 },
      unidad_medida_manual: { type: String, required: false },
    },
  ],
  estadoActualizacion: {
    type: String,
    required: false,
    default: "Pendiente",
    enum: ["Pendiente", "Administrador", "Aceptado", "Rechazado", "Inactivo"],
  },
  fecha_vencimiento: { type: Date, required: false },
  fecha_cierre_puntosft: { type: Date, required: false },
  fecha_apertura_puntosft: { type: Date, required: false },
  promocion: { type: Boolean, required: false, default: false },
  saldos: { type: Boolean, required: false, default: false },
  codigo_ft: { type: String, required: false },
  codigo_promo: { type: String, required: false },
  codigo_distribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: false,
  },
  codigo_distribuidor_producto: { type: String, required: false },
  codigo_organizacion_producto: { type: String, required: false },
  codigo_organizacion: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Organizacion",
    required: false,
  },
  marca_producto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Marca_producto",
    required: false,
  },
  categoria_producto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Categoria_producto",
    required: false,
  },
  linea_producto: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Linea_producto",
    required: false,
  },
  productos_promocion: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Producto",
    required: false,
  },
  productos_promocion_inventario_unidades: {
    type: [Number],
    required: false,
  },
  ficha_tecnica: { type: String, required: false, default: "" },
  fechaAceptado: { type: Date, required: false },
  prodBiodegradable: { type: Boolean, required: false, default: false },
  prodPedido: { type: Boolean, required: false, default: false },
  prodDescuento: { type: Boolean, required: false, default: false },
  prodPorcentajeDesc: {
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  mostrarPF: { type: Boolean, required: false, default: false },
  comentarioVencimiento: { type: String, required: false, default: "" },
  organizacion_manual: { type: String, required: false },
  marca_manual: { type: String, required: false },
  categoria_manual: { type: String, required: false },
  linea_manual: { type: String, required: false },
  establecimientos_interesados: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Usuario_horeca",
    required: false,
  },
  descuentosEspeciales: [],
  createdAt: { type: Date, required: false, default: Date.now },
});

ProductoSchema.statics = {

};

const Producto = (module.exports = mongoose.model("Producto", ProductoSchema));
