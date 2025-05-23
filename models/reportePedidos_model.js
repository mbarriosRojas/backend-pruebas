const mongoose = require("mongoose");

// Schema

const ReportePedSchema = mongoose.Schema({
  idPedido: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido",
    required: true,
  },
  idOrganizacion: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Organizacion",
    required: false,
  },
  idDistribuidor: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Distribuidor",
    required: true,
  },
  idComprador: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  idUserHoreca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  idTraking: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Pedido_tracking",
    required: true,
  },
  idPunto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Punto_entrega",
    required: true,
  },
  productoId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Producto",
    required: true,
  },
  categoriaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Categoria_producto",
    required: false,
  },
  lineaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Linea_producto",
    required: false,
  },
  marcaProducto: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Marca_producto",
    required: false,
  },
  tipoUsuario: { type: String, required: false },
  ciudad: { type: String, required: false },
  direccion: { type: String, required: false },
  estaAntTraking: { type: String, required: false },
  estaActTraking: { type: String, required: false },
  nombreProducto: { type: String, required: false },
  codigoFeatProducto: { type: String, required: false },
  codigo_organizacion_producto: { type: String, required: false },
  codigoDistribuidorProducto: { type: String, required: false },
  caja: { type: String, required: false },
  unidadesCompradas: { type: Number, required: false },
  costoProductos: { type: Number, required: false },
  referencia: { type: String, required: false },
  puntos_ft_unidad: { type: Number, required: false, default: 0 },
  puntos_ft_caja: { type: Number, required: false, default: 0 },
  puntos_ft_ganados: { type: Number, required: false, default: 0 },
  puntoCiudad: { type: String, required: false },
  puntoDomicilio: { type: Boolean, required: false },
  puntoSillas: { type: Number, required: false },
  totalCompra:  { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  subtotalCompra:  { 
    type: Number,
    default: 0,
    set: (v) => typeof v === 'string' ? parseFloat(v.replace(",", ".")) : v,
  },
  descuento: { type: Number, required: false },
  puntosGanados: { type: Number, required: false },
  puntosRedimidos: { type: Number, required: false },
  precio_caja_app: { type: Number, required: false, default: 0 },
  detalleProducto: {},
  precioEspecial: [],
  createdAt: { type: Date, required: false, default: Date.now() },
});

ReportePedSchema.statics = {

};

const ReportePed = (module.exports = mongoose.model(
  "ReportePedidos",
  ReportePedSchema
));
