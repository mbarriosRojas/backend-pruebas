const mongoose = require("mongoose");
const DistribuidorSchema = mongoose.Schema({
  nombre: { type: String, required: true },
  correo: { type: String, required: true },
  nit_cc: { type: String, require: true },
  logo: { type: String, required: false },
  descripcion: { type: String, required: true },
  listGrucam: { type: Boolean, default: false },
  tiempo_entrega: { type: String, required: false },
  departamento: { type: String, required: true },
  pais: { type: String, required: false, default: 'Colombia' },
  ciudad: { type: String, required: true },
  direccion: { type: String, required: true },
  urlPago: { type: String, required: false },
  ranking: { type: Number, required: true },
  // Almacena la identificación de los clientes pre-aprobados de un distribuidor. Puede ser tanto NIT como CC.
  clientes_preaprobados: { type: [String], default: [] },
  top_productos: {
    type: [mongoose.Schema.Types.ObjectId],
    ref: "Producto",
    default: [],
  },
  tipo: { type: String, required: false },
  valor_minimo_pedido: { type: Number, required: false },
  cobertura_coordenadas: [
    {
      longitud: { type: Number, required: false, default: 0 },
      latitud: { type: Number, required: false, default: 0 },
    },
  ],
  cant_votos_raking: { type: Number, required: false, default: 0 },
  ranking_gen: [
    {
      name: { type: String, required: false, default: 'Abastecimiento' },
      puntos: { type: Number, required: false, default: 0 },
    },
    {
      name: { type: String, required: false, default: 'Puntualidad' },
      puntos: { type: Number, required: false, default: 0 },
    },
    {
      name: { type: String, required: false, default: 'Precio' },
      puntos: { type: Number, required: false, default: 0 },
    },
  ],
  horario_atencion: { type: String, required: false },
  metodo_pago: { type: String, required: false },
  max_establecimientos: { type: Number, required: false, default: 10 },
  razon_social: { type: String, required: false },
  celular: { type: String, required: true },
  telefono: { type: String, required: false },
  solicitud_vinculacion: {
    type: String,
    required: false,
    default: "Pendiente",
  },
  tipo_persona: {
    type: String,
    required: true,
    default: "Natural",
    enum: ["Natural", "Juridica"],
  },
  zonas_cobertura: {
    type: {
      type: String,
      default: 'MultiPolygon'
    },
    coordinates: [[[[Number]]]],
    required: false
  },
  datos_poligono: [
    {
      tipo_promesa: { type: String, required: false },
      valor_promesa: { type: String, required: false },
      valor_pedido: { type: Number, required: false },
    }
  ],
  createdAt: { type: Date, required: false, default: Date.now },
});

DistribuidorSchema.statics = {}