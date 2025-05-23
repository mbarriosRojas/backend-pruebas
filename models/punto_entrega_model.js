const mongoose = require("mongoose");
const Punto_entregaSchema = mongoose.Schema({
  usuario_horeca: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Usuario_horeca",
    required: true,
  },
  nombre: { type: String, required: true },
  ciudad: { type: String, required: true },
  telefono: { type: Number, required: true },
  direccion: { type: String, required: true },
  pais: { type: String, required: true, default: "" },
  departamento: { type: String, required: true, default: "" },
  coord: {
    lat: { type: String, required: false },
    lng: { type: String, required: false },
  },
  informacion_contacto: { type: Array, required: false },
  data_contacto: {
    nombreContacto: { type: String, required: false, default: "" },
    telefonoContacto: { type: String, required: false, default: ""},
    emailContacto: { type: String, required: false, default: "" },
  },
  puntos_inscripcion: { type: Number, default: 100},
  sillas: { type: Number, required: true },
  domicilios: { type: Boolean, required: true },
  numero_trabajadores: { type: Number, required: true },
  tipo_aprobador: {
    type: String,
    required: true,
    default: "No Aprobador",
    enum: ["Aprobador", "No Aprobador"],
  },
  dias_atencion: { type: Array, required: true },
  horario: { type: String, required: true },
  estado: {
    type: String,
    required: false,
    default: "Activo",
    enum: ["Activo", "Desactivado"],
  },
  formato_coordenada: {
    type: {
      type: String,
      default: 'Point'
    },
    coordinates: [Number],
    required: false
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Punto_entregaSchema.statics = {
  
};

const Punto_entrega = (module.exports = mongoose.model(
  "Punto_entrega",
  Punto_entregaSchema
));

