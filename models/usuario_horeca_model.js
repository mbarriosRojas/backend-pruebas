const mongoose = require("mongoose");
var moment = require("moment");

// Schema

const Usuario_horecaSchema = mongoose.Schema({
  nombres: { type: String, required: true },
  apellidos: { type: String, required: true },
  correo: { type: String, required: true },
  pais: { type: String, required: false },
  departamento: { type: String, required: false },
  ciudad: { type: String, required: false },
  tipo_documento: { type: String, required: true },
  numero_documento: { type: String, required: true },
  clave: { type: String, required: true },
  telefono: { type: String, required: false },
  origen: { type: String, required: false },
  celular: { type: String, required: true },
  tipo_aprobador: {
    type: String,
    required: true,
    default: "No Aprobador",
    enum: ["Aprobador", "No Aprobador"],
  },
  nombre_establecimiento: { type: String, required: false },
  empresa_pais: { type: String, required: false },
  empresa_departamento: { type: String, required: false },
  empresa_ciudad: { type: String, required: false },
  empresa_telefono: { type: Number, required: false },
  empresa_telefono2: { type: Number, required: false },
  propietario_tipo_documento: { type: String, required: false },
  propietario_numero_documento: { type: String, required: false },
  propietario_nombres: { type: String, required: false },
  propietario_apellidos: { type: String, required: false },
  propietario_telefono: { type: Number, required: false },
  propietario_correo: { type: String, required: false },
  logo: {
    type: String,
    required: false,
  },
  longitud: { type: Number, required: false, default: 0 },
  latitud: { type: Number, required: false, default: 0 },
  solicitud_vinculacion: {
    type: String,
    required: false,
    default: "Aprobado",
    enum: ["Aprobado", "Rechazado", "Cancelado", "Pendiente", "Creado distribuidor"],
  },
  rol: {
    type: String,
    required: true,
    default: "Administrador",
    enum: ["Horeca", "Distribuidor", "Organizacion", "Administrador"],
  },
  razon_social: { type: String, required: false },
  nit: { type: String, required: false },
  tipo_usuario: {
    type: String,
    required: true,
    default: "Natural",
    enum: ["Jurídica", "Natural"],
  },
  tipo_negocio: {
    type: String,
    required: true,
    default: "RESTAURANTE",
    enum: [
      "BAR / DISCOTECA",
      "CAFETERÍA / HELADERÍA / SNACK",
      "CATERING SERVICE / SERVICIO ALIMENTACIÓN",
      "COCINA OCULTA",
      "CENTRO DE DIVERSIÓN",
      "CENTRO DEPORTIVO Y GIMNASIOS",
      "CLUB SOCIAL / NEGOCIOS",
      "COMEDOR DE EMPLEADOS",
      "COMIDA RÁPIDA",
      "HOGAR",
      "MAYORISTA / MINORISTA",
      "OFICINA / COWORKING",
      "PANADERÍA / REPOSTERÍA",
      "PROPIEDAD HORIZONTAL",
      "RESTAURANTE",
      "RESTAURANTE DE CADENA",
      "SECTOR EDUCACIÓN",
      "SECTOR HOTELERO",
      "SECTOR SALUD",
      "SECTOR PÚBLICO / PRIVADO",
    ],
  },
  createdAt: { type: Date, required: false, default: Date.now },
});

Usuario_horecaSchema.statics = {
 
};

const Usuario_horeca = (module.exports = mongoose.model(
  "Usuario_horeca",
  Usuario_horecaSchema
));
