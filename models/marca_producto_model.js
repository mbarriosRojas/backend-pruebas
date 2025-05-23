const mongoose = require("mongoose");
// Schema

const Marca_productoSchema = mongoose.Schema({
    nombre: { type: String, required: true },
    organizacion: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Organizacion",
        required: false,
    },
    categoria: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Categoria_producto",
        required: false,
    },
    linea: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Linea_producto",
        required: false,
    },
    estado: {
        type: String,
        required: false,
        default: "Aprobado",
        enum: ["Aprobado", "Inactiva"],
    },
    createdAt: { type: Date, required: false, default: Date.now },
});

Marca_productoSchema.statics = {
   
};

const Marca_producto = (module.exports = mongoose.model(
    "Marca_producto",
    Marca_productoSchema
));
