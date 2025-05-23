const mongoose = require('mongoose');
const ObjectId = require("mongoose").Types.ObjectId;
// Schema

const Linea_productoSchema = mongoose.Schema({
    nombre: {type: String, required: true},
    categoria: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Categoria_producto",
        required: true,
    },
    estado: {type: String, required: false, default: 'Activo'},
    createdAt: {type: Date, required: false, default: Date.now}
});

Linea_productoSchema.statics = {

};

const Linea_producto = (module.exports = mongoose.model('Linea_producto', Linea_productoSchema));

